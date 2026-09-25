"""V10.5.1: fast historical archive + lightweight live catch-up collector."""
import os, signal, socket, threading, time, json
from datetime import date
import collector_core as core
import backfill

if not core.DATABASE_URL: raise SystemExit('DATABASE_URL is required')
if not core.init_db(): raise SystemExit('Database initialization failed')

def period_from_index(index):
    ordinal,zero=divmod(int(index),1440)
    return date.fromordinal(ordinal).isoformat(),zero+1

def _has_complete(key,target):
    conn=core.db_connect(retries=0)
    if conn is None:return False
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT 1 FROM period_groups WHERE period_key=%s AND group_no=20 AND target_block=%s LIMIT 1',(key,target))
            return cur.fetchone() is not None
    finally: core.db_release(conn)

def _save_live_raw(rows):
    if not rows:return
    from psycopg2.extras import execute_values
    conn=core.db_connect(retries=0)
    if conn is None:return
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur,"""INSERT INTO tron_blocks(block_number,block_hash,block_time,numbers,single_count)
                    VALUES %s ON CONFLICT(block_number) DO UPDATE SET block_hash=EXCLUDED.block_hash,block_time=EXCLUDED.block_time,numbers=EXCLUDED.numbers,single_count=EXCLUDED.single_count""",
                    [(r['number'],r['block'],core.datetime.fromtimestamp(r['timestamp']/1000,core.timezone.utc),json.dumps(r['numbers']),r['singleCount']) for r in rows])
    finally: core.db_release(conn)

def live_catchup_worker():
    """Never uses future data. It only materializes periods whose target block already exists."""
    time.sleep(5)
    while True:
        core.worker_touch('live-catchup')
        try:
            backfill.mark_live_priority(2.0)
            latest=core.fetch_latest_block(); latest_no=int(latest['number'])
            ds,p,_,_=core.current_period(); now_idx=core.period_index(ds,p)
            # Catch up the last 30 periods after restart; already-complete rows are skipped.
            for idx in range(now_idx-30,now_idx+1):
                pds,pp=period_from_index(idx); target=core.period_target_block(pds,pp)
                if target>latest_no: continue
                key=f'{pds}:{pp:04d}'
                if _has_complete(key,target): continue
                backfill.mark_live_priority(3.0)
                rows=backfill._range_blocks(target-19,target+1)
                _save_live_raw(rows)
                groups={str(i+1):{'blockNumber':r['number'],'block':r['block'],'numbers':r['numbers'],'singleCount':r['singleCount']} for i,r in enumerate(rows)}
                core.persist_period_groups(pds,pp,groups,target)
                core.rebuild_omission_runtime()
            time.sleep(2.5)
        except Exception as exc:
            core.worker_touch('live-catchup',exc); time.sleep(3)

targets={'raw-200k':backfill.raw_download_worker,'rebuild-10000':backfill.backfill_worker,'live-catchup':live_catchup_worker}
threads={}; stopping=False

def stop(*_):
    global stopping; stopping=True
signal.signal(signal.SIGTERM,stop); signal.signal(signal.SIGINT,stop)
name=os.environ.get('WORKER_SERVICE_NAME','tron-monitor-worker')
instance=os.environ.get('RENDER_INSTANCE_ID') or socket.gethostname()
while not stopping:
    for label,target in targets.items():
        t=threads.get(label)
        if t is None or not t.is_alive():
            t=threading.Thread(target=target,name=label,daemon=True); threads[label]=t; t.start()
    core.persist_service_heartbeat(name,'worker',instance,'ONLINE',{'runningEngines':[k for k,t in threads.items() if t.is_alive()]})
    time.sleep(5)
