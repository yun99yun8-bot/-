"""V10.6: reset once, capture raw first, then rebuild; live TRON blocks are never intentionally skipped."""
import os, signal, socket, threading, time, json
import collector_core as core
import backfill

DATASET_ID='V10.6_RAW_FIRST_20260926'
if not core.DATABASE_URL: raise SystemExit('DATABASE_URL is required')
if not core.init_db(): raise SystemExit('Database initialization failed')

def reset_database_once():
    """Destructive reset exactly once for this dataset id, not on every Render restart."""
    conn=core.db_connect(retries=0)
    if conn is None: raise RuntimeError('database unavailable during reset')
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""CREATE TABLE IF NOT EXISTS dataset_runtime(
                    singleton SMALLINT PRIMARY KEY CHECK(singleton=1), dataset_id TEXT NOT NULL,
                    reset_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
                cur.execute('SELECT dataset_id FROM dataset_runtime WHERE singleton=1 FOR UPDATE')
                row=cur.fetchone()
                if row and row[0]==DATASET_ID: return False
                # Clear all prior application data but keep schema. dataset_runtime is the one reset marker.
                tables=['historical_raw_blocks','tron_blocks','period_groups','raw_backfill_runtime','backfill_runtime',
                        'backfill_failures','omission_runtime','ai_predictions','hash_model_runtime','research_predictions',
                        'research_replay_runtime','research_practice_results','research_selection','system_events',
                        'period_runtime','service_heartbeats']
                for table in tables: cur.execute(f'TRUNCATE TABLE {table} RESTART IDENTITY CASCADE')
                cur.execute("""INSERT INTO dataset_runtime(singleton,dataset_id,reset_at) VALUES(1,%s,NOW())
                    ON CONFLICT(singleton) DO UPDATE SET dataset_id=EXCLUDED.dataset_id,reset_at=NOW()""",(DATASET_ID,))
        return True
    finally: core.db_release(conn)

reset_database_once()
# Freeze the 10,000-period/200k-block historical window immediately after reset.
backfill._initialize()

def _save_live_raw(rows):
    if not rows:return
    from psycopg2.extras import execute_values
    conn=core.db_connect(retries=0)
    if conn is None:raise RuntimeError('database unavailable')
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur,"""INSERT INTO tron_blocks(block_number,block_hash,block_time,numbers,single_count)
                    VALUES %s ON CONFLICT(block_number) DO UPDATE SET block_hash=EXCLUDED.block_hash,
                    block_time=EXCLUDED.block_time,numbers=EXCLUDED.numbers,single_count=EXCLUDED.single_count""",
                    [(r['number'],r['block'],core.datetime.fromtimestamp(r['timestamp']/1000,core.timezone.utc),json.dumps(r['numbers']),r['singleCount']) for r in rows])
    finally: core.db_release(conn)

def live_raw_worker():
    """Save new blocks only. It does not run 360 rebuild while the 200k archive is downloading."""
    time.sleep(3); last=None
    while True:
        core.worker_touch('live-raw')
        try:
            latest=core.fetch_latest_block(); latest_no=int(latest['number'])
            if last is None:
                # Start at current chain tip; the historical worker owns the frozen older 200k range.
                row={'number':latest_no,'block':latest['block'],'timestamp':latest['timestamp'],
                     'numbers':[f'{v:02d}' for v in core.calc_numbers(latest['block'])]}
                row['singleCount']=core.calc_single_count([int(v) for v in row['numbers']]); _save_live_raw([row]); last=latest_no
            elif latest_no>last:
                start=last+1
                while start<=latest_no:
                    stop=min(start+100,latest_no+1)
                    backfill.mark_live_priority(2.0)
                    rows=backfill._range_blocks(start,stop); _save_live_raw(rows)
                    last=stop-1; start=stop
            time.sleep(2.0)
        except Exception as exc:
            core.worker_touch('live-raw',exc); time.sleep(2)

targets={'raw-200k':backfill.raw_download_worker,'rebuild-after-raw':backfill.backfill_worker,'live-raw':live_raw_worker}
threads={}; stopping=False
def stop(*_):
    global stopping; stopping=True
signal.signal(signal.SIGTERM,stop); signal.signal(signal.SIGINT,stop)
name=os.environ.get('WORKER_SERVICE_NAME','tron-monitor-worker'); instance=os.environ.get('RENDER_INSTANCE_ID') or socket.gethostname()
while not stopping:
    for label,target in targets.items():
        t=threads.get(label)
        if t is None or not t.is_alive():
            t=threading.Thread(target=target,name=label,daemon=True); threads[label]=t; t.start()
    core.persist_service_heartbeat(name,'worker',instance,'ONLINE',{'dataset':DATASET_ID,'runningEngines':[k for k,t in threads.items() if t.is_alive()]})
    time.sleep(5)
