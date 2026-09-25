"""V10.6.3 — one job only: reset -> download 200k raw blocks -> rebuild 10k periods.
No live/new block collection runs in this version. Latest data will be caught up later.
"""
import os, signal, socket, threading, time
import collector_core as core
import backfill

DATASET_ID='V10.6.3_HISTORY_ONLY_RAW200K_THEN_REBUILD_20260926'
if not core.DATABASE_URL: raise SystemExit('DATABASE_URL is required')
if not core.init_db(): raise SystemExit('Database initialization failed')

def reset_database_once():
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
                tables=['historical_raw_blocks','tron_blocks','period_groups','raw_backfill_runtime','backfill_runtime',
                        'backfill_failures','omission_runtime','ai_predictions','hash_model_runtime','research_predictions',
                        'research_replay_runtime','research_practice_results','research_selection','system_events',
                        'period_runtime','service_heartbeats']
                for table in tables:
                    cur.execute(f'TRUNCATE TABLE {table} RESTART IDENTITY CASCADE')
                cur.execute("""INSERT INTO dataset_runtime(singleton,dataset_id,reset_at) VALUES(1,%s,NOW())
                    ON CONFLICT(singleton) DO UPDATE SET dataset_id=EXCLUDED.dataset_id,reset_at=NOW()""",(DATASET_ID,))
        return True
    finally: core.db_release(conn)

did_reset=reset_database_once()
print(f'[启动] V10.6.3 HISTORY ONLY | 数据库重置={did_reset}', flush=True)
backfill._initialize()
st=backfill.read_status(); raw=(st or {}).get('raw') or {}
print(f"[锁定范围] {raw.get('start_block')} -> {raw.get('end_block')} | 目标={backfill.RAW_TARGET}组", flush=True)
print('[规则] 第一阶段只抓原始 block_number/hash/timestamp；不抓新数据、不算号码、不回填', flush=True)

threads={}; stopping=False

def stop(*_):
    global stopping; stopping=True
signal.signal(signal.SIGTERM,stop); signal.signal(signal.SIGINT,stop)

def start_thread(label,target):
    t=threading.Thread(target=target,name=label,daemon=True); threads[label]=t; t.start()

start_thread('raw-200k',backfill.raw_download_worker)
start_thread('rebuild-after-raw',backfill.backfill_worker)
name=os.environ.get('WORKER_SERVICE_NAME','tron-monitor-worker'); instance=os.environ.get('RENDER_INSTANCE_ID') or socket.gethostname()
while not stopping:
    for label,target in [('raw-200k',backfill.raw_download_worker),('rebuild-after-raw',backfill.backfill_worker)]:
        if not threads[label].is_alive():
            print(f'[线程重启] {label}', flush=True); start_thread(label,target)
    core.persist_service_heartbeat(name,'worker',instance,'ONLINE',{'dataset':DATASET_ID,'runningEngines':[k for k,t in threads.items() if t.is_alive()]})
    try:
        st=backfill.read_status(); raw=(st or {}).get('raw') or {}
        print(f"[状态] 原始={raw.get('stored_blocks',0)}/{backfill.RAW_TARGET} {raw.get('status')} | 360回填={(st or {}).get('status')} | 新数据=OFF", flush=True)
    except Exception as exc: print(f'[状态错误] {exc}', flush=True)
    time.sleep(15)
