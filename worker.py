"""Collection, official history, omission and practice. No old AI engines."""
import os
import signal
import socket
import threading
import time

import collector_core as core
import research_engine

if not core.DATABASE_URL:raise SystemExit('DATABASE_URL is required')
if not core.init_db():raise SystemExit('Database initialization failed')

targets={
    'result-db-writer':core.result_db_writer_worker,
    'db-writer':core.db_writer_worker,
    'tron-ingest':core.tron_ingest_worker,
    'target-result-fast':core.target_result_fast_worker,
    'smart-db':core.smart_db_worker,
    'omission-engine':core.omission_engine_worker,
    'practice':research_engine.practice_worker,
    'trial':research_engine.live_trial_worker,
    'trial-verify':research_engine.verify_worker,
}
threads={}
stopping=False

def stop(*_):
    global stopping
    stopping=True

signal.signal(signal.SIGTERM,stop)
signal.signal(signal.SIGINT,stop)
name=os.environ.get('WORKER_SERVICE_NAME','tron-monitor-worker')
instance=os.environ.get('RENDER_INSTANCE_ID') or socket.gethostname()

while not stopping:
    for label,target in targets.items():
        thread=threads.get(label)
        if thread is None or not thread.is_alive():
            thread=threading.Thread(target=target,name=label,daemon=True)
            threads[label]=thread
            thread.start()
    with core._runtime_health_lock:
        errs=dict(core._runtime_health.get('workerErrors') or {})
        status=dict(core._runtime_health.get('workerHeartbeats') or {})
    core.persist_service_heartbeat(name,'worker',instance,'ONLINE',{
        'runningEngines':[key for key,thread in threads.items() if thread.is_alive()],
        'engineErrors':errs,'engineHeartbeats':status,
        'resultWriterBacklog':core._result_db_write_queue.qsize()})
    time.sleep(5)
