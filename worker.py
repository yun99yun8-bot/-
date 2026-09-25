"""V10.4 dedicated historical collector: raw TRON archive -> 360-rule period rebuild."""
import os, signal, socket, threading, time
import collector_core as core
import backfill

if not core.DATABASE_URL: raise SystemExit('DATABASE_URL is required')
if not core.init_db(): raise SystemExit('Database initialization failed')

targets={'raw-200k':backfill.raw_download_worker,'rebuild-10000':backfill.backfill_worker}
threads={}; stopping=False

def stop(*_):
    global stopping; stopping=True
signal.signal(signal.SIGTERM,stop); signal.signal(signal.SIGINT,stop)
name=os.environ.get('WORKER_SERVICE_NAME','tron-history-worker')
instance=os.environ.get('RENDER_INSTANCE_ID') or socket.gethostname()
while not stopping:
    for label,target in targets.items():
        t=threads.get(label)
        if t is None or not t.is_alive():
            t=threading.Thread(target=target,name=label,daemon=True); threads[label]=t; t.start()
    core.persist_service_heartbeat(name,'worker',instance,'ONLINE',{'runningEngines':[k for k,t in threads.items() if t.is_alive()]})
    time.sleep(5)
