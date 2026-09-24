"""V8.1.1 Production Background Worker.
All autonomous engine work lives here; Web remains read/API only.
Cross-process health is persisted to PostgreSQL.
"""
import os
import signal
import socket
import time
import app as core

_stop = False
_started = time.time()
INSTANCE_ID = os.environ.get('RENDER_INSTANCE_ID') or os.environ.get('HOSTNAME') or socket.gethostname()
SERVICE_NAME = os.environ.get('WORKER_SERVICE_NAME', 'tron-monitor-worker')

def _stop_handler(*_):
    global _stop
    _stop = True

signal.signal(signal.SIGTERM, _stop_handler)
signal.signal(signal.SIGINT, _stop_handler)

if not core.DATABASE_URL:
    raise SystemExit('DATABASE_URL is required')

core.init_db()
core.start_worker_once()
core.record_system_event('worker_service_started', None, {'modelVersion': core.MODEL_VERSION, 'instanceId': INSTANCE_ID})
print(f'[V8.1.1] worker started instance={INSTANCE_ID} model={core.MODEL_VERSION}', flush=True)

last_log = 0.0
while not _stop:
    now=time.time()
    with core._runtime_health_lock:
        local_hb=dict(core._runtime_health.get('workerHeartbeats') or {})
        local_err=dict(core._runtime_health.get('workerErrors') or {})
        last_ai=core._runtime_health.get('lastAiError')
        last_db=core._runtime_health.get('lastDbError')
    detail={
        'uptimeSeconds': int(now-_started),
        'engineHeartbeats': local_hb,
        'engineErrors': local_err,
        'lastAiError': last_ai,
        'lastDbError': last_db,
        'workerRestarts': dict(core._worker_restarts),
    }
    ok=core.persist_service_heartbeat(SERVICE_NAME,'worker',INSTANCE_ID,'ONLINE',detail)
    if now-last_log >= 30:
        ds,p,ps,_=core.current_period()
        alive=[]
        with core._worker_threads_lock:
            alive=[name for name,th in core._worker_threads.items() if th and th.is_alive()]
        print(f'[V8.1.1] heartbeat period={ps} db={"ok" if ok else "error"} engines={len(alive)} {alive}', flush=True)
        last_log=now
    time.sleep(5)

try:
    core.persist_service_heartbeat(SERVICE_NAME,'worker',INSTANCE_ID,'STOPPING',{'uptimeSeconds':int(time.time()-_started)})
    core.record_system_event('worker_service_stopping', None, {'modelVersion': core.MODEL_VERSION, 'instanceId': INSTANCE_ID})
except Exception:
    pass
print('[V8.1.1] worker stopping', flush=True)
