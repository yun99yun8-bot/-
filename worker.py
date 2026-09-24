"""V8.1 Production Background Worker.
Runs all autonomous engine workers without starting the Flask web server.
"""
import os
import signal
import time
import app as core

_stop = False

def _stop_handler(*_):
    global _stop
    _stop = True

signal.signal(signal.SIGTERM, _stop_handler)
signal.signal(signal.SIGINT, _stop_handler)

if not core.DATABASE_URL:
    raise SystemExit('DATABASE_URL is required')

# Initialize schema before starting the autonomous engine.
core.init_db()
core.start_worker_once()
core.record_system_event('worker_service_started', None, {'modelVersion': core.MODEL_VERSION})

while not _stop:
    time.sleep(5)

try:
    core.record_system_event('worker_service_stopping', None, {'modelVersion': core.MODEL_VERSION})
except Exception:
    pass
