"""Dedicated AI Training Worker. Reads the same PostgreSQL, writes AI research tables only."""
import os, signal, socket, threading, time
import collector_core as core
import practice_engine
import audit360

VERSION='v8god 0.0.5-ai-worker'
if not core.DATABASE_URL: raise SystemExit('DATABASE_URL is required')
if not core.init_db(): raise SystemExit('Database initialization failed')
practice_engine.ensure_schema()
stopping=False
def stop(*_):
 global stopping; stopping=True
signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
audit=audit360.run_audit(10000)
if not audit.get('pass'):
 print('[AI锁定] 360复原审计未通过，禁止训练 | '+str(audit.get('mismatchCount'))+' mismatches',flush=True)
 raise SystemExit('360 audit failed; AI training is locked')
print(f'[AI启动] {VERSION} | 360审计PASS | 独立训练服务 | >=80%可采用但继续研究95%+',flush=True)
t=threading.Thread(target=practice_engine.worker,name='ai-training',daemon=True);t.start()
name=os.environ.get('AI_WORKER_SERVICE_NAME','tron-monitor-ai-worker'); instance=os.environ.get('RENDER_INSTANCE_ID') or socket.gethostname()
while not stopping:
 if not t.is_alive():
  print('[AI线程重启] training',flush=True); t=threading.Thread(target=practice_engine.worker,name='ai-training',daemon=True);t.start()
 try:
  lab=practice_engine.lab_status()
  core.persist_service_heartbeat(name,'ai-training',instance,'ONLINE',{'version':VERSION,'trainingEnabled':lab.get('trainingEnabled'),'bestAccuracy':lab.get('bestAccuracy'),'bestRound':lab.get('bestRound'),'adoptedRound':lab.get('adoptedRound'),'researchTarget':lab.get('researchTarget')})
 except Exception as e: print('[AI心跳错误]',type(e).__name__,str(e)[:200],flush=True)
 time.sleep(15)
