"""v8god 0.0.5 collector worker — collection/catch-up/repair only. AI training lives in ai_worker.py."""
import os, signal, socket, threading, time
import collector_core as core
import backfill

VERSION='v8god 0.0.5-three-service'
if not core.DATABASE_URL: raise SystemExit('DATABASE_URL is required')
if not core.init_db(): raise SystemExit('Database initialization failed')
print(f'[启动] {VERSION} collector | 保留现有数据库 | 禁止清库 | AI训练已拆分',flush=True)
stopping=False; threads={}
def stop(*_):
 global stopping; stopping=True
signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)

def completed_latest_index():
 c=core.db_connect(retries=0)
 try:
  with c.cursor() as q:
   q.execute("SELECT period_date,period_no FROM period_groups WHERE group_no=20 ORDER BY period_date DESC,period_no DESC LIMIT 1")
   r=q.fetchone(); return core.period_index(str(r[0]),int(r[1])) if r else None
 finally:core.db_release(c)

def fetch_chain_period(idx):
 ds,p=backfill.period_from_index(idx); target=core.period_target_block(ds,p)
 rows=backfill._range_blocks(target-19,target+1,ignore_live_priority=True)
 groups={}
 for g,r in enumerate(rows,1):
  nums=core.calc_numbers(r['block']); groups[str(g)]={'blockNumber':r['number'],'block':r['block'],'numbers':[f'{n:02d}' for n in nums],'singleCount':core.calc_single_count(nums)}
 core.persist_period_groups(ds,p,groups,target)
 return f'{ds}:{p:04d}'

def catchup_live_worker():
 print('[最新] 等待历史10000期回填完成',flush=True)
 while not stopping:
  try:
   st=backfill.read_status()
   if not st or st.get('status')!='complete': time.sleep(10);continue
   ds,p,_,_=core.current_period(); latest_closed=core.period_index(ds,p)-1
   have=completed_latest_index(); nxt=(have+1) if have is not None else latest_closed
   if nxt<=latest_closed:
    key=fetch_chain_period(nxt); core.rebuild_omission_runtime(); print(f'[最新] 已补齐 {key}',flush=True); time.sleep(.1)
   else: time.sleep(2)
  except Exception as e:
   print('[最新错误]',type(e).__name__,str(e)[:220],flush=True);time.sleep(5)

def start(name,target):
 t=threading.Thread(target=target,name=name,daemon=True);threads[name]=(t,target);t.start()

def repair_worker():
 print('[修复] 数据完整性检查线程启动',flush=True)
 while not stopping:
  try:
   st=backfill.read_status() or {}; raw=(st.get('raw') or {})
   if raw.get('status')=='complete':
    n=core.repair_calibrated_official_records(limit=10000); r=core.repair_recent_periods(limit=12); core.rebuild_omission_runtime()
    print(f'[修复] 校准修复={n} 最近期修复={r} | 完成',flush=True)
   time.sleep(300)
  except Exception as e:
   print('[修复错误]',type(e).__name__,str(e)[:220],flush=True);time.sleep(60)
start('catchup-live',catchup_live_worker);start('repair',repair_worker)
name=os.environ.get('WORKER_SERVICE_NAME','tron-monitor-worker');instance=os.environ.get('RENDER_INSTANCE_ID') or socket.gethostname()
while not stopping:
 for label,(t,target) in list(threads.items()):
  if not t.is_alive(): print('[线程重启]',label,flush=True);start(label,target)
 core.persist_service_heartbeat(name,'collector',instance,'ONLINE',{'version':VERSION,'runningEngines':[k for k,(t,_) in threads.items() if t.is_alive()]})
 time.sleep(15)
