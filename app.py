"""v8god 0.0.5 web — four boards, strategy simulator, integrity visibility."""
import os,json
from pathlib import Path
os.environ['RUN_EMBEDDED_WORKERS']='0'
from flask import Flask,Response,jsonify,request
import collector_core as core
import backfill, practice_engine, strategy_engine, audit360
ROOT=Path(__file__).resolve().parent;app=Flask(__name__)
@app.get('/')
def index():return Response((ROOT/'index.html').read_text(encoding='utf-8'),mimetype='text/html; charset=utf-8',headers={'Cache-Control':'no-store'})
@app.get('/ai-lab')
def ai_lab():return Response((ROOT/'ai_lab.html').read_text(encoding='utf-8'),mimetype='text/html; charset=utf-8',headers={'Cache-Control':'no-store'})

@app.get('/style.css')
def css():return Response((ROOT/'style.css').read_text(encoding='utf-8'),mimetype='text/css; charset=utf-8',headers={'Cache-Control':'no-store'})

def stats(limit=20):
 c=core.db_connect(retries=0)
 if c is None:return [],{},{}
 try:
  with c.cursor(cursor_factory=core.RealDictCursor) as q:
   q.execute("""SELECT period_key,period_no,block_number,numbers,single_count,captured_at FROM period_groups
     WHERE group_no=20 ORDER BY period_date DESC,period_no DESC LIMIT %s""",(limit,))
   recent=[]
   for r in q.fetchall():
    x=dict(r);nums=x['numbers'];nums=json.loads(nums) if isinstance(nums,str) else nums
    recent.append({'periodKey':x['period_key'],'periodNo':int(x['period_no']),'blockNumber':int(x['block_number']),'numbers':nums,'single':int(x['single_count'])})
   q.execute("SELECT single_count FROM period_groups WHERE group_no=20 ORDER BY period_date ASC,period_no ASC")
   seq=[int(r['single_count']) for r in q.fetchall()]
   cur={str(i):0 for i in range(8)};longest={str(i):0 for i in range(8)};running={i:0 for i in range(8)}
   for s in seq:
    for i in range(8):
     if i==s:running[i]=0
     else:running[i]+=1;longest[str(i)]=max(longest[str(i)],running[i])
   for i in range(8):cur[str(i)]=running[i]
   return recent,cur,longest
 finally:core.db_release(c)

def practice():
 c=core.db_connect(retries=0)
 if c is None:return None
 try:
  with c.cursor(cursor_factory=core.RealDictCursor) as q:
   try:q.execute('SELECT * FROM shen_practice_runtime WHERE singleton=1');r=q.fetchone()
   except Exception:c.rollback();return None
   if not r:return None
   x=dict(r);rep=x.get('report') or {};rep=json.loads(rep) if isinstance(rep,str) else rep
   return {'status':x['status'],'round':x['round_no'],'datasetSize':x['dataset_size'],'updatedAt':x['updated_at'].isoformat(),'report':rep}
 finally:core.db_release(c)

def integrity():
 c=core.db_connect(retries=0)
 if c is None:return {'ok':False,'message':'DB unavailable'}
 try:
  with c.cursor() as q:
   q.execute("SELECT COUNT(*) FROM period_groups WHERE group_no=20");g20=int(q.fetchone()[0])
   q.execute("SELECT COUNT(*) FROM (SELECT period_key,COUNT(*) c FROM period_groups GROUP BY period_key HAVING COUNT(*)<>20) x");incomplete=int(q.fetchone()[0])
   q.execute("SELECT COUNT(*) FROM period_groups WHERE single_count<0 OR single_count>7");bad=int(q.fetchone()[0])
   q.execute("SELECT COUNT(*) FROM (SELECT block_number,COUNT(*) c FROM period_groups GROUP BY block_number HAVING COUNT(*)>1) x");dup_blocks=int(q.fetchone()[0])
   q.execute("SELECT COUNT(*) FROM (SELECT period_key,COUNT(DISTINCT group_no) c FROM period_groups GROUP BY period_key HAVING COUNT(DISTINCT group_no)<>20) x");bad_groups=int(q.fetchone()[0])
   return {'ok':incomplete==0 and bad==0 and dup_blocks==0 and bad_groups==0,'group20':g20,'incompletePeriods':incomplete,'invalidSingles':bad,'duplicateBlocks':dup_blocks,'badGroupSets':bad_groups}
 finally:core.db_release(c)

@app.get('/api/status')
def status():
 try:
  st=backfill.read_status();recent,om,longest=stats();pr=practice();raw=(st or {}).get('raw') or {}
  rec=strategy_engine.recommend(practice_engine.load_dataset(10000),om) if pr and pr.get('datasetSize',0)>=10000 else {'action':'WAIT','reason':'等待固定10000期数据完成','candidates':[]}
  return jsonify({'ok':True,'version':'v8god 0.0.5-ai-trainer','raw':{'status':raw.get('status'),'stored':int(raw.get('stored_blocks',0) or 0),'target':int(((st or {}).get('raw') or {}).get('end_block',0) or 0)-int(((st or {}).get('raw') or {}).get('start_block',0) or 0)+1},'periods':{'status':(st or {}).get('status')},'recent':recent,'omission':om,'longestOmission':longest,'practice':pr,'recommendation':rec,'integrity':integrity()})
 except Exception as e:return jsonify({'ok':False,'message':str(e)[:240]}),503


@app.get('/api/ai-handoff')
def ai_handoff():
 try:
  return jsonify({'ok':True,'handoff':practice_engine.bridge_report()})
 except Exception as e:return jsonify({'ok':False,'message':str(e)[:240]}),503


@app.get('/api/ai-lab')
def ai_lab_status():
 try:return jsonify({'ok':True,'lab':practice_engine.lab_status()})
 except Exception as e:return jsonify({'ok':False,'message':str(e)[:240]}),503

@app.post('/api/ai-training/control')
def ai_training_control():
 try:
  body=request.get_json(silent=True) or {}; action=str(body.get('action','')).lower()
  if action=='stop': practice_engine.set_training(False)
  elif action=='resume': practice_engine.set_training(True)
  elif action=='adopt':
   if not practice_engine.adopt_current(): return jsonify({'ok':False,'message':'暂无可采用训练结果'}),409
  else:return jsonify({'ok':False,'message':'action必须为 stop/resume/adopt'}),400
  return jsonify({'ok':True,'lab':practice_engine.lab_status()})
 except Exception as e:return jsonify({'ok':False,'message':str(e)[:240]}),500

@app.post('/api/strategy/backtest')
def strategy_backtest():
 try:
  cfg=request.get_json(silent=True) or {};data=practice_engine.load_dataset(10000)
  if len(data)<10000:return jsonify({'ok':False,'message':'固定10000期数据尚未完成'}),409
  return jsonify({'ok':True,'result':strategy_engine.backtest(data,cfg)})
 except (ValueError,TypeError) as e:return jsonify({'ok':False,'message':'参数错误：'+str(e)[:120]}),400
 except Exception as e:return jsonify({'ok':False,'message':str(e)[:200]}),500

@app.get('/api/audit360')
def audit360_report():
 try:return jsonify({'ok':True,'audit':audit360.run_audit(10000)})
 except Exception as e:return jsonify({'ok':False,'message':str(e)[:300]}),503

if __name__=='__main__':app.run(host='0.0.0.0',port=int(os.environ.get('PORT','10000')),debug=False,use_reloader=False)
