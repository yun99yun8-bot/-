"""V10.5.1 web: historical progress + live results + omissions."""
import os, json
from pathlib import Path
os.environ['RUN_EMBEDDED_WORKERS']='0'
from flask import Flask,Response,jsonify
import collector_core as core
import backfill
ROOT=Path(__file__).resolve().parent
app=Flask(__name__)

@app.get('/')
def index(): return Response((ROOT/'index.html').read_text(encoding='utf-8'),mimetype='text/html; charset=utf-8',headers={'Cache-Control':'no-store'})
@app.get('/style.css')
def css(): return Response((ROOT/'style.css').read_text(encoding='utf-8'),mimetype='text/css; charset=utf-8',headers={'Cache-Control':'no-store'})

def recent_and_omission(limit=20):
    conn=core.db_connect(retries=0)
    if conn is None:return [],{str(i):0 for i in range(8)}
    try:
        with conn.cursor(cursor_factory=core.RealDictCursor) as cur:
            cur.execute("""SELECT period_key,period_date,period_no,target_block,block_number,block_hash,numbers,single_count,captured_at
              FROM period_groups WHERE group_no=20 ORDER BY period_date DESC,period_no DESC LIMIT %s""",(limit,))
            rows=[]
            for r in cur.fetchall():
                if not core.calibrated_group20_row(r): continue
                x=dict(r); nums=x.get('numbers') or []
                if isinstance(nums,str): nums=json.loads(nums)
                rows.append({'periodKey':x['period_key'],'periodNo':int(x['period_no']),'blockNumber':int(x['block_number']),
                    'numbers':nums,'single':int(x['single_count']),'capturedAt':x['captured_at'].isoformat() if x.get('captured_at') else None})
            cur.execute("""SELECT single_count FROM period_groups WHERE group_no=20 ORDER BY period_date ASC,period_no ASC""")
            omission={str(i):0 for i in range(8)}
            for rr in cur.fetchall():
                s=int(rr['single_count'])
                if 0<=s<=7:
                    for i in range(8): omission[str(i)]=0 if i==s else omission[str(i)]+1
            return rows,omission
    finally: core.db_release(conn)

@app.get('/api/status')
def status():
    try:
        state=backfill.read_status(); recent,omission=recent_and_omission()
        if not state:return jsonify({'ok':True,'initialized':False,'recent':recent,'omission':omission})
        raw=state.get('raw') or {}; start=int(state['start_index']); end=int(state['end_index'])
        raw_total=max(0,int(raw.get('end_block',0))-int(raw.get('start_block',0))+1); raw_stored=int(raw.get('stored_blocks',0) or 0)
        rebuilt=max(0,min(end-start+1,int(state['next_index'])-start))
        return jsonify({'ok':True,'initialized':True,'version':core.MODEL_VERSION,
          'raw':{'status':raw.get('status'),'stored':raw_stored,'target':raw_total,'percent':round(raw_stored*100/raw_total,2) if raw_total else 0,'failedBatches':int(raw.get('failed_batches',0) or 0),'error':raw.get('last_error')},
          'periods':{'status':state['status'],'rebuilt':rebuilt,'target':end-start+1,'percent':round(rebuilt*100/(end-start+1),2),'fetched':int(state['fetched_periods'] or 0),'existing':int(state['already_present'] or 0),'failed':int(state['failed_periods'] or 0),'error':state['last_error']},
          'recent':recent,'omission':omission})
    except Exception as exc:return jsonify({'ok':False,'message':str(exc)[:240]}),503
