"""V10.4 progress-only web UI for the 200k-block / 10k-period historical job."""
import os
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
def css(): return Response((ROOT/'style.css').read_text(encoding='utf-8'),mimetype='text/css; charset=utf-8')
@app.get('/api/status')
def status():
    try:
        state=backfill.read_status()
        if not state: return jsonify({'ok':True,'initialized':False})
        raw=state.get('raw') or {}; start=int(state['start_index']); end=int(state['end_index'])
        raw_total=max(0,int(raw.get('end_block',0))-int(raw.get('start_block',0))+1)
        raw_stored=int(raw.get('stored_blocks',0) or 0)
        rebuilt=max(0,min(end-start+1,int(state['next_index'])-start))
        return jsonify({'ok':True,'initialized':True,'version':core.MODEL_VERSION,
          'raw':{'status':raw.get('status'),'stored':raw_stored,'target':raw_total,'percent':round(raw_stored*100/raw_total,2) if raw_total else 0,'startBlock':raw.get('start_block'),'endBlock':raw.get('end_block'),'failedBatches':int(raw.get('failed_batches',0) or 0),'error':raw.get('last_error')},
          'periods':{'status':state['status'],'rebuilt':rebuilt,'target':end-start+1,'percent':round(rebuilt*100/(end-start+1),2),'fetched':int(state['fetched_periods'] or 0),'existing':int(state['already_present'] or 0),'failed':int(state['failed_periods'] or 0),'start':'%s:%04d'%backfill.period_from_index(start),'end':'%s:%04d'%backfill.period_from_index(end),'error':state['last_error']}})
    except Exception as exc: return jsonify({'ok':False,'message':str(exc)[:200]}),503
