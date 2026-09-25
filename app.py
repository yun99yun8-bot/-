"""Read-only Web: official results, omissions and past practice only."""
import json
import os
from pathlib import Path
from datetime import datetime,timezone

os.environ['RUN_EMBEDDED_WORKERS']='0'
from flask import Flask,Response,jsonify,request
import collector_core as core
import backfill

ROOT=Path(__file__).resolve().parent
app=Flask(__name__)


@app.get('/')
def index():
    return Response((ROOT/'index.html').read_text(encoding='utf-8'),
                    mimetype='text/html; charset=utf-8',headers={'Cache-Control':'no-store'})


@app.get('/style.css')
def stylesheet():
    return Response((ROOT/'style.css').read_text(encoding='utf-8'),
                    mimetype='text/css; charset=utf-8')


@app.get('/research-report')
def practice_page():
    return Response((ROOT/'research-report.html').read_text(encoding='utf-8'),
                    mimetype='text/html; charset=utf-8',headers={'Cache-Control':'no-store'})


def _history(limit=30,before=None):
    conn=core.db_connect(retries=0)
    if conn is None:raise RuntimeError('database unavailable')
    try:
        with conn.cursor(cursor_factory=core.RealDictCursor) as cur:
            cur.execute("""SELECT period_key,period_date,period_no,target_block,
                      block_number,block_hash,numbers,single_count
                FROM period_groups WHERE group_no=20 AND period_key<%s
                ORDER BY period_key DESC LIMIT %s""",(before or '9999',min(205,int(limit)*2+1)))
            rows=cur.fetchall()
        out=[]
        for row in rows:
            if not core.calibrated_group20_row(row):continue
            nums=row['numbers']
            if isinstance(nums,str):nums=json.loads(nums)
            out.append({'period':row['period_key'],'platformPeriod':row['period_date'].strftime('%y%m%d')+f"{int(row['period_no']):04d}",
                        'blockNumber':int(row['block_number']),'blockHash':row['block_hash'],
                        'numbers':nums,'single':int(row['single_count'])})
            if len(out)>=limit:break
        return out
    finally:core.db_release(conn)


@app.get('/api/live')
def live():
    """No model queries or front-end collection side effects."""
    try:
        ds,p,ps,platform=core.current_period()
        target=core.period_target_block(ds,p)
        rows=_history(20)
        newest=rows[0] if rows else None
        # The official block can be in tron_blocks before smart-db materializes
        # its period_groups row; display it immediately without writing from Web.
        if not newest or newest['period']!=f'{ds}:{ps}':
            with core._live_blocks_lock:
                memory=core._live_blocks.get(target)
            direct=memory or core.get_db_blocks([target]).get(target)
            if direct and direct.get('numbers'):
                nums=direct['numbers']
                if isinstance(nums,str):nums=json.loads(nums)
                newest={'period':f'{ds}:{ps}','platformPeriod':platform,'blockNumber':target,
                        'blockHash':direct['block_hash'],'numbers':nums,
                        'single':int(direct['single_count'])}
                rows=[newest]+rows[:19]
        omission=core.get_omission_runtime()
        return jsonify({'ok':True,'version':core.MODEL_VERSION,
                        'currentPeriod':platform,'targetBlock':target,
                        'latest':newest,'history':rows,
                        'omission':omission['omission'] if omission else None,
                        'omissionThrough':omission['last_period_key'] if omission else None})
    except Exception as exc:
        return jsonify({'ok':False,'error':type(exc).__name__,'message':str(exc)[:150]}),503


@app.get('/api/history')
def history():
    try:
        limit=max(1,min(100,request.args.get('limit',30,type=int)))
        before=request.args.get('before','',type=str)
        if before and (len(before)>20 or not all(c.isdigit() or c in '-:' for c in before)):
            return jsonify({'ok':False,'error':'invalid before period'}),400
        rows=_history(limit,before)
        return jsonify({'ok':True,'rows':rows,'nextBefore':rows[-1]['period'] if len(rows)==limit else None})
    except Exception as exc:
        return jsonify({'ok':False,'error':type(exc).__name__,'message':str(exc)[:150]}),503


@app.get('/api/research-report')
def research_report():
    conn=None
    try:
        conn=core.db_connect(retries=0)
        with conn.cursor(cursor_factory=core.RealDictCursor) as cur:
            cur.execute('SELECT report,research_version,updated_at FROM research_replay_runtime WHERE singleton=1')
            stored=cur.fetchone()
            cur.execute('SELECT * FROM backfill_runtime WHERE singleton=1')
            progress=cur.fetchone()
            cur.execute('SELECT * FROM raw_backfill_runtime WHERE singleton=1')
            raw_progress=cur.fetchone()
            cur.execute("""SELECT candidate,research_method,COUNT(*) AS valid,
                COUNT(*) FILTER (WHERE prediction=actual_single) AS hits,
                MIN(period_key) AS first_period,MAX(period_key) AS last_period
                FROM research_predictions WHERE model_version=%s AND valid_pre_result=TRUE
                  AND actual_single IS NOT NULL AND candidate IN ('trial_first','trial_second')
                GROUP BY candidate,research_method ORDER BY candidate,research_method""",(core.RESEARCH_VERSION,))
            trials=cur.fetchall()
            cur.execute("""SELECT COUNT(*) AS periods,
                COUNT(*) FILTER (WHERE a.prediction=a.actual_single) AS first_hits,
                COUNT(*) FILTER (WHERE b.prediction=b.actual_single) AS second_hits
                FROM research_predictions a JOIN research_predictions b
                ON a.period_key=b.period_key AND b.candidate='trial_second'
                WHERE a.candidate='trial_first' AND a.model_version=%s AND b.model_version=%s
                AND a.valid_pre_result=TRUE AND b.valid_pre_result=TRUE
                AND a.actual_single IS NOT NULL AND a.actual_single=b.actual_single""",
                (core.RESEARCH_VERSION,core.RESEARCH_VERSION))
            both=cur.fetchone()
            cur.execute("""SELECT model_version,updated_at,detail FROM service_heartbeats
                WHERE service_role='worker' ORDER BY updated_at DESC LIMIT 1""")
            worker=cur.fetchone()
        report=stored['report'] if stored and stored['research_version']==core.RESEARCH_VERSION else {}
        if isinstance(report,str):report=json.loads(report)
        n=int(both['periods'] or 0)
        progress_data=None
        if progress:
            start=int(progress['start_index']);end=int(progress['end_index'])
            scanned=max(0,min(end-start+1,int(progress['next_index'])-start))
            progress_data={'status':progress['status'],'targetPeriods':end-start+1,
                'scannedPeriods':scanned,'alreadyPresent':int(progress['already_present']),
                'fetchedPeriods':int(progress['fetched_periods']),
                'failedPeriods':int(progress['failed_periods']),
                'startPeriod':'%s:%04d'%backfill.period_from_index(start),
                'endPeriod':'%s:%04d'%backfill.period_from_index(end),
                'estimatedMappingPeriods':max(0,min(end+1,core.TAIL_ANCHOR_INDEX)-start),
                'lastError':progress['last_error'],
                'updatedAt':progress['updated_at'].isoformat()}
            if raw_progress:
                total=max(0,int(raw_progress['end_block'])-int(raw_progress['start_block'])+1)
                stored=int(raw_progress['stored_blocks'] or 0)
                progress_data['rawWarehouse']={'status':raw_progress['status'],'startBlock':int(raw_progress['start_block']),
                    'endBlock':int(raw_progress['end_block']),'targetBlocks':total,'storedBlocks':stored,
                    'percent':round(100*stored/total,2) if total else 0,'failedBatches':int(raw_progress['failed_batches'] or 0),
                    'lastError':raw_progress['last_error'],'updatedAt':raw_progress['updated_at'].isoformat()}
        return jsonify({'ok':True,'version':core.MODEL_VERSION,'researchVersion':core.RESEARCH_VERSION,
            'practice':report,'practiceUpdatedAt':stored['updated_at'].isoformat() if report else None,
            'backfill':progress_data,
            'futureTrials':[{'slot':r['candidate'],'method':r['research_method'],
                'valid':int(r['valid']),'hits':int(r['hits']),
                'rate':round(100*r['hits']/r['valid'],2),'firstPeriod':r['first_period'],
                'lastPeriod':r['last_period']} for r in trials],
            'samePeriods':{'periods':n,'firstHits':int(both['first_hits'] or 0),
                'secondHits':int(both['second_hits'] or 0),
                'firstRate':round(100*both['first_hits']/n,2) if n else None,
                'secondRate':round(100*both['second_hits']/n,2) if n else None},
            'worker':{'version':worker['model_version'],'updatedAt':worker['updated_at'].isoformat(),
                'engines':(worker['detail'] or {}).get('runningEngines',[]),
                'errors':(worker['detail'] or {}).get('engineErrors',{})} if worker else None})
    except Exception as exc:
        return jsonify({'ok':False,'error':type(exc).__name__,'message':str(exc)[:150]}),503
    finally:
        if conn is not None:core.db_release(conn)


@app.get('/api/research-status')
def research_status():
    return research_report()


@app.get('/api/practice-history')
def practice_history():
    """Page through immutable, per-period retrospective predictions."""
    conn=None
    try:
        limit=max(1,min(100,request.args.get('limit',50,type=int)))
        before=request.args.get('before','',type=str)
        if before and (len(before)>20 or not all(c.isdigit() or c in '-:' for c in before)):
            return jsonify({'ok':False,'error':'invalid before period'}),400
        conn=core.db_connect(retries=0)
        with conn.cursor(cursor_factory=core.RealDictCursor) as cur:
            cur.execute("""SELECT period_key,trained_through,actual_single,predictions,hashes_complete
                FROM research_practice_results WHERE research_version=%s AND period_key<%s
                ORDER BY period_key DESC LIMIT %s""",
                (core.RESEARCH_VERSION,before or '9999',limit+1))
            rows=cur.fetchall()
        more=len(rows)>limit
        rows=rows[:limit]
        result=[{'period':r['period_key'],'trainedThrough':r['trained_through'],
                 'actual':int(r['actual_single']),
                 'predictions':json.loads(r['predictions']) if isinstance(r['predictions'],str) else r['predictions'],
                 'completePreResultGroups':r['hashes_complete']} for r in rows]
        return jsonify({'ok':True,'rows':result,'nextBefore':result[-1]['period'] if more else None})
    except Exception as exc:
        return jsonify({'ok':False,'error':type(exc).__name__,'message':str(exc)[:150]}),503
    finally:
        if conn is not None:core.db_release(conn)


if __name__=='__main__':app.run(host='0.0.0.0',port=5000)
