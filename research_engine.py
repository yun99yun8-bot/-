"""Bounded-memory, chronological practice over every archived official period.

One database page is read and released at a time. A rolling 1000-period
training window is refitted every 100 replay periods; each replay pick is
calculated before that period's outcome is added to the window.
"""
from collections import Counter, deque
from datetime import datetime, timezone
import json
import threading
import time

import collector_core as core
import hash_research as model

METHODS=(*model.FEATURE_FAMILIES,'fixed_3','fixed_4')
TRAIN=1000
REFIT=100
RECENT=1000
PAGE=80
ARCHIVE_PERIODS=10000



class Practice:
    def __init__(self, epoch=1):
        self.epoch=epoch
        self.window=deque(maxlen=TRAIN)
        self.history=deque(maxlen=30)   # newest first; observed outcomes only
        self.recent=deque(maxlen=RECENT)
        self.last_rows=deque(maxlen=100)
        self.hits=Counter()
        self.seen=0
        self.tested=0
        self.complete=0
        self.training_from=None
        self.training_through=None
        self.first_replay=None
        self.last_key=None
        self.models={}
        self.alphas={}
        self.saved_at=None
        self.saved_status=None
        self.lock=threading.RLock()
        self.omission_last={i:None for i in range(8)}
        self.omission_max={i:0 for i in range(8)}
        self.omission_current={i:0 for i in range(8)}

    def _fit(self, tune=False):
        window=list(self.window)
        if len(window)<TRAIN:return
        for name,names in model.FEATURE_FAMILIES.items():
            if tune:
                warm=model.fit(window[:700],names)
                self.alphas[name]=min((.15,.30,.50,.75,1.0),
                    key=lambda a:(model._evaluation(warm,a,window[700:])['loss'],a))
            self.models[name]=model.fit(window,names)

    def consume(self,key,groups,actual):
        """Group 20 is a label only; no G20 hash enters a candidate feature."""
        with self.lock:
            if self.last_key is not None and key<=self.last_key:
                raise ValueError('practice periods must be strictly chronological')
            self.last_key=key
            if not isinstance(actual,int) or actual not in range(8):return
            # Omission statistics are descriptive only and are updated from the
            # archived label after the prediction boundary. They are never read as
            # a future label by a candidate.
            for i in range(8):
                if i==actual:
                    self.omission_current[i]=0
                else:
                    self.omission_current[i]+=1
                    self.omission_max[i]=max(self.omission_max[i],self.omission_current[i])
            prior=list(self.history)
            feats=model.features(groups,prior)
            has_complete_hashes=feats is not None
            if feats is None:
                feats={name:'none' for name in model.FEATURE_NAMES}
                feats.update(model._history_features(prior))
            else:self.complete+=1
            item=(key,feats,actual)
            if self.seen<TRAIN:
                if self.seen==0:self.training_from=key
                self.training_through=key
                self.window.append(item)
                self.seen+=1
                self.history.appendleft(actual)
                if self.seen==TRAIN:self._fit(tune=True)
                return
            if self.tested%REFIT==0 and self.tested:
                self._fit()
            predictions={'fixed_3':3,'fixed_4':4}
            for name,trained in self.models.items():
                scores=model._blended(trained,feats,self.alphas[name])
                predictions[name]=max(range(8),key=lambda i:(scores[i],-i))
            if self.first_replay is None:self.first_replay=key
            wins={name:int(pick==actual) for name,pick in predictions.items()}
            self.hits.update({name:win for name,win in wins.items() if win})
            self.recent.append(wins)
            result={'period':key,'actual':actual,'predictions':predictions,
                'trainedThrough':self.window[-1][0],
                'completePreResultGroups':has_complete_hashes}
            self.last_rows.append(result)
            self.tested+=1
            self.seen+=1
            self.window.append(item)
            self.history.appendleft(actual)
            return result

    def report(self,status='running'):
        with self.lock:
            n=len(self.recent)
            recent_hits={name:sum(row.get(name,0) for row in self.recent) for name in METHODS}
            ranking=[]
            for name in METHODS:
                windows={}
                rows=list(self.recent)
                for size in (50,100,300,1000):
                    sample=rows[-size:];hits=sum(r.get(name,0) for r in sample)
                    windows[str(size)]={'tested':len(sample),'hits':hits,
                        'rate':round(100*hits/len(sample),2) if sample else None}
                cur_hit=cur_miss=best_hit=best_miss=run_hit=run_miss=0
                for row in rows:
                    if row.get(name,0):run_hit+=1;run_miss=0;best_hit=max(best_hit,run_hit)
                    else:run_miss+=1;run_hit=0;best_miss=max(best_miss,run_miss)
                for row in reversed(rows):
                    if row.get(name,0):
                        if cur_miss:break
                        cur_hit+=1
                    else:
                        if cur_hit:break
                        cur_miss+=1
                ranking.append({'method':name,'hits':self.hits[name],'tested':self.tested,
                      'rate':round(100*self.hits[name]/self.tested,2) if self.tested else None,
                      'recentHits':recent_hits[name],'recentTested':n,
                      'recentRate':round(100*recent_hits[name]/n,2) if n else None,
                      'windows':windows,'currentHitStreak':cur_hit,'currentMissStreak':cur_miss,
                      'bestHitStreak1000':best_hit,'bestMissStreak1000':best_miss})
            ranking.sort(key=lambda r:(-r['recentHits'], -r['hits'],r['method']))
            return {'status':status if self.seen>=TRAIN else 'waiting_for_1000',
                    'epoch':self.epoch,'archivePeriods':ARCHIVE_PERIODS,
                    'trainingPeriods':min(self.seen,TRAIN),'historicalPeriodsRead':self.seen,
                    'completePreResultPeriods':self.complete,'replayPeriods':self.tested,
                    'trainingFrom':self.training_from,'trainingThrough':self.training_through,
                    'replayFrom':self.first_replay,'replayThrough':self.last_key if self.tested else None,
                    'trainingWindow':TRAIN,'refitInterval':REFIT,'ranking':ranking,
                    'topTwo':[r['method'] for r in ranking[:2]] if self.tested>=200 else [],
                    'evaluationMode':'historical_walk_forward_only',
                    'labelPolicy':'each prediction is generated before that archived period result is added to training',
                    'usesUnseenFutureResults':False,'liveTrialEnabled':False,
                    'fixedArchiveOnly':True,'newPeriodsUsedForTraining':False,
                    'selfRepair':'refit every 100 replay periods; retune on each fresh archive cycle',
                    'omission':{str(i):{'current':self.omission_current[i],'max':self.omission_max[i]} for i in range(8)},
                    'recentPractice':list(reversed(self.last_rows)), 'lastProcessed':self.last_key}

    def snapshot(self):
        with self.lock:
            if len(self.window)<TRAIN:return None
            # Future predictions use ONLY the last 1000 observed outcomes.
            live={name:model.fit(list(self.window),names)
                  for name,names in model.FEATURE_FAMILIES.items()}
            rank=self.report()['topTwo']
            return {'researchVersion':core.RESEARCH_VERSION,
                    'trainedThrough':self.window[-1][0],'sample':self.seen,
                    'researchModels':live,'replayAlpha':dict(self.alphas),
                    'replayTopTwo':rank}


def history_pages(after):
    """Yield one short DB transaction's official results and groups at a time."""
    conn=core.db_connect(retries=0)
    if conn is None:raise RuntimeError('database unavailable')
    try:
        with conn.cursor(cursor_factory=core.RealDictCursor) as cur:
            cur.execute("""SELECT p.period_key,p.period_date,p.period_no,p.target_block,
                      p.single_count AS actual,g.group_no,g.block_hash,g.single_count
                FROM (SELECT period_key,period_date,period_no,target_block,single_count
                      FROM period_groups WHERE group_no=20 AND period_key>%s
                      ORDER BY period_key LIMIT %s) p
                LEFT JOIN period_groups g ON g.period_key=p.period_key
                    AND g.target_block=p.target_block AND g.group_no BETWEEN 1 AND 17
                ORDER BY p.period_key,g.group_no""",(after,PAGE))
            rows=cur.fetchall()
        return rows
    finally:core.db_release(conn)


def _persist(practice, status):
    report=practice.report(status)
    if status in ('catching_up','backfilling'):
        report['topTwo']=[]
    snapshot=(practice.snapshot() if practice.last_key!=practice.saved_at
              or status!=practice.saved_status else None)
    if snapshot and status in ('catching_up','backfilling'):
        # Do not trial a winner selected from only a fraction of the archive.
        snapshot['replayTopTwo']=[]
    conn=core.db_connect(retries=0)
    if conn is None:raise RuntimeError('database unavailable')
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO research_replay_runtime(singleton,research_version,report,updated_at)
                    VALUES(1,%s,%s::jsonb,NOW()) ON CONFLICT(singleton) DO UPDATE SET
                    research_version=EXCLUDED.research_version,report=EXCLUDED.report,
                    updated_at=NOW()""",(core.RESEARCH_VERSION,json.dumps(report)))
                if snapshot:
                    cur.execute("""INSERT INTO hash_model_runtime(singleton,trained_through,snapshot,updated_at)
                        VALUES(1,%s,%s::jsonb,NOW()) ON CONFLICT(singleton) DO UPDATE SET
                        trained_through=EXCLUDED.trained_through,snapshot=EXCLUDED.snapshot,
                        updated_at=NOW()""",(snapshot['trainedThrough'],json.dumps(snapshot)))
        if snapshot:practice.saved_at=practice.last_key
        practice.saved_status=status
    finally:core.db_release(conn)


def _save_practice_rows(rows):
    if not rows:return
    from psycopg2.extras import execute_values
    conn=core.db_connect(retries=0)
    if conn is None:raise RuntimeError('database unavailable')
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur,"""INSERT INTO research_practice_results
                    (research_version,period_key,trained_through,actual_single,predictions,hashes_complete)
                    VALUES %s ON CONFLICT(research_version,period_key) DO UPDATE SET
                    trained_through=EXCLUDED.trained_through,
                    actual_single=EXCLUDED.actual_single,
                    predictions=EXCLUDED.predictions,
                    hashes_complete=EXCLUDED.hashes_complete""",
                    [(core.RESEARCH_VERSION,r['period'],r['trainedThrough'],r['actual'],
                      json.dumps(r['predictions']),r['completePreResultGroups']) for r in rows])
    finally:core.db_release(conn)


def practice_worker():
    time.sleep(30)  # allow collection and the backfill checkpoint to initialize
    import backfill
    epoch=1
    state=Practice(epoch)
    last_save=time.monotonic()
    pending=[]
    known_revision=None
    backfill_info=None
    checked_at=0
    while True:
        core.worker_touch('practice')
        try:
            if time.monotonic()-checked_at>8:
                backfill_info=backfill.read_status()
                checked_at=time.monotonic()
                if backfill_info:
                    rev=int(backfill_info['revision'])
                    if known_revision is None:known_revision=rev
                    elif (rev!=known_revision and backfill_info['status']=='complete'):
                        # Recompute chronological history when earlier blocks
                        # have been restored. Retrospective rows are revised,
                        # genuine future trials remain immutable.
                        if pending:_save_practice_rows(pending);pending=[]
                        epoch+=1;state=Practice(epoch);last_save=0
                        known_revision=rev
            if pending:
                _save_practice_rows(pending)
                pending=[]
            if state.seen>=ARCHIVE_PERIODS:
                _persist(state,'archive_cycle_complete')
                # Forget the learned labels/models and replay the SAME fixed archive
                # again. New/live periods are deliberately excluded from research.
                epoch+=1
                state=Practice(epoch)
                pending=[]
                last_save=time.monotonic()
                time.sleep(2)
            rows=history_pages(state.last_key or '')
            # Never let a cycle consume more than the fixed 10,000 archived periods.
            if rows and state.seen + len({r['period_key'] for r in rows}) > ARCHIVE_PERIODS:
                allowed=[];keys=[]
                for r in rows:
                    if r['period_key'] not in keys:
                        if state.seen+len(keys)>=ARCHIVE_PERIODS: break
                        keys.append(r['period_key'])
                    allowed.append(r)
                rows=allowed
            if not rows:
                if time.monotonic()-last_save>20:
                    active=backfill_info and backfill_info['status']!='complete'
                    _persist(state,'backfilling' if active else 'following_new_periods')
                    last_save=time.monotonic()
                time.sleep(4);continue
            period=None;groups={};header=None
            def finish():
                if header and core.calibrated_group20_row(header):
                    record=state.consume(period,groups,int(header['actual']))
                    if record:pending.append(record)
            for row in rows:
                key=row['period_key']
                if period is not None and key!=period:
                    finish();groups={}
                period=key;header=row
                if row['group_no'] is not None:
                    groups[str(row['group_no'])]={
                        'block':row['block_hash'],'singleCount':row['single_count']}
            deferred=False
            if period and len(groups)<17:
                ds,p,_,_=core.current_period()
                current_index=core.period_index(ds,p)
                row_date=header['period_date'].isoformat() if hasattr(header['period_date'],'isoformat') else str(header['period_date'])
                age=current_index-core.period_index(row_date,header['period_no'])
                # Allow the independent group writer to finish this latest
                # official result before treating missing hashes as missing.
                deferred=0<=age<=1
            if not deferred:finish()
            if period and not deferred and state.last_key!=period:
                # An uncalibrated row must not pin the pagination cursor.
                state.last_key=period
            if pending:
                _save_practice_rows(pending)
                pending=[]
            count=len({row['period_key'] for row in rows})
            if time.monotonic()-last_save>5 or count<PAGE:
                active=backfill_info and backfill_info['status']!='complete'
                label='backfilling' if active else ('catching_up' if count==PAGE else 'following_new_periods')
                _persist(state,label)
                last_save=time.monotonic()
            # Avoid a long-held read transaction or a tight scan on a small DB.
            time.sleep(2 if deferred else .25)
        except Exception as exc:
            core.worker_touch('practice',exc)
            time.sleep(5)


def live_trial_worker():
    import backfill
    early=None;after_g17=None
    def ready_snapshot(key):
        snapshot=core.get_hash_model_snapshot(key)
        if snapshot and snapshot.get('researchVersion')!=core.RESEARCH_VERSION:
            return None
        state=backfill.read_status()
        if snapshot and (not state or state['status']!='complete'):
            return {**snapshot,'replayTopTwo':[]}
        return snapshot
    while True:
        core.worker_touch('trial')
        try:
            ds,p,_,_=core.current_period();key=f'{ds}:{int(p):04d}'
            target=core.period_target_block(ds,p)
            if early!=key:
                snapshot=ready_snapshot(key)
                history=core.get_historical_official_singles(ds,p,limit=30)
                if core.save_research_forecasts(ds,p,target,history,snapshot):early=key
            if after_g17!=key:
                with core._live_blocks_lock:
                    g17_ready=(target-3 in core._live_blocks)
                if g17_ready and not core._target_block_observed(target):
                    groups=core._reconstruct_period_groups_dbfirst(ds,p,target)
                    if all(str(i) in groups for i in range(1,18)):
                        snap=ready_snapshot(key)
                        history=core.get_historical_official_singles(ds,p,limit=30)
                        if core.save_research_forecasts(ds,p,target,history,snap,groups):
                            after_g17=key
        except Exception as exc:core.worker_touch('trial',exc)
        time.sleep(.75)


def verify_worker():
    while True:
        core.worker_touch('trial-verify')
        conn=None
        try:
            conn=core.db_connect(retries=0)
            if conn:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute("""UPDATE research_predictions p SET actual_single=b.single_count,
                            verified_at=NOW(),valid_pre_result=(p.locked_at<b.block_time)
                            FROM tron_blocks b WHERE p.target_block=b.block_number
                              AND b.block_time IS NOT NULL AND p.verified_at IS NULL
                              AND p.model_version=%s""",(core.RESEARCH_VERSION,))
        except Exception as exc:core.worker_touch('trial-verify',exc)
        finally:
            if conn is not None:core.db_release(conn)
        time.sleep(3)
