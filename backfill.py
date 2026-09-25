"""Resume a bounded 10,000-period historical chain import in the background.

Never derive a result from a missing hash. Live block collection always has
priority over historical HTTP calls; range scans share its rate governor.
"""
from datetime import date, datetime, timedelta, timezone
import json
import time

import collector_core as core

TARGET_PERIODS=10000
MIN_HTTP_INTERVAL=4.0
_last_http=0.0


def period_from_index(index):
    ordinal,zero=divmod(int(index),1440)
    return date.fromordinal(ordinal).isoformat(),zero+1


def read_status():
    conn=core.db_connect(retries=0)
    if conn is None:raise RuntimeError('database unavailable')
    try:
        with conn.cursor(cursor_factory=core.RealDictCursor) as cur:
            cur.execute('SELECT * FROM backfill_runtime WHERE singleton=1')
            return cur.fetchone()
    finally:core.db_release(conn)


def _initialize():
    ds,p,_,_=core.current_period()
    end=core.period_index(ds,p)-1
    start=end-TARGET_PERIODS+1
    conn=core.db_connect(retries=0)
    if conn is None:raise RuntimeError('database unavailable')
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO backfill_runtime
                    (singleton,start_index,end_index,next_index,status)
                    VALUES(1,%s,%s,%s,'scanning') ON CONFLICT(singleton) DO NOTHING""",
                    (start,end,start))
    finally:core.db_release(conn)


def _live_has_priority():
    if core._result_db_write_queue.qsize() or core._db_write_queue.qsize()>50:return True
    countdown=core.calibrated_countdown_value()
    if countdown<=20 or countdown>=59:return True
    with core._live_blocks_lock:
        recent=core._live_blocks.get(core._live_latest_number)
    if not recent:return True
    stamp=core._block_timestamp_seconds(recent.get('timestamp'))
    return stamp is None or time.time()-stamp>8


def _existing(period_key,target):
    conn=core.db_connect(retries=0)
    if conn is None:raise RuntimeError('database unavailable')
    try:
        with conn.cursor(cursor_factory=core.RealDictCursor) as cur:
            cur.execute("""SELECT group_no,block_number,block_hash,numbers,single_count
                FROM period_groups WHERE period_key=%s AND target_block=%s
                  AND group_no BETWEEN 1 AND 20""",(period_key,target))
            return {int(r['group_no']):r for r in cur.fetchall()}
    finally:core.db_release(conn)


def _normalize(raw):
    header=(raw.get('block_header') or {}).get('raw_data') or {}
    number=header.get('number')
    h=raw.get('blockID')
    timestamp=header.get('timestamp')
    if number is None or not isinstance(h,str) or len(h)!=64 or not all(c in '0123456789abcdefABCDEF' for c in h):
        raise ValueError('invalid chain block number or hash')
    if timestamp is None or int(timestamp)<1_000_000_000_000:
        raise ValueError('missing chain timestamp')
    nums=core.calc_numbers(h)
    if len(nums)!=7:raise ValueError('not enough valid numbers in historic hash')
    return {'number':int(number),'block':h,'timestamp':int(timestamp),
            'numbers':[f'{v:02d}' for v in nums],
            'singleCount':core.calc_single_count(nums)}


def _range_blocks(start,end):
    global _last_http
    if _live_has_priority():raise RuntimeError('live collection has priority')
    remaining=MIN_HTTP_INTERVAL-(time.monotonic()-_last_http)
    if remaining>0:time.sleep(remaining)
    if _live_has_priority():raise RuntimeError('live collection has priority')
    _last_http=time.monotonic()
    url=f'{core.TRON_SOLIDITY_API}/getblockbylimitnext'
    payload=core._get_json(url,method='POST',payload={'startNum':int(start),'endNum':int(end)})
    if isinstance(payload,dict) and payload.get('Error'):
        raise RuntimeError('TRON range API: '+str(payload['Error'])[:120])
    values=payload.get('block') if isinstance(payload,dict) else payload
    if not isinstance(values,list):raise RuntimeError('TRON range API returned no block list')
    out={}
    for row in values:
        normalized=_normalize(row)
        height=normalized['number']
        if height in out or not start<=height<end:raise ValueError('unexpected height in chain range')
        out[height]=normalized
    return out


def _verified_row(row,expected):
    if int(row['block_number'])!=expected or len(str(row['block_hash']))!=64:return False
    try:
        from_hash=core.calc_numbers(row['block_hash'])
        nums=row['numbers']
        if isinstance(nums,str):nums=json.loads(nums)
        return (len(from_hash)==7 and nums==[f'{n:02d}' for n in from_hash] and
                int(row['single_count'])==core.calc_single_count(from_hash))
    except (TypeError,ValueError,RuntimeError):return False


def fetch_period(index):
    """Save 20 authentic block hashes for one period or retain a retry entry."""
    ds,p=period_from_index(index)
    key=f'{ds}:{p:04d}'
    target=core.period_target_block(ds,p)
    archived=_existing(key,target)
    valid={g:r for g,r in archived.items()
           if _verified_row(r,target-(20-g))}
    if len(valid)==20:return 'already'
    missing={target-(20-g) for g in range(1,21) if g not in valid}
    raw=core.get_db_blocks(missing)
    observed={}
    for height,r in raw.items():
        if int(height) not in missing:continue
        try:
            nums=core.calc_numbers(r['block_hash'])
            if len(nums)!=7:continue
            observed[int(height)]={'number':int(height),'block':r['block_hash'],
                'numbers':[f'{n:02d}' for n in nums],
                'singleCount':core.calc_single_count(nums)}
        except (TypeError,ValueError,RuntimeError):continue
    remaining=missing-set(observed)
    if remaining:
        chain=_range_blocks(target-19,target+1)
        for height in remaining:
            if height not in chain:raise RuntimeError(f'missing chain height {height}')
            observed[height]=chain[height]
    groups={}
    for g in range(1,21):
        height=target-(20-g)
        if g in valid:
            r=valid[g]
            groups[str(g)]={'blockNumber':height,'block':r['block_hash'],
                            'numbers':r['numbers'] if isinstance(r['numbers'],list) else json.loads(r['numbers']),
                            'singleCount':int(r['single_count'])}
        else:
            r=observed[height]
            if len(str(r['block']))!=64 or len(r['numbers'])!=7:
                raise ValueError('incomplete verified block')
            groups[str(g)]={'blockNumber':height,'block':r['block'],
                            'numbers':r['numbers'],'singleCount':r['singleCount']}
    core.persist_period_groups(ds,p,groups,target)
    return 'fetched'


def _record(index,result,error=None,retry=False):
    conn=core.db_connect(retries=0)
    if conn is None:raise RuntimeError('database unavailable')
    try:
        with conn:
            with conn.cursor() as cur:
                if error:
                    cur.execute("""INSERT INTO backfill_failures(period_index,reason,attempts,last_attempt)
                        VALUES(%s,%s,1,NOW()) ON CONFLICT(period_index) DO UPDATE SET
                        reason=EXCLUDED.reason,attempts=backfill_failures.attempts+1,last_attempt=NOW()""",
                        (index,str(error)[:220]))
                elif retry:
                    cur.execute('DELETE FROM backfill_failures WHERE period_index=%s',(index,))
                changes=[];params=[]
                if not retry:changes.append('next_index=GREATEST(next_index,%s)');params.append(index+1)
                if result=='fetched':changes.append('fetched_periods=fetched_periods+1')
                if result=='already':changes.append('already_present=already_present+1')
                if error and not retry:changes.append('failed_periods=failed_periods+1')
                if retry and not error:changes.append('failed_periods=GREATEST(0,failed_periods-1)')
                changes+=['last_error=%s','updated_at=NOW()'];params.append(str(error)[:220] if error else None)
                params.append(1)
                cur.execute('UPDATE backfill_runtime SET '+','.join(changes)+' WHERE singleton=%s',params)
    finally:core.db_release(conn)


def _next_retry():
    conn=core.db_connect(retries=0)
    if conn is None:raise RuntimeError('database unavailable')
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT period_index FROM backfill_failures
                WHERE last_attempt<NOW()-INTERVAL '10 minutes'
                ORDER BY attempts,last_attempt LIMIT 1""")
            row=cur.fetchone()
            return int(row[0]) if row else None
    finally:core.db_release(conn)


def _status(status,error=None,revision=False):
    conn=core.db_connect(retries=0)
    if conn is None:raise RuntimeError('database unavailable')
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""UPDATE backfill_runtime SET status=%s,last_error=%s,
                    revision=revision+%s,updated_at=NOW() WHERE singleton=1""",
                    (status,str(error)[:220] if error else None,1 if revision else 0))
    finally:core.db_release(conn)


def backfill_worker():
    time.sleep(15)
    consecutive=0
    while True:
        core.worker_touch('backfill')
        try:
            _initialize()
            state=read_status()
            if state['status']=='complete':
                time.sleep(300);continue
            index=int(state['next_index'])
            retry=False
            if index>int(state['end_index']):
                index=_next_retry()
                if index is None:
                    if int(state['failed_periods'])==0:
                        _status('complete',revision=state['status']!='complete')
                        core.rebuild_omission_runtime()
                    else:_status('retrying',state['last_error'])
                    time.sleep(40);continue
                retry=True
            if _live_has_priority():time.sleep(2);continue
            try:
                outcome=fetch_period(index)
                _record(index,outcome,retry=retry)
                consecutive=0
            except Exception as exc:
                if str(exc)=='live collection has priority':
                    time.sleep(2);continue
                _record(index,None,error=exc,retry=retry)
                consecutive+=1
                if '429' in str(exc) or '403' in str(exc):time.sleep(120)
                elif consecutive>=10:
                    _status('retrying',exc)
                    time.sleep(120);consecutive=0
            time.sleep(.2)
        except Exception as exc:
            core.worker_touch('backfill',exc)
            time.sleep(10)
