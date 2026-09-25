"""V10.2 two-stage archive loader.

Stage 1 downloads the bounded historical TRON block span into a permanent raw
warehouse in large range requests. Stage 2 rebuilds period_groups locally from
that warehouse. Missing/invalid hashes are never guessed.
"""
from datetime import date
import json
import time

import collector_core as core

TARGET_PERIODS=10000
RANGE_CHUNK=100
_live_priority_until=0.0

def mark_live_priority(seconds=2.0):
    global _live_priority_until
    _live_priority_until=max(_live_priority_until,time.monotonic()+float(seconds))


def period_from_index(index):
    ordinal,zero=divmod(int(index),1440)
    return date.fromordinal(ordinal).isoformat(),zero+1


def _bounds():
    ds,p,_,_=core.current_period()
    end=core.period_index(ds,p)-1
    start=end-TARGET_PERIODS+1
    start_ds,start_p=period_from_index(start)
    end_ds,end_p=period_from_index(end)
    first=core.period_target_block(start_ds,start_p)-19
    last=core.period_target_block(end_ds,end_p)
    return start,end,first,last


def read_status():
    conn=core.db_connect(retries=0)
    if conn is None:raise RuntimeError('database unavailable')
    try:
        with conn.cursor(cursor_factory=core.RealDictCursor) as cur:
            cur.execute('SELECT * FROM backfill_runtime WHERE singleton=1')
            row=cur.fetchone()
            cur.execute('SELECT * FROM raw_backfill_runtime WHERE singleton=1')
            raw=cur.fetchone()
            if row and raw:
                row=dict(row);row['raw']=dict(raw)
            return row
    finally:core.db_release(conn)


def _initialize():
    start,end,first,last=_bounds()
    conn=core.db_connect(retries=0)
    if conn is None:raise RuntimeError('database unavailable')
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO backfill_runtime
                    (singleton,start_index,end_index,next_index,status)
                    VALUES(1,%s,%s,%s,'waiting_raw') ON CONFLICT(singleton) DO NOTHING""",
                    (start,end,start))
                cur.execute("""INSERT INTO raw_backfill_runtime
                    (singleton,start_block,end_block,next_block,status)
                    VALUES(1,%s,%s,%s,'downloading') ON CONFLICT(singleton) DO NOTHING""",
                    (first,last,first))
    finally:core.db_release(conn)


def _live_has_priority():
    return time.monotonic() < _live_priority_until


def _normalize(raw):
    header=(raw.get('block_header') or {}).get('raw_data') or {}
    number=header.get('number');h=raw.get('blockID');timestamp=header.get('timestamp')
    if number is None or not isinstance(h,str) or len(h)!=64 or not all(c in '0123456789abcdefABCDEF' for c in h):
        raise ValueError('invalid chain block number or hash')
    if timestamp is None or int(timestamp)<1_000_000_000_000:raise ValueError('missing chain timestamp')
    nums=core.calc_numbers(h)
    if len(nums)!=7:raise ValueError('not enough valid numbers in historic hash')
    return {'number':int(number),'block':h,'timestamp':int(timestamp),
            'numbers':[f'{v:02d}' for v in nums],'singleCount':core.calc_single_count(nums)}


def _range_blocks(start,end):
    if _live_has_priority():raise RuntimeError('live collection has priority')
    url=f'{core.TRON_SOLIDITY_API}/getblockbylimitnext'
    payload=core._get_json(url,method='POST',payload={'startNum':int(start),'endNum':int(end)})
    if isinstance(payload,dict) and payload.get('Error'):raise RuntimeError('TRON range API: '+str(payload['Error'])[:120])
    values=payload.get('block') if isinstance(payload,dict) else payload
    if not isinstance(values,list):raise RuntimeError('TRON range API returned no block list')
    out=[];seen=set()
    for row in values:
        normalized=_normalize(row);height=normalized['number']
        if height in seen or not start<=height<end:raise ValueError('unexpected height in chain range')
        seen.add(height);out.append(normalized)
    expected=set(range(start,end))
    if seen!=expected:raise RuntimeError(f'range incomplete {start}-{end-1}: got {len(seen)}/{len(expected)}')
    return out


def _save_raw(rows):
    if not rows:return
    from psycopg2.extras import execute_values
    conn=core.db_connect(retries=0)
    if conn is None:raise RuntimeError('database unavailable')
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur,"""INSERT INTO historical_raw_blocks
                    (block_number,block_hash,block_time,numbers,single_count)
                    VALUES %s ON CONFLICT(block_number) DO UPDATE SET
                    block_hash=EXCLUDED.block_hash,block_time=EXCLUDED.block_time,
                    numbers=EXCLUDED.numbers,single_count=EXCLUDED.single_count""",
                    [(r['number'],r['block'],core.datetime.fromtimestamp(r['timestamp']/1000,core.timezone.utc),
                      json.dumps(r['numbers']),r['singleCount']) for r in rows])
    finally:core.db_release(conn)


def _raw_count(start,end):
    conn=core.db_connect(retries=0)
    if conn is None:return 0
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM historical_raw_blocks WHERE block_number BETWEEN %s AND %s',(start,end))
            return int(cur.fetchone()[0])
    finally:core.db_release(conn)


def raw_download_worker():
    time.sleep(12);consecutive=0
    while True:
        core.worker_touch('raw-backfill')
        try:
            _initialize();state=read_status();raw=state.get('raw') if state else None
            if not raw or raw['status']=='complete':time.sleep(120);continue
            start=int(raw['next_block']);end=int(raw['end_block'])
            if start>end:
                count=_raw_count(int(raw['start_block']),end)
                expected=end-int(raw['start_block'])+1
                status='complete' if count==expected else 'verify_failed'
                conn=core.db_connect(retries=0)
                try:
                    with conn:
                        with conn.cursor() as cur:cur.execute("UPDATE raw_backfill_runtime SET status=%s,stored_blocks=%s,last_error=%s,updated_at=NOW() WHERE singleton=1",(status,count,None if status=='complete' else f'warehouse {count}/{expected}'))
                finally:core.db_release(conn)
                time.sleep(10);continue
            if _live_has_priority():time.sleep(.5);continue
            stop=min(start+RANGE_CHUNK,end+1)
            rows=_range_blocks(start,stop);_save_raw(rows)
            conn=core.db_connect(retries=0)
            try:
                with conn:
                    with conn.cursor() as cur:cur.execute("UPDATE raw_backfill_runtime SET next_block=%s,stored_blocks=stored_blocks+%s,status='downloading',last_error=NULL,updated_at=NOW() WHERE singleton=1",(stop,len(rows)))
            finally:core.db_release(conn)
            consecutive=0
        except Exception as exc:
            consecutive+=1;core.worker_touch('raw-backfill',exc)
            try:
                conn=core.db_connect(retries=0)
                if conn:
                    with conn:
                        with conn.cursor() as cur:cur.execute("UPDATE raw_backfill_runtime SET failed_batches=failed_batches+1,last_error=%s,updated_at=NOW() WHERE singleton=1",(str(exc)[:220],))
                    core.db_release(conn)
            except Exception:pass
            time.sleep(min(30,2**min(consecutive,5)))


def _existing(period_key,target):
    conn=core.db_connect(retries=0)
    if conn is None:raise RuntimeError('database unavailable')
    try:
        with conn.cursor(cursor_factory=core.RealDictCursor) as cur:
            cur.execute("""SELECT group_no,block_number,block_hash,numbers,single_count FROM period_groups
                WHERE period_key=%s AND target_block=%s AND group_no BETWEEN 1 AND 20""",(period_key,target))
            return {int(r['group_no']):r for r in cur.fetchall()}
    finally:core.db_release(conn)


def _verified_row(row,expected):
    if int(row['block_number'])!=expected or len(str(row['block_hash']))!=64:return False
    try:
        nums=row['numbers'];nums=json.loads(nums) if isinstance(nums,str) else nums
        calculated=core.calc_numbers(row['block_hash'])
        return nums==[f'{n:02d}' for n in calculated] and int(row['single_count'])==core.calc_single_count(calculated)
    except Exception:return False


def _warehouse_period(first,last):
    conn=core.db_connect(retries=0)
    if conn is None:raise RuntimeError('database unavailable')
    try:
        with conn.cursor(cursor_factory=core.RealDictCursor) as cur:
            cur.execute("""SELECT block_number,block_hash,numbers,single_count FROM historical_raw_blocks
                WHERE block_number BETWEEN %s AND %s ORDER BY block_number""",(first,last))
            return {int(r['block_number']):r for r in cur.fetchall()}
    finally:core.db_release(conn)


def fetch_period(index):
    ds,p=period_from_index(index);key=f'{ds}:{p:04d}';target=core.period_target_block(ds,p)
    archived=_existing(key,target)
    valid={g:r for g,r in archived.items() if _verified_row(r,target-(20-g))}
    if len(valid)==20:return 'already'
    raw=_warehouse_period(target-19,target)
    groups={}
    for g in range(1,21):
        height=target-(20-g);r=valid.get(g) or raw.get(height)
        if not r:raise RuntimeError(f'raw warehouse missing block {height}')
        nums=r['numbers'];nums=json.loads(nums) if isinstance(nums,str) else nums
        if not _verified_row({'block_number':height,'block_hash':r['block_hash'],'numbers':nums,'single_count':r['single_count']},height):
            raise ValueError(f'raw warehouse verification failed {height}')
        groups[str(g)]={'blockNumber':height,'block':r['block_hash'],'numbers':nums,'singleCount':int(r['single_count'])}
    core.persist_period_groups(ds,p,groups,target);return 'fetched'


def _record(index,result,error=None):
    conn=core.db_connect(retries=0)
    if conn is None:raise RuntimeError('database unavailable')
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""UPDATE backfill_runtime SET next_index=GREATEST(next_index,%s),
                    fetched_periods=fetched_periods+%s,already_present=already_present+%s,
                    failed_periods=failed_periods+%s,last_error=%s,updated_at=NOW() WHERE singleton=1""",
                    (index+1,int(result=='fetched'),int(result=='already'),int(error is not None),str(error)[:220] if error else None))
    finally:core.db_release(conn)


def _status(status,error=None,revision=False):
    conn=core.db_connect(retries=0)
    if conn is None:return
    try:
        with conn:
            with conn.cursor() as cur:cur.execute("UPDATE backfill_runtime SET status=%s,last_error=%s,revision=revision+%s,updated_at=NOW() WHERE singleton=1",(status,str(error)[:220] if error else None,1 if revision else 0))
    finally:core.db_release(conn)


def backfill_worker():
    time.sleep(20)
    while True:
        core.worker_touch('backfill')
        try:
            _initialize();state=read_status();raw=(state or {}).get('raw') or {}
            if raw.get('status')!='complete':_status('waiting_raw');time.sleep(5);continue
            if state['status']=='complete':time.sleep(300);continue
            index=int(state['next_index']);end=int(state['end_index'])
            if index>end:
                _status('complete',revision=True);core.rebuild_omission_runtime();time.sleep(300);continue
            try:result=fetch_period(index);_record(index,result)
            except Exception as exc:_record(index,'failed',exc);core.worker_touch('backfill',exc)
            _status('local_rebuild')
            time.sleep(.02)
        except Exception as exc:
            core.worker_touch('backfill',exc);time.sleep(5)
