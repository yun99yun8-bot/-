from pathlib import Path
from flask import Flask, Response, jsonify, request
from urllib.request import urlopen, Request
from urllib.error import HTTPError
import json
import os
import time
import threading
import queue
import socket
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
import hash_research
import research_selection

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2.pool import ThreadedConnectionPool
except Exception:
    psycopg2 = None

BASE_DIR = Path(__file__).resolve().parent
app = Flask(__name__, static_folder=str(BASE_DIR), static_url_path='')

# Keep the same TRONGrid interface family already used by the previous version.
TRON_API = 'https://api.trongrid.io/wallet'
TRON_NOWBLOCK = f'{TRON_API}/getnowblock'
TRON_BLOCK_BY_NUM = f'{TRON_API}/getblockbynum'
TRON_SOLIDITY_API = 'https://api.trongrid.io/walletsolidity'
TRON_SOLIDITY_NOWBLOCK = f'{TRON_SOLIDITY_API}/getnowblock'
TRON_SOLIDITY_BLOCK_BY_NUM = f'{TRON_SOLIDITY_API}/getblockbynum'
TRONSCAN_BLOCK = 'https://apilist.tronscan.org/api/block'
TRONSCAN_LATEST = 'https://apilist.tronscan.org/api/block/latest'

CN_TZ = timezone(timedelta(hours=8))
STATE_FILE = BASE_DIR / 'draw_state.json'
DATABASE_URL = os.environ.get('DATABASE_URL', '').strip()
DB_RETENTION_DAYS = 7
_worker_started = False
_worker_lock = threading.Lock()
_db_pool = None
_db_pool_lock = threading.Lock()
_draw_cache = None
_draw_cache_lock = threading.Lock()
_live_blocks = {}
_live_blocks_lock = threading.Lock()
_db_write_queue = queue.Queue(maxsize=1000)
_result_db_write_queue = queue.Queue(maxsize=32)
_live_latest_number = None
_relation_cache = {'key': None, 'at': 0, 'value': None}
_relation_cache_lock = threading.Lock()
_fast_diag = {'target': None, 'provider': None, 'firstSeenAt': None, 'latencyMs': None, 'errors': {}}
_fast_diag_lock = threading.Lock()
_period_group_cache = {}
_period_group_cache_lock = threading.Lock()
_omission_bootstrap_lock = threading.Lock()
_omission_bootstrapped = False
_history_cache = {'key': None, 'at': 0, 'value': None}
_history_cache_lock = threading.Lock()
_ai_summary_cache = {'key': None, 'at': 0, 'value': None}
_ai_summary_cache_lock = threading.Lock()
_pending_ai_predictions = {}
_pending_ai_lock = threading.Lock()
_prediction_context = {'key': None, 'value': None}
_prediction_context_lock = threading.Lock()
_runtime_health = {'lastAiError': None, 'lastDbError': None, 'lastRepairAt': None, 'repairCount': 0, 'workerHeartbeats': {}, 'workerErrors': {}}
_runtime_health_lock = threading.Lock()
MODEL_VERSION = 'v10.2-fast-raw-warehouse-old-only-training'
RESEARCH_VERSION = 'research-family-v5-fixed-10000-loop'
RESEARCH_ONLY_MODE = True

_historical_singles_cache = {'key': None, 'at': 0, 'value': None}
_historical_singles_cache_lock = threading.Lock()
_worker_threads = {}
_worker_threads_lock = threading.Lock()
_worker_restarts = {}
AUTONOMOUS_STATES = ('COLLECTING','G17_READY','AI_LOCK_ATTEMPT','AI_LOCKED','WAIT_G20','RESULT_READY','VERIFIED','COMPLETE','MISSED_PREDICTION')

# V8.1.4 shared upstream rate governor. All TRON HTTP paths share it so
# independent engines cannot accidentally hammer the same public APIs.
_tron_http_lock = threading.Lock()
_tron_last_request_at = 0.0
_tron_cooldown_until = 0.0
_tron_rate_diag = {'last429At': None, 'cooldownUntil': None, 'requestCount': 0, '429Count': 0}
TRON_MIN_REQUEST_INTERVAL = 0.85
TRON_429_COOLDOWN_SECONDS = 18.0
TRON_PRO_API_KEY = os.environ.get('TRON_PRO_API_KEY', '').strip()

# V8.1.5 event-driven prediction path. G17 publication emits a local event;
# G20 has a pre-publication barrier that gives DB/RAM reconstruction one final
# chance to durably lock the already-available pre-result snapshot.
_g17_event_queue = queue.Queue(maxsize=32)
_g17_event_seen = set()
_g17_first_seen = {}
_g17_event_lock = threading.Lock()
_g17_durable = set()
_g17_durable_lock = threading.Lock()
_barrier_diag = {'g17Events': 0, 'g17Locks': 0, 'last': None}
_barrier_diag_lock = threading.Lock()

def _tron_rate_wait():
    global _tron_last_request_at
    while True:
        with _tron_http_lock:
            now=time.monotonic()
            wait=max(_tron_cooldown_until-now, TRON_MIN_REQUEST_INTERVAL-(now-_tron_last_request_at), 0.0)
            if wait <= 0:
                _tron_last_request_at=now
                _tron_rate_diag['requestCount'] += 1
                return
        time.sleep(min(wait, 1.0))

def _tron_mark_429():
    global _tron_cooldown_until
    with _tron_http_lock:
        _tron_cooldown_until=max(_tron_cooldown_until, time.monotonic()+TRON_429_COOLDOWN_SECONDS)
        _tron_rate_diag['429Count'] += 1
        _tron_rate_diag['last429At']=datetime.now(CN_TZ).isoformat(timespec='seconds')
        _tron_rate_diag['cooldownUntil']=(datetime.now(CN_TZ)+timedelta(seconds=TRON_429_COOLDOWN_SECONDS)).isoformat(timespec='seconds')



def worker_touch(name, error=None):
    now=datetime.now(CN_TZ).isoformat(timespec='seconds')
    with _runtime_health_lock:
        _runtime_health.setdefault('workerHeartbeats', {})[name]=now
        if error is not None:
            _runtime_health.setdefault('workerErrors', {})[name]=str(error)[:300]


def current_period():
    # Keep backend period selection on the same calibrated clock as the UI.
    # The platform display was measured about 4 seconds behind Beijing time.
    now = datetime.now(CN_TZ) - timedelta(seconds=4)
    if now.hour == 0 and now.minute == 0:
        # 00:00 is the closing minute of the previous day's period 1440.
        date_for_period = now.date() - timedelta(days=1)
        period = 1440
    else:
        date_for_period = now.date()
        period = now.hour * 60 + now.minute
    date_str = date_for_period.strftime('%Y-%m-%d')
    period_str = f'{period:04d}'
    platform_period = date_for_period.strftime('%y%m%d') + period_str
    return date_str, period, period_str, platform_period


def period_index(date_str, period):
    d = datetime.strptime(date_str, '%Y-%m-%d').date()
    return d.toordinal() * 1440 + (int(period) - 1)


def _get_json(url, method='GET', payload=None):
    # Every public TRON request passes through one process-wide governor.
    _tron_rate_wait()
    data = json.dumps(payload).encode('utf-8') if payload is not None else None
    headers={'Content-Type': 'application/json', 'User-Agent': 'TornMonitor/2.2'}
    if TRON_PRO_API_KEY and urlparse(url).hostname == 'api.trongrid.io':
        headers['TRON-PRO-API-KEY']=TRON_PRO_API_KEY
    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=4) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except HTTPError as exc:
        if getattr(exc, 'code', None) == 429:
            _tron_mark_429()
        raise

def fetch_latest_block():
    errors = []
    for url in (TRON_NOWBLOCK, TRON_SOLIDITY_NOWBLOCK):
        try:
            data = _get_json(url)
            block_id = data.get('blockID')
            number = data.get('block_header', {}).get('raw_data', {}).get('number')
            timestamp = data.get('block_header', {}).get('raw_data', {}).get('timestamp')
            if block_id and isinstance(block_id, str) and number is not None:
                return {'block': block_id, 'number': int(number), 'timestamp': timestamp}
            errors.append(url + ': invalid response')
        except Exception as e:
            errors.append(url + ': ' + str(e))
    try:
        data = _get_json(TRONSCAN_LATEST)
        row = (data.get('data') or [None])[0] if isinstance(data, dict) else None
        if row and row.get('hash') and row.get('number') is not None:
            return {'block': row['hash'], 'number': int(row['number']), 'timestamp': row.get('timestamp')}
        if isinstance(data, dict) and data.get('number') is not None and data.get('hash'):
            return {'block': data['hash'], 'number': int(data['number']), 'timestamp': data.get('timestamp')}
        errors.append(TRONSCAN_LATEST + ': invalid response')
    except Exception as e:
        errors.append(TRONSCAN_LATEST + ': ' + str(e))
    raise RuntimeError('TRON 最新区块接口不可用；' + ' | '.join(errors))

def fetch_block_by_number(number):
    errors = []
    payload = {'num': int(number)}
    for url in (TRON_BLOCK_BY_NUM, TRON_SOLIDITY_BLOCK_BY_NUM):
        try:
            data = _get_json(url, method='POST', payload=payload)
            block_id = data.get('blockID')
            if block_id and isinstance(block_id, str):
                return {
                    'block': block_id,
                    'number': int(data.get('block_header', {}).get('raw_data', {}).get('number', number)),
                    'timestamp': data.get('block_header', {}).get('raw_data', {}).get('timestamp')
                }
            errors.append(url + ': block not found')
        except Exception as e:
            errors.append(url + ': ' + str(e))
    try:
        data = _get_json(f'{TRONSCAN_BLOCK}?number={int(number)}')
        rows = data.get('data') if isinstance(data, dict) else None
        row = rows[0] if isinstance(rows, list) and rows else None
        if row and row.get('hash'):
            return {'block': row['hash'], 'number': int(row.get('number', number)), 'timestamp': row.get('timestamp')}
        errors.append('TRONScan: block not found')
    except Exception as e:
        errors.append('TRONScan: ' + str(e))
    raise RuntimeError(f'目标区块 {number} 尚未可读取；' + ' | '.join(errors))

def fetch_block_fast(number):
    """Rate-safe target lookup. Avoid provider racing, which multiplied 429 traffic."""
    number=int(number)
    started=time.perf_counter()
    errors={}
    providers=[('TRONGrid-FullNode', TRON_BLOCK_BY_NUM, 'POST'), ('TRONScan', TRONSCAN_BLOCK, 'GET')]
    for name,url,method in providers:
        try:
            if method == 'POST':
                data=_get_json(url, method='POST', payload={'num':number})
                block_id=data.get('blockID')
                if not block_id: raise RuntimeError('block not found')
                block={'block':block_id,'number':int(data.get('block_header',{}).get('raw_data',{}).get('number',number)),'timestamp':data.get('block_header',{}).get('raw_data',{}).get('timestamp')}
            else:
                data=_get_json(f'{url}?number={number}')
                rows=data.get('data') if isinstance(data,dict) else None
                row=rows[0] if isinstance(rows,list) and rows else None
                if not row or not row.get('hash'): raise RuntimeError('block not found')
                block={'block':row['hash'],'number':int(row.get('number',number)),'timestamp':row.get('timestamp')}
            if block.get('block') and int(block.get('number',-1)) == number:
                return block,name,round((time.perf_counter()-started)*1000,1),errors
        except Exception as exc:
            errors[name]=str(exc)[:220]
            # A 429 activates the shared cooldown; do not fan out immediately.
            if '429' in str(exc): break
    raise RuntimeError('target block not yet available: '+' | '.join(errors.values()))


def _reconstruct_period_groups_dbfirst(date_str, period, target20):
    """Build the current 1..20 snapshot from RAM + PostgreSQL only.
    No upstream HTTP is allowed here: this function is safe inside the G20 barrier.
    """
    wanted=[int(target20)-(20-g) for g in range(1,21)]
    rows={}
    with _live_blocks_lock:
        for bn in wanted:
            if bn in _live_blocks: rows[bn]=dict(_live_blocks[bn])
    # The live publisher has the complete G1..G17 snapshot in the common case.
    # Do not put a PostgreSQL round trip on the critical locking path.
    missing=[bn for bn in wanted[:17] if bn not in rows]
    if missing:
        try:
            historical=get_db_blocks(missing)
            for bn,row in historical.items(): rows.setdefault(bn,row)
        except Exception: pass
    groups={}; key=f'{date_str}:{int(period):04d}'
    for g,bn in enumerate(wanted,1):
        row=rows.get(bn)
        if not row: continue
        nums=row.get('numbers') or []
        if not isinstance(nums,list):
            try: nums=json.loads(nums)
            except Exception: nums=[]
        groups[str(g)]={'group':g,'period':f'{int(period):04d}','periodKey':key,'target20':int(target20),
                        'blockNumber':bn,'block':row.get('block_hash') or row.get('block'),
                        'numbers':nums,'singleCount':int(row.get('single_count'))}
    return groups


def _lock_g17_snapshot(date_str, period, target20, source):
    """Idempotently lock only when G1..G17 are present and G20 is not published."""
    key=f'{date_str}:{int(period):04d}'
    with _g17_durable_lock:
        if key in _g17_durable:return True
    groups=_reconstruct_period_groups_dbfirst(date_str,period,target20)
    if groups.get('20') or not all(str(i) in groups for i in range(1,18)):
        trace_period_lifecycle(date_str,period,target20,'event_lock_not_ready',groups,{'source':source})
        return False
    with _g17_event_lock:
        first_seen=_g17_first_seen.get(key)
    attempt_at=datetime.now(timezone.utc).isoformat(timespec='milliseconds')
    # Save the prediction first: lifecycle telemetry is not allowed to consume
    # the short interval between G17 and the result block.
    save_prediction_if_ready(date_str,period,groups,target20,force_backend=True)
    flush_pending_ai_predictions()
    ok=prediction_exists(key) is not None
    trace_period_lifecycle(date_str,period,target20,'event_lock_attempt',groups,
                           {'source':source,'firstSeenAt':first_seen},increment_attempt=True,event_at=attempt_at)
    trace_period_lifecycle(date_str,period,target20,'event_lock_success' if ok else 'event_lock_not_durable',groups,{'source':source})
    if ok:
        with _g17_durable_lock:
            _g17_durable.add(key)
            if len(_g17_durable)>100:
                _g17_durable.clear(); _g17_durable.add(key)
        set_period_runtime(date_str,period,target20,'AI_LOCKED',len(groups))
    return ok


def g17_event_worker():
    while True:
        worker_touch('g17-event')
        try:
            ds,p,target,key=_g17_event_queue.get(timeout=1.0)
        except queue.Empty:
            continue
        try:
            if RESEARCH_ONLY_MODE:
                groups=_reconstruct_period_groups_dbfirst(ds,p,target)
                with _prediction_context_lock:
                    context=_prediction_context['value'] if _prediction_context['key']==key else None
                ok=bool(context and all(str(i) in groups for i in range(1,18)) and
                        save_research_forecasts(ds,p,target,
                            context['historical'],context['hashSnapshot'],groups))
            else:
                ok=_lock_g17_snapshot(ds,p,target,'g17_publish_event')
            with _barrier_diag_lock:
                if ok: _barrier_diag['g17Locks'] += 1
                _barrier_diag['last']={'event':'g17_event_lock','periodKey':key,'ok':bool(ok)}
        except Exception as exc:
            worker_touch('g17-event',exc)
        finally:
            _g17_event_queue.task_done()


def should_poll_target(latest_live, target, countdown):
    """Resume direct target lookup at G17 or in the final 12 seconds."""
    return (latest_live is not None and int(latest_live)>=int(target)-3) or int(countdown)<=12


def _block_timestamp_seconds(value):
    if isinstance(value,datetime):return value.timestamp()
    try:
        numeric=float(value)
        return numeric/1000 if numeric>10**11 else numeric
    except (TypeError,ValueError):return None


def target_poll_due(g17_timestamp, countdown, now_seconds):
    """Reserve public HTTP for G17 until the target can plausibly exist."""
    stamp=_block_timestamp_seconds(g17_timestamp)
    if stamp is not None:
        return now_seconds >= stamp+8.4
    return int(countdown)<=4


def g17_fast_worker():
    """Watch the last pre-result input even when ordinary block ingestion lags."""
    last_target=None
    while True:
        worker_touch('g17-fast')
        try:
            ds,p,_,_=current_period(); target=period_target_block(ds,p)
            if target!=last_target:last_target=target
            g17=target-3
            with _live_blocks_lock:
                seen=g17 in _live_blocks
                latest=_live_latest_number
                g16=_live_blocks.get(g17-1)
            countdown=calibrated_countdown_value()
            g16_time=_block_timestamp_seconds(g16.get('timestamp')) if g16 else None
            # A fixed attempt limit can expire before G17 is produced. Keep
            # watching until the actual block arrives or the result is observed.
            if (not seen and countdown>4
                    and not _target_block_observed(target)
                    and ((latest is not None and latest>=target-4) or countdown<=10)
                    and (g16_time is None or time.time()>=g16_time+2.3)):
                try:
                    block,provider,latency_ms,_=fetch_block_fast(g17)
                    publish_block_live(block)
                    enqueue_block_for_db(block)
                    with _barrier_diag_lock:
                        _barrier_diag['g17FastProvider']=provider
                        _barrier_diag['g17FastLatencyMs']=latency_ms
                except Exception as exc:
                    worker_touch('g17-fast',exc)
        except Exception as exc:
            worker_touch('g17-fast',exc)
        time.sleep(0.9)


def target_result_fast_worker():
    """Watch the target near its block slot, without polling a future height all minute."""
    last_target = None
    while True:
        worker_touch('target-result-fast')
        sleep_for = 0.95
        try:
            date_str, period, _, _ = current_period()
            target = period_target_block(date_str, period)
            with _live_blocks_lock:
                ready = target in _live_blocks
                g17_row=_live_blocks.get(target-3)
            if target != last_target:
                last_target = target
                with _fast_diag_lock:
                    _fast_diag.update({'target': target, 'provider': None, 'firstSeenAt': None,
                                       'latencyMs': None, 'barrierMs': None,
                                       'dbSavedAt': None, 'dbDelayMs': None, 'errors': {}})
            if not ready:
                with _live_blocks_lock:
                    latest_live=_live_latest_number
                # Start when G17 is visible, or within the last 12 seconds if
                # the normal ingestion thread lags. Preserve the known target.
                if not should_poll_target(latest_live,target,calibrated_countdown_value()):
                    time.sleep(sleep_for)
                    continue
                g17_time=g17_row.get('timestamp') if g17_row else None
                if not target_poll_due(g17_time,calibrated_countdown_value(),time.time()):
                    time.sleep(sleep_for)
                    continue
                try:
                    block, provider, latency_ms, errors = fetch_block_fast(target)
                    # The target already exists on-chain. Never create a new
                    # prediction here, even if this process has not published it.
                    publish_block_live(block)
                    with _fast_diag_lock:
                        _fast_diag.update({
                            'target': target, 'provider': provider,
                            'firstSeenAt': datetime.now(CN_TZ).isoformat(timespec='milliseconds'),
                            'latencyMs': latency_ms, 'barrierMs': 0,
                            'blockTime': block.get('timestamp'), 'errors': errors
                        })
                    enqueue_block_for_db(block,official=True)
                except Exception as exc:
                    with _fast_diag_lock:
                        _fast_diag['errors'] = {'last': str(exc)[:300]}
            else:
                sleep_for = 1.0
        except Exception as exc:
            with _fast_diag_lock:
                _fast_diag['errors'] = {'worker': str(exc)[:300]}
            sleep_for = 0.5
        time.sleep(sleep_for)


def calc_numbers(block_hash):
    """Platform rule: read the hash from right to left.
    Take A-E letters and 0-9 digits in their respective order, pair them,
    map A=0...E=4, discard 00 and duplicate pairs, and continue until 7 valid values.
    """
    s = block_hash.upper()[::-1]
    letters = [c for c in s if c in 'ABCDE']
    digits = [c for c in s if c.isdigit()]
    li = di = 0
    used = set()
    result = []
    while len(result) < 7 and li < len(letters) and di < len(digits):
        value = 'ABCDE'.index(letters[li]) * 10 + int(digits[di])
        li += 1
        di += 1
        if value == 0 or value > 49 or value in used:
            continue
        used.add(value)
        result.append(value)
    if len(result) != 7:
        raise RuntimeError('当前区块哈希按规则无法取得7个有效号码')
    return result



def calc_single_count(numbers):
    return sum(int(n) % 2 for n in numbers)


def previous_official_result(date_str, period, lookback=3):
    """Bootstrap the home result from confirmed prior group-20 blocks."""
    idx=period_index(date_str,period)
    candidates=[]
    for off in range(1,int(lookback)+1):
        ordinal,zero=divmod(idx-off,1440)
        d=date.fromordinal(ordinal)
        ds=d.strftime('%Y-%m-%d'); p=zero+1
        candidates.append((ds,p,period_target_block(ds,p)))
    rows=get_db_blocks([x[2] for x in candidates])
    for ds,p,target in candidates:
        row=rows.get(target)
        if not row: continue
        nums=row.get('numbers')
        if isinstance(nums,str):
            try: nums=json.loads(nums)
            except (TypeError,ValueError): continue
        if not isinstance(nums,list) or len(nums)!=7: continue
        return {'numbers':nums,'singleCount':calc_single_count(nums),
                'blockNumber':target,'block':row.get('block_hash'),
                'platformPeriod':date.fromisoformat(ds).strftime('%y%m%d')+f'{p:04d}'}
    return None


TAIL_ANCHOR_INDEX = period_index('2026-09-24', 481)
TAIL_INTERVAL_PERIODS = (period_index('2026-09-24', 1201) - TAIL_ANCHOR_INDEX) // 2
TAIL_SEQUENCE = (6, 4, 2, 0, 8)


def tail_schedule(date_str, period):
    """Average six-hour phase; estimates are explicitly separate from observations."""
    idx = period_index(date_str, period)
    phase = (idx - TAIL_ANCHOR_INDEX) // TAIL_INTERVAL_PERIODS
    start = TAIL_ANCHOR_INDEX + phase * TAIL_INTERVAL_PERIODS
    return {'tail': TAIL_SEQUENCE[phase % len(TAIL_SEQUENCE)],
            'phaseStart': _tail_boundary(start), 'nextSwitch': _tail_boundary(start + TAIL_INTERVAL_PERIODS),
            'estimated': start not in (TAIL_ANCHOR_INDEX, period_index('2026-09-24', 1201)),
            'intervalPeriods': TAIL_INTERVAL_PERIODS}


def _tail_boundary(index):
    ordinal, zero = divmod(index, 1440)
    d = date.fromordinal(ordinal)
    p = zero + 1
    return {'date': d.isoformat(), 'period': f'{p:04d}', 'timeCN': f'{p // 60:02d}:{p % 60:02d}'}


def period_target_block(date_str, period):
    """Twenty blocks per period, with a two-block correction per tail switch.

    The 360-period switch interval is an estimate based on two confirmed
    transitions. Confirmed sample anchors are tested separately below.
    """
    idx = period_index(date_str, int(period))
    switches = (idx - TAIL_ANCHOR_INDEX) // TAIL_INTERVAL_PERIODS
    return 86511906 + (idx - TAIL_ANCHOR_INDEX) * 20 - 2 * switches

def get_db_pool():
    """Create a small thread-safe PostgreSQL pool lazily.

    A small pool is intentional: the collector is the main writer and the
    frontend should reuse existing connections instead of opening a new TCP
    connection on every poll.
    """
    global _db_pool
    if not DATABASE_URL or psycopg2 is None:
        return None
    if _db_pool is not None:
        return _db_pool
    with _db_pool_lock:
        if _db_pool is None:
            # The worker owns several independent collection and lifecycle
            # threads. Four slots forced the G17 lock to compete with slow
            # historical scans and repeatedly fail with pool exhaustion.
            max_connections=4 if os.environ.get('RUN_EMBEDDED_WORKERS')=='0' else 12
            _db_pool = ThreadedConnectionPool(1,max_connections,DATABASE_URL,connect_timeout=5)
    return _db_pool


def db_connect(retries=2):
    """Borrow a pooled connection, retrying transient Render timeouts."""
    pool = get_db_pool()
    if pool is None:
        return None
    last = None
    for attempt in range(retries + 1):
        try:
            conn = pool.getconn()
            if conn.closed:
                pool.putconn(conn, close=True)
                raise psycopg2.OperationalError('pooled connection was closed')
            return conn
        except Exception as exc:
            last = exc
            if attempt < retries:
                time.sleep(1 if attempt == 0 else 3)
    raise last


def db_release(conn, broken=False):
    if conn is None:
        return
    try:
        if not conn.closed:
            try:
                conn.rollback()
            except Exception:
                broken = True
        pool = get_db_pool()
        if pool is not None:
            pool.putconn(conn, close=broken)
        else:
            conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass


def init_db():
    conn = db_connect()
    if conn is None:
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tron_blocks (
                        block_number BIGINT PRIMARY KEY,
                        block_hash TEXT NOT NULL,
                        block_time TIMESTAMPTZ,
                        numbers JSONB NOT NULL,
                        single_count SMALLINT NOT NULL,
                        fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_tron_blocks_fetched_at ON tron_blocks(fetched_at DESC)")
                cur.execute("""CREATE TABLE IF NOT EXISTS historical_raw_blocks (
                    block_number BIGINT PRIMARY KEY, block_hash TEXT NOT NULL,
                    block_time TIMESTAMPTZ NOT NULL, numbers JSONB NOT NULL,
                    single_count SMALLINT NOT NULL, fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_historical_raw_time ON historical_raw_blocks(block_time)")
                cur.execute("""CREATE TABLE IF NOT EXISTS raw_backfill_runtime (
                    singleton SMALLINT PRIMARY KEY CHECK(singleton=1),
                    start_block BIGINT NOT NULL,end_block BIGINT NOT NULL,next_block BIGINT NOT NULL,
                    stored_blocks INTEGER NOT NULL DEFAULT 0,failed_batches INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'downloading',last_error TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ai_predictions (
                        period_key TEXT PRIMARY KEY,
                        period_date DATE NOT NULL,
                        period_no INTEGER NOT NULL,
                        target_block BIGINT NOT NULL,
                        data_conclusion SMALLINT,
                        ai_analysis SMALLINT NOT NULL,
                        prediction_top3 JSONB,
                        sample_size SMALLINT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        actual_single SMALLINT,
                        verified_at TIMESTAMPTZ
                    )
                """)
                cur.execute("ALTER TABLE ai_predictions ADD COLUMN IF NOT EXISTS prediction_top3 JSONB")
                cur.execute("ALTER TABLE ai_predictions ADD COLUMN IF NOT EXISTS conclusion17 SMALLINT")
                cur.execute("ALTER TABLE ai_predictions ADD COLUMN IF NOT EXISTS conclusion17_mode TEXT")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_predictions_date ON ai_predictions(period_date DESC, period_no DESC)")
                cur.execute("ALTER TABLE ai_predictions ADD COLUMN IF NOT EXISTS model_version TEXT")
                cur.execute("ALTER TABLE ai_predictions ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ")
                cur.execute("ALTER TABLE ai_predictions ADD COLUMN IF NOT EXISTS confidence REAL")
                cur.execute("ALTER TABLE ai_predictions ADD COLUMN IF NOT EXISTS ensemble_detail JSONB")
                cur.execute("ALTER TABLE ai_predictions ADD COLUMN IF NOT EXISTS model_weights JSONB")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS period_groups (
                        period_key TEXT NOT NULL,
                        period_date DATE NOT NULL,
                        period_no INTEGER NOT NULL,
                        group_no SMALLINT NOT NULL,
                        target_block BIGINT NOT NULL,
                        block_number BIGINT NOT NULL,
                        block_hash TEXT NOT NULL,
                        numbers JSONB NOT NULL,
                        single_count SMALLINT NOT NULL,
                        captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY(period_key, group_no)
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_period_groups_period ON period_groups(period_date DESC, period_no DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_period_groups_complete ON period_groups(period_date DESC,period_no DESC) WHERE group_no=20")
                cur.execute("""CREATE TABLE IF NOT EXISTS hash_model_runtime (
                    singleton SMALLINT PRIMARY KEY CHECK (singleton=1),
                    trained_through TEXT, snapshot JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
                cur.execute("""CREATE TABLE IF NOT EXISTS research_predictions (
                    period_key TEXT NOT NULL, period_date DATE NOT NULL,
                    period_no INTEGER NOT NULL, target_block BIGINT NOT NULL,
                    candidate TEXT NOT NULL, prediction SMALLINT NOT NULL,
                    scores JSONB NOT NULL, model_version TEXT NOT NULL,
                    trained_through TEXT NOT NULL, locked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY(period_key,candidate))""")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_research_predictions_date ON research_predictions(period_date DESC,period_no DESC)")
                cur.execute("ALTER TABLE research_predictions ADD COLUMN IF NOT EXISTS actual_single SMALLINT")
                cur.execute("ALTER TABLE research_predictions ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ")
                cur.execute("ALTER TABLE research_predictions ADD COLUMN IF NOT EXISTS valid_pre_result BOOLEAN")
                cur.execute("ALTER TABLE research_predictions ADD COLUMN IF NOT EXISTS research_method TEXT")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_research_verified ON research_predictions(period_date DESC,period_no DESC) WHERE valid_pre_result=TRUE")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_research_pending ON research_predictions(target_block) WHERE verified_at IS NULL")
                cur.execute("""CREATE INDEX IF NOT EXISTS idx_research_trial_summary
                    ON research_predictions(model_version,candidate,period_key)
                    INCLUDE (research_method,prediction,actual_single)
                    WHERE valid_pre_result=TRUE
                      AND candidate IN ('trial_first','trial_second')""")
                cur.execute("""CREATE TABLE IF NOT EXISTS research_replay_runtime (
                    singleton SMALLINT PRIMARY KEY CHECK(singleton=1),
                    research_version TEXT NOT NULL, report JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
                cur.execute("""CREATE TABLE IF NOT EXISTS research_practice_results (
                    research_version TEXT NOT NULL, period_key TEXT NOT NULL,
                    trained_through TEXT NOT NULL, actual_single SMALLINT NOT NULL,
                    predictions JSONB NOT NULL, hashes_complete BOOLEAN NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY(research_version,period_key))""")
                cur.execute("""CREATE TABLE IF NOT EXISTS backfill_runtime (
                    singleton SMALLINT PRIMARY KEY CHECK(singleton=1),
                    start_index BIGINT NOT NULL,end_index BIGINT NOT NULL,
                    next_index BIGINT NOT NULL,already_present INTEGER NOT NULL DEFAULT 0,
                    fetched_periods INTEGER NOT NULL DEFAULT 0,failed_periods INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'scanning',last_error TEXT,
                    revision BIGINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
                cur.execute("""CREATE TABLE IF NOT EXISTS backfill_failures (
                    period_index BIGINT PRIMARY KEY,reason TEXT,
                    attempts INTEGER NOT NULL DEFAULT 1,
                    last_attempt TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
                cur.execute("""CREATE TABLE IF NOT EXISTS research_selection (
                    singleton SMALLINT PRIMARY KEY CHECK(singleton=1),
                    research_version TEXT NOT NULL,
                    last_verified_period TEXT,
                    status TEXT NOT NULL,
                    candidate TEXT,
                    evidence JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS system_events (
                        id BIGSERIAL PRIMARY KEY,
                        event_type TEXT NOT NULL,
                        period_key TEXT,
                        detail JSONB,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_system_events_created ON system_events(created_at DESC)")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS period_runtime (
                        period_key TEXT PRIMARY KEY,
                        period_date DATE NOT NULL,
                        period_no INTEGER NOT NULL,
                        target_block BIGINT NOT NULL,
                        state TEXT NOT NULL DEFAULT 'COLLECTING',
                        groups_seen SMALLINT NOT NULL DEFAULT 0,
                        prediction_locked_at TIMESTAMPTZ,
                        result_ready_at TIMESTAMPTZ,
                        verified_at TIMESTAMPTZ,
                        last_error TEXT,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                cur.execute("ALTER TABLE period_runtime ADD COLUMN IF NOT EXISTS g17_ready_at TIMESTAMPTZ")
                cur.execute("ALTER TABLE period_runtime ADD COLUMN IF NOT EXISTS lock_attempts INTEGER NOT NULL DEFAULT 0")
                cur.execute("ALTER TABLE period_runtime ADD COLUMN IF NOT EXISTS lifecycle_trace JSONB NOT NULL DEFAULT '{}'::jsonb")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_period_runtime_updated ON period_runtime(updated_at DESC)")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS service_heartbeats (
                        service_name TEXT PRIMARY KEY,
                        service_role TEXT NOT NULL,
                        instance_id TEXT,
                        model_version TEXT,
                        current_period TEXT,
                        current_period_key TEXT,
                        status TEXT NOT NULL DEFAULT 'ONLINE',
                        detail JSONB,
                        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_service_heartbeats_updated ON service_heartbeats(updated_at DESC)")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS omission_runtime (
                        singleton SMALLINT PRIMARY KEY DEFAULT 1 CHECK (singleton=1),
                        omission JSONB NOT NULL DEFAULT '{}'::jsonb,
                        last_period_key TEXT,
                        last_period_date DATE,
                        last_period_no INTEGER,
                        last_single SMALLINT,
                        processed_count BIGINT NOT NULL DEFAULT 0,
                        source TEXT NOT NULL DEFAULT 'period_groups group20',
                        last_error TEXT,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
        return True
    finally:
        db_release(conn)


def persist_service_heartbeat(service_name='tron-monitor-worker', service_role='worker', instance_id=None, status='ONLINE', detail=None):
    """Persist cross-process service health so Web can observe the Background Worker."""
    ds,p,ps,_=current_period()
    conn=None
    try:
        conn=db_connect()
        if conn is None: return False
        payload=detail if isinstance(detail,dict) else {}
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO service_heartbeats(service_name,service_role,instance_id,model_version,current_period,current_period_key,status,detail,started_at,updated_at)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW(),NOW())
                    ON CONFLICT(service_name) DO UPDATE SET
                      service_role=EXCLUDED.service_role, instance_id=EXCLUDED.instance_id,
                      model_version=EXCLUDED.model_version, current_period=EXCLUDED.current_period,
                      current_period_key=EXCLUDED.current_period_key, status=EXCLUDED.status,
                      detail=EXCLUDED.detail, updated_at=NOW()
                """,(service_name,service_role,instance_id,MODEL_VERSION,ps,f'{ds}:{int(p):04d}',status,json.dumps(payload,ensure_ascii=False)))
        return True
    except Exception as exc:
        with _runtime_health_lock: _runtime_health['lastDbError']=str(exc)[:300]
        return False
    finally:
        if conn is not None: db_release(conn)


def get_service_heartbeats():
    conn=None
    try:
        conn=db_connect()
        if conn is None: return []
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT service_name,service_role,instance_id,model_version,current_period,current_period_key,status,detail,started_at,updated_at FROM service_heartbeats ORDER BY updated_at DESC")
            return [dict(r) for r in cur.fetchall()]
    finally:
        if conn is not None: db_release(conn)


def publish_block_live(block):
    """Calculate and publish a block to RAM immediately; never wait for PostgreSQL."""
    global _live_latest_number
    nums = calc_numbers(block['block'])
    formatted = [f'{n:02d}' for n in nums]
    single_count = calc_single_count(nums)
    bn = int(block['number'])
    with _live_blocks_lock:
        _live_blocks[bn] = {
            'block_number': bn, 'block_hash': block['block'],
            'numbers': formatted, 'single_count': single_count,
            'timestamp': block.get('timestamp')
        }
        _live_latest_number = bn if _live_latest_number is None else max(_live_latest_number, bn)
        if len(_live_blocks) > 240:
            for old_bn in sorted(_live_blocks)[:-160]:
                _live_blocks.pop(old_bn, None)
    return bn

def persist_block_to_db(block):
    """Persist a block after it is already visible in RAM."""
    nums = calc_numbers(block['block'])
    formatted = [f'{n:02d}' for n in nums]
    single_count = calc_single_count(nums)
    bn = int(block['number'])
    ts = block.get('timestamp')
    block_time = datetime.fromtimestamp(ts / 1000, timezone.utc) if ts else None
    conn = db_connect()
    if conn is None:
        raise RuntimeError('database unavailable')
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO tron_blocks(block_number, block_hash, block_time, numbers, single_count)
                    VALUES (%s,%s,%s,%s::jsonb,%s)
                    ON CONFLICT (block_number) DO UPDATE SET
                      block_hash=EXCLUDED.block_hash, block_time=EXCLUDED.block_time,
                      numbers=EXCLUDED.numbers, single_count=EXCLUDED.single_count
                """, (bn, block['block'], block_time, json.dumps(formatted), single_count))
    finally:
        db_release(conn)

def enqueue_block_for_db(block, official=False):
    """Give G20 its own writer so routine group backfill cannot queue ahead."""
    if not official:
        try:
            ds,p,_,_=current_period()
            official=int(block['number'])==period_target_block(ds,p)
        except Exception:
            official=False
    if official:
        try:
            _result_db_write_queue.put_nowait(dict(block))
            return
        except queue.Full:
            pass
    try:
        _db_write_queue.put_nowait(dict(block))
    except queue.Full:
        pass


def result_db_writer_worker():
    """Persist official blocks independently of the 1..19 group write queue."""
    while True:
        worker_touch('result-db-writer')
        block=_result_db_write_queue.get()
        saved=False
        try:
            for delay in (0,0.35,1,2):
                if delay:time.sleep(delay)
                try:
                    persist_block_to_db(block)
                    saved=True
                    with _fast_diag_lock:
                        if int(block['number'])==_fast_diag.get('target'):
                            now=datetime.now(CN_TZ)
                            seen=_fast_diag.get('firstSeenAt')
                            _fast_diag['dbSavedAt']=now.isoformat(timespec='milliseconds')
                            if seen:
                                _fast_diag['dbDelayMs']=round((now-datetime.fromisoformat(seen)).total_seconds()*1000,1)
                    break
                except Exception as exc:
                    worker_touch('result-db-writer',exc)
            if not saved:
                try:_db_write_queue.put_nowait(block)
                except queue.Full:pass
        finally:
            _result_db_write_queue.task_done()

def db_writer_worker():
    """Database writes are deliberately off the realtime result path."""
    while True:
        worker_touch('db-writer')
        block = _db_write_queue.get()
        try:
            for delay in (0, 1, 3):
                if delay:
                    time.sleep(delay)
                try:
                    persist_block_to_db(block)
                    break
                except Exception:
                    continue
        finally:
            _db_write_queue.task_done()

def save_block_to_db(block):
    # Backwards-compatible helper used by older call sites.
    publish_block_live(block)
    enqueue_block_for_db(block)

def cleanup_old_db_rows():
    conn = db_connect()
    if conn is None:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM tron_blocks WHERE fetched_at < NOW() - INTERVAL '7 days'")
    finally:
        db_release(conn)


def get_db_blocks(numbers):
    nums = sorted({int(x) for x in numbers})
    if not nums:
        return {}
    conn = db_connect()
    if conn is None:
        return {}
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT block_number, block_hash, block_time, numbers, single_count FROM tron_blocks WHERE block_number = ANY(%s)", (nums,))
            return {int(r['block_number']): r for r in cur.fetchall()}
    finally:
        db_release(conn)


def get_db_latest_number():
    conn = db_connect()
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(block_number) FROM tron_blocks")
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None
    finally:
        db_release(conn)


def get_db_recent_rows(limit=2000):
    conn = db_connect()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT block_number, block_hash, block_time, numbers, single_count, fetched_at FROM tron_blocks ORDER BY block_number DESC LIMIT %s", (int(limit),))
            return cur.fetchall()
    finally:
        db_release(conn)



def restore_omission_from_db(date_str=None, period=None):
    """Rebuild official-result omission counters from retained PostgreSQL data.

    PostgreSQL is the persistence source across deploys/restarts.  We read the
    retained platform group-20 blocks once, derive the current 单0..单7 omission
    values, then normal realtime updates continue in memory/state.
    """
    global _omission_bootstrapped
    with _omission_bootstrap_lock:
        if _omission_bootstrapped:
            return None
        if date_str is None or period is None:
            date_str, period, _, _ = current_period()
        idx_now = period_index(date_str, int(period))
        # Three days matches the database retention window.  Include current
        # period if its group-20 block has already been persisted.
        periods = []
        wanted = []
        for off in range(0, DB_RETENTION_DAYS * 1440):
            idx = idx_now - off
            ordinal, zero = divmod(idx, 1440)
            d = date.fromordinal(ordinal)
            pno = zero + 1
            target = period_target_block(d.strftime('%Y-%m-%d'), pno)
            periods.append((d.strftime('%Y-%m-%d'), pno, target))
            wanted.append(target)
        rows = get_db_blocks(wanted)
        official = []
        for dstr, pno, target in reversed(periods):
            row = rows.get(target)
            if row and row.get('single_count') is not None:
                official.append((dstr, pno, target, int(row['single_count'])))
        if not official:
            return None
        omission = {str(i): 0 for i in range(8)}
        for _, _, _, single in official:
            for i in range(8):
                omission[str(i)] = 0 if i == single else omission[str(i)] + 1
        last_d, last_p, _, _ = official[-1]
        state = read_state() or {}
        state['omission'] = omission
        state['lastOmissionPeriod'] = f'{last_d}:{int(last_p):04d}'
        state['omissionSource'] = 'PostgreSQL history + realtime memory'
        state['omissionHistorySample'] = len(official)
        write_state(state)
        _omission_bootstrapped = True
        return omission

def get_omission_runtime():
    """Read the durable omission snapshot shared by Worker and Web."""
    conn=None
    try:
        conn=db_connect(retries=0)
        if conn is None: return None
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT omission,last_period_key,last_period_date,last_period_no,last_single,processed_count,source,last_error,updated_at FROM omission_runtime WHERE singleton=1")
            row=cur.fetchone()
            if not row: return None
            out=dict(row)
            om=out.get('omission') or {}
            if isinstance(om,str):
                try: om=json.loads(om)
                except Exception: om={}
            out['omission']={str(i):int(om.get(str(i),0) or 0) for i in range(8)}
            return out
    finally:
        if conn is not None: db_release(conn)


def rebuild_omission_runtime():
    """Rebuild omission from durable official group-20 rows only.

    This is intentionally independent of /api/draw and process-local state files.
    It is safe after deploy/restart and never uses AI predictions as official data.
    """
    conn=None
    try:
        conn=db_connect()
        if conn is None: return False
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""SELECT period_key,period_date,period_no,target_block,single_count
                               FROM period_groups WHERE group_no=20
                               ORDER BY period_date ASC,period_no ASC""")
                rows=[r for r in cur.fetchall() if calibrated_group20_row(r)]
                if not rows: return False
                omission={str(i):0 for i in range(8)}
                for r in rows:
                    single=int(r['single_count'])
                    if not 0 <= single <= 7: continue
                    for i in range(8): omission[str(i)]=0 if i==single else omission[str(i)]+1
                last=rows[-1]
                cur.execute("""INSERT INTO omission_runtime(singleton,omission,last_period_key,last_period_date,last_period_no,last_single,processed_count,source,last_error,updated_at)
                               VALUES(1,%s::jsonb,%s,%s,%s,%s,%s,'period_groups group20 rebuild',NULL,NOW())
                               ON CONFLICT(singleton) DO UPDATE SET omission=EXCLUDED.omission,last_period_key=EXCLUDED.last_period_key,
                               last_period_date=EXCLUDED.last_period_date,last_period_no=EXCLUDED.last_period_no,last_single=EXCLUDED.last_single,
                               processed_count=EXCLUDED.processed_count,source=EXCLUDED.source,last_error=NULL,updated_at=NOW()""",
                            (json.dumps(omission),last['period_key'],last['period_date'],int(last['period_no']),int(last['single_count']),len(rows)))
        return True
    except Exception as exc:
        with _runtime_health_lock: _runtime_health['lastDbError']=str(exc)[:300]
        return False
    finally:
        if conn is not None: db_release(conn)


def omission_engine_worker():
    """Independent durable omission heart.

    Consumes confirmed period_groups.group_no=20 rows, catches up missed periods,
    persists counters to PostgreSQL, and therefore keeps running with Web closed.
    """
    while True:
        worker_touch('omission-engine')
        conn=None
        try:
            snap=get_omission_runtime()
            if not snap:
                rebuild_omission_runtime()
                time.sleep(0.5); continue
            omission=dict(snap['omission'])
            last_date=snap.get('last_period_date'); last_no=snap.get('last_period_no')
            conn=db_connect(retries=0)
            if conn is None:
                time.sleep(0.5); continue
            with conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    if last_date is None or last_no is None:
                        cur.execute("SELECT period_key,period_date,period_no,target_block,single_count FROM period_groups WHERE group_no=20 ORDER BY period_date,period_no")
                    else:
                        cur.execute("""SELECT period_key,period_date,period_no,target_block,single_count FROM period_groups
                                       WHERE group_no=20 AND (period_date>%s OR (period_date=%s AND period_no>%s))
                                       ORDER BY period_date,period_no""",(last_date,last_date,int(last_no)))
                    rows=[r for r in cur.fetchall() if calibrated_group20_row(r)]
                    processed=int(snap.get('processed_count') or 0)
                    for r in rows:
                        single=int(r['single_count'])
                        if not 0 <= single <= 7: continue
                        for i in range(8): omission[str(i)]=0 if i==single else omission[str(i)]+1
                        processed += 1
                        cur.execute("""UPDATE omission_runtime SET omission=%s::jsonb,last_period_key=%s,last_period_date=%s,last_period_no=%s,
                                       last_single=%s,processed_count=%s,source='independent omission engine',last_error=NULL,updated_at=NOW() WHERE singleton=1""",
                                    (json.dumps(omission),r['period_key'],r['period_date'],int(r['period_no']),single,processed))
            if rows:
                record_system_event('omission_catchup',rows[-1]['period_key'],{'processed':len(rows),'omission':omission})
        except Exception as exc:
            worker_touch('omission-engine',exc)
            try:
                c=db_connect(retries=0)
                if c:
                    with c:
                        with c.cursor() as cur: cur.execute("UPDATE omission_runtime SET last_error=%s,updated_at=NOW() WHERE singleton=1",(str(exc)[:300],))
                    db_release(c)
            except Exception: pass
        finally:
            if conn is not None: db_release(conn)
        time.sleep(0.5)


def tron_ingest_worker():
    """Low-latency TRON collector. PostgreSQL never blocks live publication."""
    global _live_latest_number
    try:
        init_db()
        try:
            restore_omission_from_db()
        except Exception:
            pass
    except Exception:
        pass
    last_seen = None
    last_cleanup = 0
    while True:
        worker_touch('tron-ingest')
        try:
            latest = fetch_latest_block()
            end = int(latest['number'])
            if last_seen is None:
                # V8.1.3: always warm the recent chain window into RAM.
                # Do NOT seed last_seen from PostgreSQL: target-result-fast can
                # persist a later target block first, which previously made the
                # collector skip groups 1..19 after a deploy/restart.
                # First hydrate RAM from PostgreSQL at zero upstream cost.  Do not
                # trust DB MAX as the collector cursor because group20 may have been
                # persisted ahead of groups1..19 by the fast watcher.
                try:
                    for row in get_db_recent_rows(90):
                        publish_block_live({'block': row['block_hash'], 'number': int(row['block_number']), 'timestamp': row.get('block_time')})
                except Exception as exc:
                    worker_touch('tron-ingest', f'db-warmcache: {exc}')
                # Repair only the small recent gap window and skip blocks already in RAM.
                warm_start=max(end-23,0)
                for n in range(warm_start,end+1):
                    try:
                        with _live_blocks_lock:
                            already=n in _live_blocks
                        if already:
                            last_seen=max(last_seen or 0,n); continue
                        block=latest if n==end else fetch_block_by_number(n)
                        publish_block_live(block); enqueue_block_for_db(block); last_seen=n
                    except Exception as exc:
                        worker_touch('tron-ingest', f'gaprepair block={n}: {exc}')
                        if '429' in str(exc): break
                        # preserve order; retry the missing block on the next pass
                        break
                if last_seen is None:
                    last_seen=max(end-1,0)
            if end > last_seen:
                start_n = max(last_seen + 1, end - 23)
                for n in range(start_n, end + 1):
                    try:
                        block = latest if n == end else fetch_block_by_number(n)
                        publish_block_live(block)
                        enqueue_block_for_db(block)
                        last_seen = n
                    except Exception:
                        # Do not skip a missing block permanently.
                        break
            if time.time() - last_cleanup > 3600:
                # Cleanup is intentionally not on every realtime iteration.
                last_cleanup = time.time()
        except Exception:
            pass
        # Public APIs are shared resources; ~1.25s latest polling plus the global
        # governor is sufficient for a ~3s block cadence without self-throttling.
        time.sleep(1.25)

def record_system_event(event_type, period_key=None, detail=None):
    conn=None
    try:
        conn=db_connect(retries=0)
        if conn is None:return
        with conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO system_events(event_type,period_key,detail) VALUES(%s,%s,%s::jsonb)",
                            (str(event_type),period_key,json.dumps(detail or {})))
    except Exception:
        pass
    finally:
        if conn is not None: db_release(conn)


def persist_period_groups(date_str,period,groups,target20):
    """Materialize all observed group rows by period, independently of raw block retention."""
    if not groups:return 0
    key=f'{date_str}:{int(period):04d}'; rows=[]
    for gtxt,row in groups.items():
        try:
            g=int(gtxt)
            if not (1 <= g <= 20): continue
            rows.append((key,date_str,int(period),g,int(target20),int(row['blockNumber']),
                         row['block'],json.dumps(row['numbers']),int(row['singleCount'])))
        except Exception:
            continue
    if not rows:return 0
    conn=db_connect()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO period_groups(period_key,period_date,period_no,group_no,target_block,block_number,block_hash,numbers,single_count)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
                    ON CONFLICT(period_key,group_no) DO UPDATE SET
                      target_block=EXCLUDED.target_block, block_number=EXCLUDED.block_number,
                      block_hash=EXCLUDED.block_hash, numbers=EXCLUDED.numbers,
                      single_count=EXCLUDED.single_count
                """,rows)
        return len(rows)
    finally: db_release(conn)


def calibrated_group20_row(row):
    """Exclude period-group results saved under an obsolete target mapping."""
    ds=row['period_date'].isoformat() if hasattr(row['period_date'],'isoformat') else str(row['period_date'])
    return int(row['target_block'])==period_target_block(ds,int(row['period_no']))


def repair_calibrated_official_records(limit=5000):
    """Rebuild completed periods mapped with an older tail schedule from raw blocks.

    Keep the original block archive. Do not replace an old result with a guess:
    a period is rewritten only when its newly calibrated group-20 block exists.
    """
    conn=db_connect()
    if conn is None:return 0
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT period_key,period_date,period_no,target_block
                           FROM period_groups WHERE group_no=20
                           ORDER BY period_date DESC,period_no DESC LIMIT %s""",(int(limit),))
            saved=cur.fetchall()
    finally: db_release(conn)
    mismatches=[]
    for r in saved:
        ds=r['period_date'].isoformat() if hasattr(r['period_date'],'isoformat') else str(r['period_date'])
        target=period_target_block(ds,int(r['period_no']))
        if int(r['target_block'])!=target:
            mismatches.append((str(r['period_key']),ds,int(r['period_no']),target))
    if not mismatches:return 0
    # One indexed read for all candidate blocks; missing raw blocks remain untouched.
    needed=[target-(20-g) for _,_,_,target in mismatches for g in range(1,21)]
    raw=get_db_blocks(needed)
    repaired=0
    for key,ds,p,target in mismatches:
        if target not in raw:
            # No substitute result: keep the archived raw blocks and mark the
            # old wrong-target prediction unverified until data is available.
            conn=db_connect()
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute("""UPDATE ai_predictions SET actual_single=NULL,verified_at=NULL
                                       WHERE period_key=%s AND target_block<>%s""",(key,target))
            finally: db_release(conn)
            continue
        groups={}
        for g in range(1,21):
            bn=target-(20-g); row=raw.get(bn)
            if row is None:continue
            nums=row['numbers'] if isinstance(row['numbers'],list) else json.loads(row['numbers'])
            groups[str(g)]={'blockNumber':bn,'block':row['block_hash'],
                            'numbers':nums,'singleCount':int(row['single_count'])}
        persist_period_groups(ds,p,groups,target)
        conn=db_connect()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM period_groups WHERE period_key=%s AND target_block<>%s",(key,target))
                    # The old prediction remains for audit, but its wrong-block
                    # verification is invalid and must not inflate accuracy.
                    cur.execute("""UPDATE ai_predictions SET actual_single=NULL,verified_at=NULL
                                   WHERE period_key=%s AND target_block<>%s""",(key,target))
                    cur.execute("UPDATE period_runtime SET target_block=%s,updated_at=NOW() WHERE period_key=%s",(target,key))
            repaired+=1
        finally: db_release(conn)
    if mismatches:
        rebuild_omission_runtime()
    if repaired:
        record_system_event('tail_calibration_records_repaired',None,{'periods':repaired,'candidates':len(mismatches)})
    return repaired


def repair_recent_periods(limit=6):
    """Self-heal missing period_groups and verification from retained raw blocks."""
    global _runtime_health
    date_str,period,_,_=current_period(); repaired=0
    now_idx=period_index(date_str,period)
    for off in range(0,int(limit)):
        idx=now_idx-off; ordinal,zero=divmod(idx,1440); d=date.fromordinal(ordinal); p=zero+1
        ds=d.strftime('%Y-%m-%d'); target=period_target_block(ds,p)
        wanted=[target-(20-g) for g in range(1,21)]
        try: rows=get_db_blocks(wanted)
        except Exception: rows={}
        groups={}
        for g,bn in enumerate(wanted,1):
            row=rows.get(bn)
            if row:
                nums=row['numbers'] if isinstance(row['numbers'],list) else json.loads(row['numbers'])
                groups[str(g)]={'group':g,'blockNumber':bn,'block':row['block_hash'],
                                'numbers':nums,'singleCount':int(row['single_count'])}
        if groups:
            try: persist_period_groups(ds,p,groups,target); repaired += 1
            except Exception: pass
        if groups.get('20'):
            try: verify_prediction(ds,p,groups['20'])
            except Exception: pass
    with _runtime_health_lock:
        _runtime_health['lastRepairAt']=datetime.now(CN_TZ).isoformat(timespec='seconds')
        _runtime_health['repairCount']=int(_runtime_health.get('repairCount') or 0)+repaired
    return repaired


def set_period_runtime(date_str, period, target, state, groups_seen=0, error=None):
    key=f'{date_str}:{int(period):04d}'
    conn=None
    try:
        conn=db_connect(retries=0)
        if conn is None:return
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO period_runtime(period_key,period_date,period_no,target_block,state,groups_seen,last_error,updated_at)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT(period_key) DO UPDATE SET
                      target_block=EXCLUDED.target_block,state=EXCLUDED.state,
                      groups_seen=GREATEST(period_runtime.groups_seen,EXCLUDED.groups_seen),
                      last_error=EXCLUDED.last_error,updated_at=NOW()
                """,(key,date_str,int(period),int(target),state,int(groups_seen),error))
                if state=='AI_LOCKED':
                    cur.execute("UPDATE period_runtime SET prediction_locked_at=COALESCE(prediction_locked_at,NOW()) WHERE period_key=%s",(key,))
                elif state=='RESULT_READY':
                    cur.execute("UPDATE period_runtime SET result_ready_at=COALESCE(result_ready_at,NOW()) WHERE period_key=%s",(key,))
                elif state in ('VERIFIED','COMPLETE'):
                    cur.execute("UPDATE period_runtime SET verified_at=COALESCE(verified_at,NOW()) WHERE period_key=%s",(key,))
    except Exception as exc:
        with _runtime_health_lock:_runtime_health['lastDbError']=str(exc)[:300]
    finally:
        if conn is not None:db_release(conn)


def prediction_exists(period_key):
    conn=None
    try:
        conn=db_connect(retries=0)
        with conn.cursor() as cur:
            cur.execute("SELECT actual_single,locked_at FROM ai_predictions WHERE period_key=%s",(period_key,))
            return cur.fetchone()
    except Exception:return None
    finally:
        if conn is not None:db_release(conn)


def _target_block_observed(target):
    with _live_blocks_lock:
        return _live_latest_number is not None and _live_latest_number>=int(target)


def trace_period_lifecycle(date_str, period, target, event, groups=None, detail=None, increment_attempt=False,event_at=None):
    """Persist compact lifecycle diagnostics so Web can inspect the Worker process."""
    key=f'{date_str}:{int(period):04d}'
    groups=groups or {}
    present=sorted(int(x) for x in groups.keys() if str(x).isdigit())
    now=event_at or datetime.now(CN_TZ).isoformat(timespec='milliseconds')
    patch={str(event): {'at':now,'groups':present,'groupsSeen':len(present),
                        'latestLiveBlock':_live_latest_number,'target20':int(target)}}
    if detail is not None: patch[str(event)]['detail']=detail
    conn=None
    try:
        conn=db_connect(retries=0)
        if conn is None:return
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO period_runtime(period_key,period_date,period_no,target_block,state,groups_seen,g17_ready_at,lock_attempts,lifecycle_trace,updated_at)
                    VALUES(%s,%s,%s,%s,'COLLECTING',%s,CASE WHEN %s='g17_ready' THEN NOW() ELSE NULL END,%s,%s::jsonb,NOW())
                    ON CONFLICT(period_key) DO UPDATE SET
                      groups_seen=GREATEST(period_runtime.groups_seen,EXCLUDED.groups_seen),
                      g17_ready_at=CASE WHEN %s='g17_ready' THEN COALESCE(period_runtime.g17_ready_at,NOW()) ELSE period_runtime.g17_ready_at END,
                      lock_attempts=period_runtime.lock_attempts + %s,
                      lifecycle_trace=COALESCE(period_runtime.lifecycle_trace,'{}'::jsonb) || EXCLUDED.lifecycle_trace,
                      updated_at=NOW()
                """,(key,date_str,int(period),int(target),len(present),str(event),1 if increment_attempt else 0,json.dumps(patch),str(event),1 if increment_attempt else 0))
    except Exception as exc:
        with _runtime_health_lock:_runtime_health['lastDbError']=str(exc)[:300]
    finally:
        if conn is not None:db_release(conn)


def autonomous_period_worker():
    """Durable period state machine with persisted V8.1.3 lifecycle tracing."""
    last_key=None; last_seen=-1; last_official=False; last_attempt_at=0
    while True:
        worker_touch('period-engine')
        try:
            ds,p,_,_=current_period(); target=period_target_block(ds,p); key=f'{ds}:{int(p):04d}'
            groups,_=collect_groups_live(ds,p); seen=len(groups); official=groups.get('20')
            if key != last_key:
                last_key=key; last_seen=-1; last_official=False; last_attempt_at=0
                trace_period_lifecycle(ds,p,target,'period_enter',groups)
            changed=seen!=last_seen
            if changed:
                last_seen=seen
                trace_period_lifecycle(ds,p,target,'groups_progress',groups)
            g17=all(str(i) in groups for i in range(1,18))
            if official:
                if not last_official:
                    last_official=True
                    trace_period_lifecycle(ds,p,target,'g20_seen',groups)
                row=prediction_exists(key)
                if row is None:
                    # Never fabricate after result. Persist the exact reason/context.
                    trace_period_lifecycle(ds,p,target,'missed_prediction',groups,
                        {'reason':'group20_seen_before_durable_prediction','g17Complete':g17})
                    set_period_runtime(ds,p,target,'MISSED_PREDICTION',seen,'group20_seen_before_durable_prediction')
                else:
                    set_period_runtime(ds,p,target,'RESULT_READY',seen)
                    try:
                        if verify_prediction(ds,p,official):
                            trace_period_lifecycle(ds,p,target,'verified',groups)
                            set_period_runtime(ds,p,target,'VERIFIED',seen)
                    except Exception as exc:
                        trace_period_lifecycle(ds,p,target,'verify_error',groups,str(exc)[:300])
                        set_period_runtime(ds,p,target,'RESULT_READY',seen,str(exc)[:300])
            elif g17:
                with _g17_durable_lock:
                    already_locked=key in _g17_durable
                if already_locked or time.monotonic()-last_attempt_at<0.7:
                    time.sleep(0.20)
                    continue
                last_attempt_at=time.monotonic()
                attempt_at=datetime.now(timezone.utc).isoformat(timespec='milliseconds')
                try:
                    # Prediction persistence has priority over lifecycle DB
                    # writes; trace the actual start time after the attempt.
                    result=save_prediction_if_ready(ds,p,groups,target,force_backend=True)
                    flush_pending_ai_predictions()
                    row=prediction_exists(key)
                    trace_period_lifecycle(ds,p,target,'g17_ready',groups,event_at=attempt_at)
                    trace_period_lifecycle(ds,p,target,'lock_attempt',groups,
                                           increment_attempt=True,event_at=attempt_at)
                    if row is not None:
                        trace_period_lifecycle(ds,p,target,'lock_success',groups,{'result':bool(result)})
                        set_period_runtime(ds,p,target,'AI_LOCKED',seen)
                        with _g17_durable_lock:_g17_durable.add(key)
                    else:
                        trace_period_lifecycle(ds,p,target,'lock_not_durable',groups,
                            {'pending':key in _pending_ai_predictions})
                        set_period_runtime(ds,p,target,'G17_READY',seen,'lock_not_durable_retrying')
                except Exception as exc:
                    with _runtime_health_lock:_runtime_health['lastAiError']=str(exc)[:300]
                    trace_period_lifecycle(ds,p,target,'lock_error',groups,str(exc)[:300])
                    set_period_runtime(ds,p,target,'G17_READY',seen,str(exc)[:300])
            else:
                if changed:set_period_runtime(ds,p,target,'COLLECTING',seen)
        except Exception as exc:
            worker_touch('period-engine',exc)
        time.sleep(0.20)

def _start_managed_worker(name, target):
    th=threading.Thread(target=target,name=name,daemon=True)
    with _worker_threads_lock:_worker_threads[name]=th
    th.start(); return th


def supervisor_worker():
    """Restart workers that exit. Heartbeats expose blocked workers for diagnosis."""
    targets={'db-writer':db_writer_worker,'result-db-writer':result_db_writer_worker,'tron-ingest':tron_ingest_worker,
             'target-result-fast':target_result_fast_worker,'g17-fast':g17_fast_worker,
             'g17-event':g17_event_worker,'smart-db':smart_db_worker,
             'hash-research':hash_research_worker,'prediction-context':prediction_context_worker,
             'research-verify':research_verify_worker}
    if RESEARCH_ONLY_MODE:targets['research-period']=research_period_worker
    if not RESEARCH_ONLY_MODE:
        targets.update({'ai-prediction':ai_prediction_worker,'period-engine':autonomous_period_worker})
    while True:
        worker_touch('supervisor')
        for name,target in targets.items():
            with _worker_threads_lock: th=_worker_threads.get(name)
            if th is None or not th.is_alive():
                try:
                    _start_managed_worker(name,target)
                    with _runtime_health_lock:
                        _worker_restarts[name]=int(_worker_restarts.get(name,0))+1
                    record_system_event('worker_restart',None,{'worker':name,'count':_worker_restarts[name]})
                except Exception as exc: worker_touch('supervisor',exc)
        time.sleep(3.0)


def smart_db_worker():
    """Continuously materialize current period and repair recent holes."""
    last_repair=0; last_calibration=0
    while True:
        worker_touch('smart-db')
        try:
            ds,p,_,_=current_period()
            groups,target=collect_groups_live(ds,p)
            if groups:
                persist_period_groups(ds,p,groups,target)
            if time.time()-last_repair > 30:
                last_repair=time.time()
                repair_recent_periods(6)
            if time.time()-last_calibration > 1800:
                last_calibration=time.time()
                repair_calibrated_official_records()
        except Exception as exc:
            with _runtime_health_lock:
                _runtime_health['lastDbError']=str(exc)[:300]
        time.sleep(1.0)


def get_hash_model_snapshot(current_key):
    """One small shared DB read on the prediction path; never train here."""
    conn=None
    try:
        conn=db_connect(retries=0)
        if conn is None:return None
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT trained_through,snapshot FROM hash_model_runtime WHERE singleton=1")
            row=cur.fetchone()
        if not row or not row['trained_through'] or row['trained_through']>=current_key:return None
        snap=row['snapshot']
        return json.loads(snap) if isinstance(snap,str) else snap
    except Exception:
        return None
    finally:
        if conn is not None:db_release(conn)


def save_research_forecasts(date_str,period,target,historical,snapshot,groups=None):
    """Persist immutable candidate rows before the target exists on-chain."""
    key=f'{date_str}:{int(period):04d}'
    if _target_block_observed(target):return False
    candidates={'fixed_3':{'single':3,'scores':[int(i==3) for i in range(8)]},
                'fixed_4':{'single':4,'scores':[int(i==4) for i in range(8)]}}
    if snapshot and snapshot.get('trainedThrough') and snapshot['trainedThrough']<key:
        candidates.update(hash_research.research_forecasts(snapshot,groups,historical))
        for position,method in enumerate(snapshot.get('replayTopTwo') or []):
            if position>=2:break
            if method in candidates:
                candidates['trial_first' if position==0 else 'trial_second']={
                    **candidates[method], 'researchMethod':method}
    if not candidates:return False
    conn=db_connect(retries=0)
    if conn is None:return False
    try:
        with conn:
            with conn.cursor() as cur:
                for name,choice in candidates.items():
                    cur.execute("""INSERT INTO research_predictions
                        (period_key,period_date,period_no,target_block,candidate,prediction,scores,model_version,trained_through,research_method)
                        SELECT %s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s
                        WHERE NOT EXISTS (SELECT 1 FROM tron_blocks WHERE block_number=%s)
                        ON CONFLICT(period_key,candidate) DO NOTHING""",
                        (key,date_str,int(period),int(target),name,int(choice['single']),
                         json.dumps(choice['scores']),RESEARCH_VERSION,
                         snapshot['trainedThrough'] if snapshot else 'baseline',
                         choice.get('researchMethod',name),int(target)))
                return True
    finally:db_release(conn)


def save_history_shadow_prediction(date_str,period,target,historical,snapshot):
    """Backwards-compatible caller for early pure-history and control rows."""
    return save_research_forecasts(date_str,period,target,historical,snapshot)


def prediction_context_worker():
    """Prepare DB-heavy prior-period evidence before the G17 deadline."""
    shadow_written=None
    while True:
        worker_touch('prediction-context')
        try:
            ds,p,_,_=current_period(); key=f'{ds}:{int(p):04d}'
            with _prediction_context_lock:
                ready=_prediction_context['key']==key and _prediction_context['value'] is not None
            if not ready:
                historical=get_historical_official_singles(ds,p)
                snapshot=get_hash_model_snapshot(key)
                # Research-only history forecast has no need to wait for the
                # expensive group-position and ensemble performance queries.
                if shadow_written!=key and save_history_shadow_prediction(
                        ds,p,period_target_block(ds,p),historical,snapshot):
                    shadow_written=key
                relation=group20_relation_model(ds,p) if not RESEARCH_ONLY_MODE else None
                performance=research_model_performance(500) if not RESEARCH_ONLY_MODE else None
                with _prediction_context_lock:
                    _prediction_context.update({'key':key,'value':{'historical':historical,
                        'relation':relation,'performance':performance,'hashSnapshot':snapshot}})
            if shadow_written!=key:
                with _prediction_context_lock:
                    context=_prediction_context['value'] if _prediction_context['key']==key else None
                if context and save_history_shadow_prediction(ds,p,period_target_block(ds,p),
                                                context['historical'],context['hashSnapshot']):
                    shadow_written=key
        except Exception as exc:
            worker_touch('prediction-context',exc)
        time.sleep(2.0)


def hash_research_worker():
    """Train from all stored complete periods; stream rows without a 2500 cap."""
    time.sleep(15)
    while True:
        worker_touch('hash-research')
        conn=None
        try:
            ds,p,_,_=current_period(); current_key=f'{ds}:{int(p):04d}'
            conn=db_connect(retries=0)
            with conn.cursor(name='research_history_stream',cursor_factory=RealDictCursor) as cur:
                cur.itersize=2000
                cur.execute("""SELECT g.period_key,g.group_no,g.block_hash,g.single_count,
                           p.period_date,p.period_no,p.target_block
                    FROM period_groups p JOIN period_groups g ON g.period_key=p.period_key
                    WHERE p.group_no=20 AND p.period_key<%s
                      AND (g.group_no BETWEEN 1 AND 17 OR g.group_no=20)
                      AND g.target_block=p.target_block
                    ORDER BY p.period_date,p.period_no,g.group_no""",(current_key,))
                def calibrated_rows():
                    checked_key=None;valid=False
                    for row in cur:
                        if row['period_key']!=checked_key:
                            checked_key=row['period_key']
                            valid=calibrated_group20_row(row)
                        if valid:yield row
                examples=hash_research.build_examples(calibrated_rows())
            db_release(conn);conn=None
            snapshot=hash_research.train_snapshot(examples)
            snapshot['completePeriods']=len(examples)
            replay=hash_research.replay_first_1000_next_1500(examples)
            snapshot['replayAlpha']=replay['alphas']
            snapshot['replayTopTwo']=replay['topTwo']
            conn=db_connect(retries=0)
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""INSERT INTO hash_model_runtime(singleton,trained_through,snapshot,updated_at)
                        VALUES(1,%s,%s::jsonb,NOW()) ON CONFLICT(singleton) DO UPDATE
                        SET trained_through=EXCLUDED.trained_through,snapshot=EXCLUDED.snapshot,
                            updated_at=EXCLUDED.updated_at""",
                        (snapshot.get('trainedThrough'),json.dumps(snapshot)))
                    cur.execute("""INSERT INTO research_replay_runtime
                        (singleton,research_version,report,updated_at)
                        VALUES(1,%s,%s::jsonb,NOW()) ON CONFLICT(singleton) DO UPDATE SET
                        research_version=EXCLUDED.research_version,report=EXCLUDED.report,
                        updated_at=NOW()""",(RESEARCH_VERSION,json.dumps(replay)))
            record_system_event('hash_research_trained',current_key,
                                {k:snapshot.get(k) for k in ('sample','testSample','active','baselineLoss','modelLoss')})
        except Exception as exc:
            worker_touch('hash-research',exc)
        finally:
            if conn is not None:db_release(conn)
        time.sleep(900)


def research_verify_worker():
    """Match immutable research forecasts to saved original target blocks."""
    last_key=None
    while True:
        worker_touch('research-verify')
        conn=None
        try:
            conn=db_connect(retries=0)
            with conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""UPDATE research_predictions p SET
                        actual_single=b.single_count,verified_at=NOW(),
                        valid_pre_result=(p.locked_at<b.block_time)
                        FROM tron_blocks b
                        WHERE p.target_block=b.block_number AND b.block_time IS NOT NULL
                          AND p.verified_at IS NULL AND p.model_version=%s""",(RESEARCH_VERSION,))
                    cur.execute("""SELECT MAX(period_key) AS last_key
                        FROM research_predictions
                        WHERE model_version=%s AND valid_pre_result=TRUE""",(RESEARCH_VERSION,))
                    latest=cur.fetchone()['last_key']
                    if latest and latest!=last_key:
                        cur.execute("""SELECT period_key,candidate,prediction,actual_single
                            FROM research_predictions
                            WHERE model_version=%s AND valid_pre_result=TRUE
                              AND actual_single IS NOT NULL
                            ORDER BY period_date DESC,period_no DESC LIMIT 20000""",(RESEARCH_VERSION,))
                        evidence=research_selection.choose_candidate(cur.fetchall())
                        cur.execute("""INSERT INTO research_selection
                            (singleton,research_version,last_verified_period,status,candidate,evidence,updated_at)
                            VALUES(1,%s,%s,%s,%s,%s::jsonb,NOW())
                            ON CONFLICT(singleton) DO UPDATE SET
                              research_version=EXCLUDED.research_version,
                              last_verified_period=EXCLUDED.last_verified_period,
                              status=EXCLUDED.status,candidate=EXCLUDED.candidate,
                              evidence=EXCLUDED.evidence,updated_at=NOW()""",
                            (RESEARCH_VERSION,latest,evidence['status'],evidence['candidate'],
                             json.dumps(evidence)))
                        last_key=latest
        except Exception as exc:
            worker_touch('research-verify',exc)
        finally:
            if conn is not None:db_release(conn)
        time.sleep(3.0)


def research_period_worker():
    """Retry G17 research if the fast event precedes prepared history context."""
    saved_key=None
    while True:
        worker_touch('research-period')
        try:
            ds,p,_,_=current_period();key=f'{ds}:{int(p):04d}'
            if key!=saved_key:
                with _prediction_context_lock:
                    context=_prediction_context['value'] if _prediction_context['key']==key else None
                if context:
                    groups,target=collect_groups_live(ds,p)
                    if (not groups.get('20') and
                            all(str(i) in groups for i in range(1,18)) and
                            save_research_forecasts(ds,p,target,
                                context['historical'],context['hashSnapshot'],groups)):
                        saved_key=key
        except Exception as exc:
            worker_touch('research-period',exc)
        time.sleep(0.6)


def ai_prediction_worker():
    """Server-owned AI lifecycle; no browser request is required."""
    last_verified_key=None
    while True:
        worker_touch('ai-prediction')
        try:
            flush_pending_ai_predictions()
            date_str,period,_,_=current_period()
            groups,target20=collect_groups_live(date_str,period)
            official=groups.get('20'); key=f'{date_str}:{int(period):04d}'
            if not official and all(str(i) in groups for i in range(1,18)):
                try: save_prediction_if_ready(date_str,period,groups,target20,force_backend=True)
                except Exception: pass
            if official and last_verified_key != key:
                # Flush first: a prediction captured before group20 may still be
                # waiting for PostgreSQL. Never declare verification complete
                # when UPDATE matched zero rows.
                flush_pending_ai_predictions()
                try:
                    if verify_prediction(date_str,period,official):
                        last_verified_key=key
                        with _ai_summary_cache_lock:
                            _ai_summary_cache.update({'key':None,'at':0,'value':None})
                except Exception:
                    pass
        except Exception as exc:
            with _runtime_health_lock:
                _runtime_health['lastAiError']=str(exc)[:300]
        time.sleep(0.35)


def start_worker_once():
    global _worker_started
    if not DATABASE_URL or psycopg2 is None:
        return
    with _worker_lock:
        if _worker_started:
            return
        _worker_started = True
        _start_managed_worker('db-writer',db_writer_worker)
        _start_managed_worker('result-db-writer',result_db_writer_worker)
        _start_managed_worker('tron-ingest',tron_ingest_worker)
        _start_managed_worker('target-result-fast',target_result_fast_worker)
        _start_managed_worker('g17-fast',g17_fast_worker)
        _start_managed_worker('g17-event',g17_event_worker)
        if not RESEARCH_ONLY_MODE:_start_managed_worker('ai-prediction',ai_prediction_worker)
        _start_managed_worker('smart-db',smart_db_worker)
        if not RESEARCH_ONLY_MODE:_start_managed_worker('period-engine',autonomous_period_worker)
        _start_managed_worker('omission-engine',omission_engine_worker)
        _start_managed_worker('hash-research',hash_research_worker)
        _start_managed_worker('prediction-context',prediction_context_worker)
        _start_managed_worker('research-verify',research_verify_worker)
        if RESEARCH_ONLY_MODE:_start_managed_worker('research-period',research_period_worker)
        _start_managed_worker('supervisor',supervisor_worker)


def collect_period_groups(date_str, period, latest_number, state):
    """Incrementally collect the 20 positions.

    The table is a fixed 20-row frame. Each API call fills only a small batch
    of missing rows, so the page does not wait for all 20 blocks before showing
    anything. Group 20 is included in the first batch so the official result
    can become available immediately when its block exists.
    """
    target20 = period_target_block(date_str, period)
    if target20 is None:
        if state and state.get('date') == date_str and state.get('period') == f'{int(period):04d}' and state.get('blockNumber') is not None:
            target20 = int(state['blockNumber'])
        else:
            return {}, None

    groups = (state or {}).get('groups', {}) if state else {}
    if not isinstance(groups, dict):
        groups = {}
    groups = {str(k): v for k, v in groups.items() if isinstance(v, dict)}

    # Only fetch a small batch each poll. This is the key change: rows already
    # fetched stay on screen, while missing rows are filled progressively.
    missing = []
    for group_no in range(1, 21):
        block_number = target20 - (20 - group_no)
        if block_number <= latest_number:
            key = str(group_no)
            if not (key in groups and groups[key].get('blockNumber') == block_number and groups[key].get('numbers')):
                missing.append((group_no, block_number))

    # Put the official group 20 first, then fill the remaining rows in order.
    missing.sort(key=lambda x: (0 if x[0] == 20 else 1, x[0]))
    batch = missing[:4]

    def fetch_one(item):
        group_no, block_number = item
        block = fetch_block_by_number(block_number)
        nums = calc_numbers(block['block'])
        return str(group_no), {
            'group': group_no,
            'blockNumber': block_number,
            'block': block['block'],
            'numbers': [f'{n:02d}' for n in nums],
            'singleCount': calc_single_count(nums)
        }

    if batch:
        # Parallelize the small batch so a slow provider does not make the page
        # wait 4x the network timeout. Failed rows simply remain '--' and are
        # retried on the next poll.
        with ThreadPoolExecutor(max_workers=len(batch)) as pool:
            futures = [pool.submit(fetch_one, item) for item in batch]
            for future in as_completed(futures):
                try:
                    key, row = future.result()
                    groups[key] = row
                except Exception:
                    pass

    return groups, target20

def collect_groups_live(date_str, period):
    """Return a genuinely incremental current-period group list.

    Live RAM is the realtime source. PostgreSQL is used only once to seed a
    period after a restart, never on every 200 ms frontend poll. New blocks
    published by the collector are merged immediately, so group N appears as
    soon as that exact block is available.
    """
    target20 = period_target_block(date_str, period)
    wanted = [target20 - (20 - g) for g in range(1, 21)]
    period_key = f'{date_str}:{int(period):04d}'
    with _period_group_cache_lock:
        cache = _period_group_cache.get(period_key)
    if cache is None:
        try:
            seed = get_db_blocks(wanted)
        except Exception:
            seed = {}
        cache = dict(seed)
        with _period_group_cache_lock:
            _period_group_cache.clear()
            _period_group_cache[period_key] = cache
    with _live_blocks_lock:
        live = {bn: dict(_live_blocks[bn]) for bn in wanted if bn in _live_blocks}
    if live:
        with _period_group_cache_lock:
            cache = _period_group_cache.setdefault(period_key, {})
            cache.update(live)
            rows = dict(cache)
    else:
        rows = dict(cache)
    groups = {}
    for g, bn in enumerate(wanted, start=1):
        row = rows.get(bn)
        if row:
            nums = row['numbers'] if isinstance(row['numbers'], list) else json.loads(row['numbers'])
            groups[str(g)] = {
                'group': g, 'period': f'{int(period):04d}', 'periodKey': period_key,
                'target20': int(target20),
                'blockNumber': bn, 'block': row['block_hash'],
                'numbers': nums, 'singleCount': int(row['single_count'])
            }
    return groups, target20


def build_recent_official_history(date_str, period, count=20):
    """Build recent official results with one batched DB read, cached briefly."""
    cache_key = f"{date_str}:{int(period):04d}:{int(count)}"
    now = time.time()
    with _history_cache_lock:
        if _history_cache.get('key') == cache_key and _history_cache.get('value') is not None and now - _history_cache.get('at', 0) < 2.0:
            return list(_history_cache['value'])
    items=[]; wanted=[]
    idx_now=period_index(date_str, period)
    for offset in range(count-1,-1,-1):
        idx=idx_now-offset; ordinal,zero=divmod(idx,1440); d=date.fromordinal(ordinal); p=zero+1
        target=period_target_block(d.strftime('%Y-%m-%d'),p)
        items.append((d,p,target)); wanted.append(target)
    try: rows=get_db_blocks(wanted)
    except Exception: rows={}
    history=[]
    for d,p,target in items:
        row=rows.get(target)
        if row:
            history.append({'platformPeriod':d.strftime('%y%m%d')+f'{p:04d}','period':f'{p:04d}','blockNumber':target,'singleCount':int(row['single_count'])})
    with _history_cache_lock:
        _history_cache.update({'key':cache_key,'at':now,'value':list(history)})
    return history


def stats_from_groups(groups):
    """Calculate statistics from groups 1-18 of the current 20-group round.

    Groups 19 and 20 are still recorded, but they are not part of the
    statistics sample. Group 20 being available must NOT clear groups 1-18.
    """
    groups = groups if isinstance(groups, dict) else {}
    counts = {str(i): 0 for i in range(8)}
    used = 0
    for group_no in range(1, 19):
        row = groups.get(str(group_no))
        if row and row.get('singleCount') is not None:
            counts[str(int(row['singleCount']))] += 1
            used += 1
    stats = []
    for i in range(8):
        c = counts[str(i)]
        stats.append({'single': i, 'count': c, 'probability': round(c / used * 100, 2) if used else 0})
    max_count = max((x['count'] for x in stats), default=0)
    highest = [x for x in stats if x['count'] == max_count] if used else []
    return {'sampleSize': used, 'stats': stats, 'highest': highest}



def get_historical_official_singles(date_str, period, limit=1000):
    """Return prior official group-20 singles using calibrated period targets.

    This avoids assuming every historical boundary is exactly +20 blocks; known
    +18 calibration transitions are respected by period_target_block().
    """
    cache_key=f'{date_str}:{int(period):04d}:{int(limit)}'; now=time.time()
    with _historical_singles_cache_lock:
        if _historical_singles_cache.get('key')==cache_key and _historical_singles_cache.get('value') is not None and now-_historical_singles_cache.get('at',0)<10:
            return list(_historical_singles_cache['value'])
    idx_now=period_index(date_str,period); wanted=[]
    for off in range(1,int(limit)+1):
        idx=idx_now-off; ordinal,zero=divmod(idx,1440); d=date.fromordinal(ordinal); p=zero+1
        wanted.append(period_target_block(d.strftime('%Y-%m-%d'),p))
    try: rows=get_db_blocks(wanted)
    except Exception: rows={}
    values=[int(rows[t]['single_count']) for t in wanted if t in rows and rows[t].get('single_count') is not None]
    with _historical_singles_cache_lock:
        _historical_singles_cache.update({'key':cache_key,'at':now,'value':list(values)})
    return values


def group20_relation_model(date_str, period, lookback=120):
    """Historical position model: which groups 1-19 have matched group 20.

    Uses completed prior periods only.  For each fixed group position it stores
    both the overall match rate and the conditional distribution of group-20
    single-count given that position's observed single-count.  Cached briefly
    so the 1-second frontend polling does not hammer PostgreSQL.
    """
    cache_key = f'{date_str}:{int(period):04d}'
    now = time.time()
    with _relation_cache_lock:
        if _relation_cache.get('key') == cache_key and _relation_cache.get('value') is not None and now - _relation_cache.get('at', 0) < 20:
            return _relation_cache['value']

    periods = []
    idx_now = period_index(date_str, period)
    wanted = []
    for off in range(1, int(lookback) + 1):
        idx = idx_now - off
        ordinal, zero = divmod(idx, 1440)
        d = date.fromordinal(ordinal)
        pno = zero + 1
        target = period_target_block(d.strftime('%Y-%m-%d'), pno)
        bns = [target - (20-g) for g in range(1,21)]
        periods.append((d.strftime('%Y-%m-%d'), pno, target, bns))
        wanted.extend(bns)
    try:
        rows = get_db_blocks(wanted)
    except Exception:
        rows = {}

    pos = {i: {'matches':0, 'sample':0, 'conditional': {x:[0]*8 for x in range(8)}} for i in range(1,20)}
    complete = 0
    for _, _, target, bns in periods:
        r20 = rows.get(target)
        if not r20:
            continue
        actual = int(r20['single_count'])
        complete += 1
        for g in range(1,20):
            row = rows.get(bns[g-1])
            if not row:
                continue
            observed = int(row['single_count'])
            pos[g]['sample'] += 1
            if observed == actual:
                pos[g]['matches'] += 1
            pos[g]['conditional'][observed][actual] += 1
    ranking = []
    for g in range(1,20):
        st = pos[g]
        rate = (st['matches']/st['sample']*100.0) if st['sample'] else 0.0
        ranking.append({'group':g,'matches':st['matches'],'sample':st['sample'],'matchRate':round(rate,2)})
    ranking.sort(key=lambda x:(-x['matchRate'],-x['sample'],x['group']))
    value = {'samplePeriods':complete,'positions':pos,'ranking':ranking}
    with _relation_cache_lock:
        _relation_cache.update({'key':cache_key,'at':now,'value':value})
    return value



def omission_hazard_model(hist):
    """Empirical P(next official result == class | current omission length).

    `hist` is newest -> oldest.  We estimate each class separately from prior
    official results only.  Beta smoothing and a +/-2 omission band keep tiny
    samples from dominating the model.  Returned values are evidence summaries,
    not guaranteed probabilities.
    """
    seq=[int(x) for x in reversed(hist) if 0 <= int(x) <= 7]
    out={}
    base_counts=[seq.count(i) for i in range(8)]
    n=max(1,len(seq))
    for cls in range(8):
        exposures={} ; hits={} ; omit=0
        for actual in seq:
            k=min(120,omit)
            exposures[k]=exposures.get(k,0)+1
            if actual==cls:
                hits[k]=hits.get(k,0)+1
                omit=0
            else:
                omit+=1
        current=0
        for x in hist:
            if int(x)==cls: break
            current+=1
        lo=max(0,current-2); hi=min(120,current+2)
        sample=sum(exposures.get(k,0) for k in range(lo,hi+1))
        hit=sum(hits.get(k,0) for k in range(lo,hi+1))
        base=(base_counts[cls]+1)/(n+8)
        # Prior strength 12 periods centered on the class long-run rate.
        prob=(hit+12*base)/(sample+12) if sample>=0 else base
        lift=prob/base if base>0 else 1.0
        zones=[]
        for k,sm in exposures.items():
            if sm < 5: continue
            h=hits.get(k,0); pr=(h+8*base)/(sm+8); lf=pr/base if base else 1.0
            zones.append((lf,sm,k,pr))
        zones.sort(reverse=True)
        best=zones[0] if zones else (1.0,0,None,base)
        out[cls]={'currentOmission':current,'band':[lo,hi],'sample':sample,'hits':hit,
                  'conditionalRate':round(prob*100,2),'baseRate':round(base*100,2),
                  'lift':round(lift,3),'bestOmission':best[2],
                  'bestRate':round(best[3]*100,2) if best[2] is not None else None,
                  'bestSample':best[1]}
    return out

def pattern_insights(date_str, period, limit=1000):
    """History-tab evidence used by AI: distribution + omission hazard + hash diagnostics."""
    idx_now=period_index(date_str,period); items=[]; wanted=[]
    for off in range(1,int(limit)+1):
        idx=idx_now-off; ordinal,zero=divmod(idx,1440); d=date.fromordinal(ordinal); pno=zero+1
        target=period_target_block(d.strftime('%Y-%m-%d'),pno); items.append((d,pno,target)); wanted.append(target)
    try: rows=get_db_blocks(wanted)
    except Exception: rows={}
    hist=[]; hashes=[]
    for _,_,target in items:
        r=rows.get(target)
        if r and r.get('single_count') is not None:
            hist.append(int(r['single_count'])); hashes.append(str(r.get('block_hash') or '').lower())
    counts=[hist.count(i) for i in range(8)]; total=len(hist)
    dist=[{'single':i,'count':counts[i],'rate':round(counts[i]/total*100,2) if total else 0} for i in range(8)]
    hazard=omission_hazard_model(hist)
    # Descriptive hash diagnostics only.  They are deliberately not labelled as a
    # predictable next-hash formula because cryptographic hashes should not expose one.
    ae={c:0 for c in 'abcde'}; digits={str(i):0 for i in range(10)}
    for h in hashes:
        for ch in h:
            if ch in ae: ae[ch]+=1
            if ch in digits: digits[ch]+=1
    return {'samplePeriods':total,'requestedPeriods':int(limit),'distribution':dist,
            'omissionHazard':{str(k):v for k,v in hazard.items()},
            'hashDiagnostics':{'sampleHashes':len(hashes),'aeCounts':ae,'digitCounts':digits},
            'note':'遗漏条件率为历史统计并经过平滑；哈希字符统计只用于检测偏差，不代表存在可追踪公式。'}

def ai_analysis_from_data(groups, historical, relation=None):
    """Adaptive 8-class scoring model.

    Every class 单0..单7 is scored independently.  The model combines:
    - current 1-18 group distribution
    - short/medium/long historical windows
    - omission/recency
    - transition behaviour after the most recent official result
    - recent-vs-long momentum

    The returned percentages are normalized *model scores*, not guaranteed
    probabilities.  No class is artificially promoted merely for variety.
    """
    current = [int(groups[str(i)]['singleCount']) for i in range(1, 20)
               if str(i) in groups and groups[str(i)].get('singleCount') is not None]
    if not current:
        return None, {}

    hist = [int(x) for x in historical if 0 <= int(x) <= 7]

    def dist(values):
        counts = [0] * 8
        for x in values:
            counts[x] += 1
        n = len(values)
        return [(counts[i] + 1.0) / (n + 8.0) for i in range(8)]  # Laplace smoothing

    cur_d = dist(current)
    d20 = dist(hist[:20])
    d50 = dist(hist[:50])
    d100 = dist(hist[:100])
    dall = dist(hist)

    # How long each class has been absent from official group-20 results.
    omission = [0] * 8
    cap = max(20, min(80, len(hist)))
    for i in range(8):
        n = 0
        for x in hist[:cap]:
            if x == i:
                break
            n += 1
        omission[i] = n
    max_omit = max(omission) if omission else 0

    # Empirical next-result distribution conditional on the latest official
    # result. Historical is newest -> oldest, so hist[j] is followed by hist[j-1].
    transition = [0] * 8
    transition_n = 0
    if hist:
        last = hist[0]
        for j in range(1, min(len(hist), 400)):
            if hist[j] == last:
                transition[hist[j-1]] += 1
                transition_n += 1
    trans_d = [(transition[i] + 1.0) / (transition_n + 8.0) for i in range(8)]

    hazard = omission_hazard_model(hist)
    relation_signal = [1.0/8.0] * 8
    if relation and relation.get('positions'):
        votes = [1.0] * 8
        weight_total = 8.0
        for g in range(1,20):
            row = groups.get(str(g))
            st = relation['positions'].get(g) if isinstance(relation.get('positions'), dict) else None
            if not row or not st or row.get('singleCount') is None:
                continue
            observed = int(row['singleCount'])
            cond = st.get('conditional', {}).get(observed, [0]*8)
            n = sum(cond)
            # fixed-position historical reliability; Laplace-smoothed
            w = min(2.0, 0.5 + st.get('sample',0)/80.0)
            for i in range(8):
                votes[i] += w * ((cond[i] + 1.0) / (n + 8.0))
            weight_total += w
        total_votes = sum(votes) or 1.0
        relation_signal = [v/total_votes for v in votes]

    raw = {}
    for i in range(8):
        # Positive momentum means the class is appearing more in the recent
        # window than in the long-run window.  It is deliberately bounded.
        momentum = max(-0.12, min(0.12, d20[i] - dall[i]))
        omit_signal = (omission[i] / max_omit) if max_omit else 0.0
        raw[i] = max(0.000001,
            0.24 * cur_d[i] +
            0.18 * d20[i] +
            0.15 * d50[i] +
            0.12 * d100[i] +
            0.12 * dall[i] +
            0.09 * trans_d[i] +
            0.16 * relation_signal[i] +
            0.03 * omit_signal +
            0.02 * (0.5 + momentum) +
            0.10 * dall[i] * max(0.60, min(1.40, float(hazard.get(i,{}).get('lift',1.0))))
        )

    total = sum(raw.values()) or 1.0
    scores = {i: raw[i] / total * 100.0 for i in range(8)}
    ranked = sorted(range(8), key=lambda i: (-scores[i], i))
    pick = ranked[0]
    return pick, {str(i): round(scores[i], 2) for i in range(8)}

def ai_conclusion17_from_data(groups, historical, date_str=None, period=None, target20=None):
    """17-group conclusion model using ONLY groups 1..17 of one exact period.

    When period identity is supplied, every source row is verified against that
    period's expected block number. This prevents rows from a previous/next
    period or a stale cache from being mixed into the 17-group model.
    """
    if date_str is not None and period is not None:
        target20 = int(target20 if target20 is not None else period_target_block(date_str, period))
        for i in range(1, 18):
            row = groups.get(str(i)) if isinstance(groups, dict) else None
            expected = target20 - (20 - i)
            if not row or int(row.get('blockNumber', -1)) != expected:
                return None
    vals=[int(groups[str(i)]['singleCount']) for i in range(1,18)
          if str(i) in groups and groups[str(i)].get('singleCount') is not None]
    if len(vals) < 17:
        return None
    hist=[int(x) for x in historical if 0 <= int(x) <= 7]
    cur=[vals.count(i) for i in range(8)]
    h50=[hist[:50].count(i) for i in range(8)]
    hall=[hist.count(i) for i in range(8)]
    # Natural binomial baseline for 7 odd/even observations. This prevents the
    # model from mistaking the expected 单3/单4 concentration for a discovered rule.
    baseline=[1,7,21,35,35,21,7,1]
    bsum=sum(baseline)
    baseline=[x/bsum for x in baseline]
    hazard=omission_hazard_model(hist)
    scores={}
    for i in range(8):
        recent=(h50[i]+1)/(len(hist[:50])+8)
        long=(hall[i]+1)/(len(hist)+8)
        expected=17*baseline[i]
        # Current 17-group frequency is used as context, but both unusually high
        # and unusually low states are bounded so random streaks cannot dominate.
        z=max(-1.5,min(1.5,(cur[i]-expected)/(max(expected,1)**0.5)))
        context=0.5 + 0.08*z
        hz=max(0.65,min(1.35,float(hazard.get(i,{}).get('lift',1.0))))
        scores[i]=max(.0001, .40*recent + .35*long + .15*baseline[i] + .10*long*hz) * context
    total=sum(scores.values()) or 1
    norm={str(i):round(scores[i]/total*100,2) for i in range(8)}
    pick=max(range(8), key=lambda i:(scores[i],-i))
    ordered=sorted(cur)
    rank=ordered.index(cur[pick])
    mode='低频倾向' if rank<=2 else ('高频倾向' if rank>=5 else '居中倾向')
    return {'single':pick,'scores':norm,'mode':mode,'sampleSize':17}


def save_conclusion17_if_ready(date_str, period, groups, target20):
    if groups.get('20') or any(str(i) not in groups for i in range(1,18)):
        return None
    historical=get_historical_official_singles(date_str,period)
    model=ai_conclusion17_from_data(groups,historical,date_str,period,target20)
    if not model: return None
    key=f'{date_str}:{int(period):04d}'
    conn=db_connect()
    if conn is None: return model
    try:
        with conn:
            with conn.cursor() as cur:
                # Create a shell row only when the 10-second AI row does not yet
                # exist. ai_analysis uses the current 17-group tendency until the
                # official 10-second lock overwrites it below.
                cur.execute("SELECT 1 FROM ai_predictions WHERE period_key=%s",(key,))
                exists=cur.fetchone() is not None
                if exists:
                    cur.execute("UPDATE ai_predictions SET conclusion17=COALESCE(conclusion17,%s), conclusion17_mode=COALESCE(conclusion17_mode,%s) WHERE period_key=%s",
                                (model['single'],model['mode'],key))
    finally: db_release(conn)
    return model


def calibrated_countdown_value():
    """Countdown on the same Beijing-4s clock used by current_period/UI."""
    now = datetime.now(CN_TZ) - timedelta(seconds=4)
    sec = now.second
    # Backend does not need the UI's brief 00 animation; second 0 is the new period.
    return 60 if sec == 0 else 60 - sec


def _persist_ai_payload(payload):
    if _target_block_observed(payload['target20']):
        raise RuntimeError('target block already observed; refuse late prediction')
    conn=db_connect()
    if conn is None:
        raise RuntimeError('database unavailable')
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_predictions(period_key,period_date,period_no,target_block,data_conclusion,ai_analysis,prediction_top3,sample_size,conclusion17,conclusion17_mode,model_version,locked_at,confidence,ensemble_detail,model_weights)
                    SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s::jsonb,%s::jsonb
                    WHERE NOT EXISTS (SELECT 1 FROM tron_blocks WHERE block_number=%s)
                    ON CONFLICT(period_key) DO UPDATE SET
                      data_conclusion=COALESCE(ai_predictions.data_conclusion,EXCLUDED.data_conclusion),
                      ai_analysis=COALESCE(ai_predictions.ai_analysis,EXCLUDED.ai_analysis),
                      prediction_top3=COALESCE(ai_predictions.prediction_top3,EXCLUDED.prediction_top3),
                      sample_size=GREATEST(COALESCE(ai_predictions.sample_size,0),EXCLUDED.sample_size),
                      conclusion17=COALESCE(ai_predictions.conclusion17,EXCLUDED.conclusion17),
                      conclusion17_mode=COALESCE(ai_predictions.conclusion17_mode,EXCLUDED.conclusion17_mode)
                """,(payload['key'],payload['date'],payload['period'],payload['target20'],
                     payload['dataConclusion'],payload['ai'],json.dumps(payload['top3']),
                     payload['sampleSize'],payload.get('c17'),payload.get('c17Mode'),MODEL_VERSION,
                         payload.get('confidence'),json.dumps(payload.get('ensemble') or {}),
                         json.dumps((payload.get('ensemble') or {}).get('weights') or {}),
                         int(payload['target20'])))
                if cur.rowcount == 0:
                    raise RuntimeError('target block already stored; refuse late prediction')
    finally:
        db_release(conn)
    return True


def flush_pending_ai_predictions():
    """Retry only while the target block has not been observed."""
    with _pending_ai_lock:
        items=list(_pending_ai_predictions.items())
    for key,payload in items:
        if _target_block_observed(payload['target20']):
            with _pending_ai_lock:
                _pending_ai_predictions.pop(key,None)
            record_system_event('ai_missed_deadline',key,{'targetBlock':payload['target20']})
            continue
        try:
            _persist_ai_payload(payload)
            with _pending_ai_lock:
                _pending_ai_predictions.pop(key,None)
            with _ai_summary_cache_lock:
                _ai_summary_cache.update({'key':None,'at':0,'value':None})
        except Exception:
            pass



def _norm_scores(scores):
    vals={int(k):max(0.0,float(v)) for k,v in (scores or {}).items() if str(k).isdigit() or isinstance(k,int)}
    total=sum(vals.values())
    if total <= 0:return {i:1/8 for i in range(8)}
    return {i:vals.get(i,0.0)/total for i in range(8)}

def _candidate_rank(score_map):
    return sorted(range(8), key=lambda i:(-float(score_map.get(i,0.0)), i))


def outside_candidate(score_map):
    """Highest scored category outside 3/4, reported separately from Top3."""
    if not isinstance(score_map,dict) or not score_map:return None
    try: scores={int(k):float(v) for k,v in score_map.items()}
    except (TypeError,ValueError): return None
    eligible=[i for i in (0,1,2,5,6,7) if i in scores]
    if not eligible:return None
    pick=min(eligible,key=lambda i:(-scores[i],i))
    return {'single':pick,'score':round(scores[pick],2)}


def research_model_performance(limit=500):
    """Score candidate models only on predictions that were locked before results.

    This is a rolling out-of-sample scoreboard: each stored row was created before
    its group-20 result, so later verification can be used without label leakage.
    Scores are shrunk toward neutral when the sample is small.
    """
    names=['long','short','structure17','transition','omission','hash_context','relation','binomial','hash_learned']
    perf={n:{'n':0,'top1':0,'top3':0,'pairedWins':0,'pairedLosses':0} for n in names}
    conn=db_connect()
    if conn is None: return perf
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT p.actual_single,p.ensemble_detail,p.period_date,p.period_no,p.target_block
                           FROM ai_predictions p
                           JOIN tron_blocks b ON b.block_number=p.target_block
                           WHERE p.actual_single IS NOT NULL AND p.actual_single=b.single_count
                             AND p.ensemble_detail IS NOT NULL
                             AND p.model_version LIKE 'v9.%%'
                             AND p.locked_at IS NOT NULL AND b.block_time IS NOT NULL
                             AND p.locked_at < b.block_time
                           ORDER BY p.period_date DESC,p.period_no DESC LIMIT %s""",(int(limit),))
            rows=cur.fetchall()
        for r in rows:
            if r.get('period_date') is not None and r.get('target_block') is not None:
                ds=r['period_date'].isoformat() if hasattr(r['period_date'],'isoformat') else str(r['period_date'])
                if int(r['target_block'])!=period_target_block(ds,int(r['period_no'])):
                    continue
            actual=int(r['actual_single']); detail=r.get('ensemble_detail') or {}
            if isinstance(detail,str):
                try: detail=json.loads(detail)
                except Exception: detail={}
            parts=detail.get('components') or {}
            baseline=parts.get('binomial')
            if not isinstance(baseline,dict): continue
            try: baseline_rank=_candidate_rank({int(k):float(v) for k,v in baseline.items()})
            except (TypeError,ValueError): continue
            baseline_hit=bool(baseline_rank and baseline_rank[0]==actual)
            for n in names:
                comp=parts.get(n)
                if not isinstance(comp,dict): continue
                try: sm={int(k):float(v) for k,v in comp.items()}
                except Exception: continue
                rank=_candidate_rank(sm)
                perf[n]['n']+=1
                perf[n]['top1']+=int(bool(rank and rank[0]==actual))
                perf[n]['top3']+=int(actual in rank[:3])
                if n != 'binomial':
                    hit=bool(rank and rank[0]==actual)
                    perf[n]['pairedWins']+=int(hit and not baseline_hit)
                    perf[n]['pairedLosses']+=int(baseline_hit and not hit)
    finally: db_release(conn)
    for n,v in perf.items():
        nn=v['n']; v['top1Rate']=round(v['top1']/nn*100,2) if nn else None
        v['top3Rate']=round(v['top3']/nn*100,2) if nn else None
        delta=(v['pairedWins']-v['pairedLosses'])/nn if nn else 0.0
        variance=max(0.0,(v['pairedWins']+v['pairedLosses'])/nn-delta*delta) if nn else 0.0
        v['pairedLift']=round(delta*100,2) if nn else None
        v['pairedSE']=round((variance/nn)**0.5*100,2) if nn else None
    return perf


def _hash_context_scores(groups):
    """Pre-result hash-context model using only current groups 1..17.

    This is a diagnostic feature model, not a claim that a cryptographic hash has
    a predictable formula. It summarizes A-E/digit composition, right-tail
    structure and the platform-derived single counts already available before G20.
    It earns production weight only through stored out-of-sample verification.
    """
    score={i:1.0 for i in range(8)}
    letter_odd=0; digit_odd=0; pairs=0
    for g in range(1,18):
        row=groups.get(str(g)) or {}
        h=str(row.get('block') or row.get('block_hash') or '').upper()
        if not h: continue
        rev=h[::-1]
        letters=[c for c in rev if c in 'ABCDE'][:7]
        digits=[c for c in rev if c.isdigit()][:7]
        for c in letters: letter_odd += (ord(c)-ord('A')) % 2
        for c in digits: digit_odd += int(c) % 2
        pairs += min(len(letters),len(digits))
        if row.get('singleCount') is not None:
            score[int(row['singleCount'])]+=1.5
    # Tiny composition signal; OOS weighting decides whether it deserves influence.
    if pairs:
        ratio=(letter_odd+digit_odd)/max(1,2*pairs)
        center=max(0,min(7,round(ratio*7)))
        score[center]+=0.75
        if center>0: score[center-1]+=0.25
        if center<7: score[center+1]+=0.25
    return _norm_scores(score)


def _relation_scores(groups, relation):
    score={i:1.0 for i in range(8)}
    for item in ((relation or {}).get('ranking') or [])[:6]:
        try: g=int(item.get('group') or 0); rate=float(item.get('matchRate') or 0)/100.0
        except Exception: continue
        row=groups.get(str(g)) or {}
        if row.get('singleCount') is not None:
            score[int(row['singleCount'])]+=max(0.0,rate)*3.0
    return _norm_scores(score)

def v9_research_ensemble(groups, historical, relation=None, hash_snapshot=None, performance=None):
    """V9 leakage-safe adaptive ensemble.

    `historical` is newest -> oldest. Candidate weights are learned only from
    already-verified, pre-result snapshots. A natural Binomial(7, .5) baseline
    is always included so a complex signal must earn weight against a simple
    reference instead of merely rediscovering 单3/单4 prevalence.
    """
    hist=[int(x) for x in (historical or []) if 0 <= int(x) <= 7]  # newest -> oldest
    recent=hist[:120]
    long=hist[:1000]
    long_s={i:1.0 for i in range(8)}; short_s={i:1.0 for i in range(8)}
    for x in long: long_s[x]+=1
    for x in recent: short_s[x]+=1
    long_s=_norm_scores(long_s); short_s=_norm_scores(short_s)

    struct={i:1.0 for i in range(8)}
    for g in range(1,18):
        row=groups.get(str(g))
        if row and row.get('singleCount') is not None: struct[int(row['singleCount'])]+=1
    struct=_norm_scores(struct)

    # Historical is newest -> oldest. hist[j] happened after hist[j+1].
    trans={i:1.0 for i in range(8)}
    if hist:
        latest=hist[0]
        for j in range(1,min(len(hist)-1,500)):
            if hist[j]==latest: trans[hist[j-1]]+=1
    trans=_norm_scores(trans)

    omit={i:1.0 for i in range(8)}
    for i in range(8):
        gap=0
        for x in hist:  # newest first: correct current omission
            if x==i: break
            gap+=1
        # Keep omission deliberately weak; it must prove itself in OOS scoring.
        omit[i]=1.0+min(gap,40)/80.0
    omit=_norm_scores(omit)

    binomial_raw=[1,7,21,35,35,21,7,1]
    binomial={i:binomial_raw[i]/128.0 for i in range(8)}
    hash_context=_hash_context_scores(groups)
    relation_s=_relation_scores(groups,relation)
    parts={'long':long_s,'short':short_s,'structure17':struct,'transition':trans,
           'omission':omit,'hash_context':hash_context,'relation':relation_s,'binomial':binomial}
    learned_scores=hash_research.snapshot_scores(hash_snapshot,groups,hist)
    if learned_scores is not None: parts['hash_learned']=learned_scores

    perf=performance if performance is not None else research_model_performance(500)
    # Explicitly exploratory prior: use current, pre-result information even
    # before enough verified periods exist. These are fixed design weights,
    # NOT evidence that any component can predict future hashes.
    prior={'binomial':0.25,'structure17':0.20,'relation':0.15,'short':0.12,
           'long':0.08,'transition':0.08,'omission':0.06,'hash_context':0.06}
    # Adjust only using predictions frozen before the compared result. Requiring
    # 200 paired observations and 2.58 SE limits the chance that selecting from
    # several models rewards a noisy short-run winner. Negative evidence shrinks
    # a model that previously retained its full exploratory weight indefinitely.
    raww=dict(prior)
    if learned_scores is not None:raww['hash_learned']=0.35
    weight_decisions={n:'exploratory' for n in raww}
    for n in parts:
        if n=='binomial': continue
        st=perf.get(n,{})
        nn=int(st.get('n') or 0)
        lift=float(st.get('pairedLift') or 0)
        se=float(st.get('pairedSE') or 0)
        if nn>=200 and lift>2.58*se and lift>=2.0:
            raww[n]+=min(0.80,(lift-2.58*se)/100.0*6.0)
            weight_decisions[n]='validated_gain'
        elif nn>=200 and lift< -2.58*se:
            raww[n]=max(0.005,raww[n]*0.10)
            weight_decisions[n]='validated_loss'
    sw=sum(raww.values()) or 1.0
    weights={n:raww[n]/sw for n in raww}
    score={i:sum(weights[n]*parts[n][i] for n in weights) for i in range(8)}
    order=_candidate_rank(score)

    # Descriptive paired evidence only; selecting the best of several candidates
    # still makes these labels exploratory rather than a predictive guarantee.
    complex_names=['long','short','structure17','transition','omission','hash_context','relation','hash_learned']
    tested=[perf[n] for n in complex_names if perf.get(n,{}).get('n',0)>=100]
    baseline_perf=perf.get('binomial',{})
    best_complex=max((x.get('top1Rate') or 0 for x in tested),default=0)
    base_rate=baseline_perf.get('top1Rate') or 0
    edge='NO_EDGE'
    if any((x.get('pairedLift') or 0)>1.64*(x.get('pairedSE') or 0) for x in tested): edge='WEAK_EDGE'
    if any(x['n']>=200 and (x.get('pairedLift') or 0)>2.58*(x.get('pairedSE') or 0) for x in tested): edge='EVIDENCE'
    sep=max(0.0,score[order[0]]-score[order[1]])
    confidence=max(0.0,min(100.0,sep*800.0))
    return {'single':order[0],'top3':order[:3],
            'scores':{str(i):round(score[i]*100,3) for i in range(8)},
            'confidence':round(confidence,1),'weights':weights,'components':parts,
            'research':{'mode':'hash_history_with_paired_oos','window':500,'performance':perf,
                        'edgeStatus':edge,'bestComplexTop1':round(best_complex,2),
                        'baselineTop1':round(base_rate,2),'weightMode':'exploratory_prior_plus_strict_evidence',
                        'weightDecisions':weight_decisions,
                        'hashModel':{k:hash_snapshot.get(k) for k in ('status','sample','testSample','trainedThrough','baselineLoss','modelLoss','active','selectedFamily')}
                                    if hash_snapshot else None}}

def save_prediction_if_ready(date_str, period, groups, target20, ui_countdown=None, force_backend=False):
    """Lock a pre-result prediction.

    V8.1.2 fast path: when the autonomous worker owns the lock we deliberately
    avoid the older relation/legacy-analysis queries before persisting.  Those
    queries were wasted because V7 ensemble replaced their answer afterwards,
    and on a ~9 second G17->G20 window they could make a healthy worker miss the
    lock entirely.  The durable snapshot is now created from exactly groups
    1..17 plus history and is placed in the retry queue before the DB write.
    """
    if RESEARCH_ONLY_MODE:return None
    countdown=int(ui_countdown) if ui_countdown is not None else None
    if not force_backend and countdown != 10:
        return None
    if groups.get('20'):
        return None
    # Formal autonomous locks require the complete strict 17-group input.
    if force_backend and not all(str(i) in groups for i in range(1,18)):
        return None
    if _target_block_observed(target20):
        return None
    stats=stats_from_groups(groups)
    if int(stats.get('sampleSize') or 0) < 1:
        return None
    key=f'{date_str}:{int(period):04d}'
    # Idempotency: never recompute an already durable pre-result lock.
    existing=prediction_exists(key)
    if existing is not None:
        return {'single':None,'scores':{},'historicalSample':0,'frozen':True,
                'period':int(period),'lockCountdown':countdown,'lockSource':'existing'}

    with _prediction_context_lock:
        context=(_prediction_context['value'] if _prediction_context['key']==key else None)
    if context is None:
        # Never do several historical PostgreSQL scans within the ~9s G17 window.
        # The context worker will finish and the live worker will retry.
        return None
    historical=context['historical']
    c17=ai_conclusion17_from_data(groups,historical,date_str,period,target20)

    # The production prediction is the V9 rolling out-of-sample research ensemble.  It uses only data
    # available before group20.  Do this before any nonessential analytics.
    v7=v9_research_ensemble(groups,historical,context['relation'],
                            context['hashSnapshot'],performance=context['performance'])
    ai=int(v7['single'])
    top3=[int(x) for x in v7['top3']]
    highest=stats.get('highest') or []
    dc=int(highest[0]['single']) if len(highest)==1 else None
    payload={'key':key,'date':date_str,'period':int(period),'target20':int(target20),
             'dataConclusion':dc,'ai':ai,'top3':top3,
             'confidence':v7.get('confidence'),'ensemble':v7,
             'sampleSize':int(stats.get('sampleSize') or 0),
             'c17':c17.get('single') if c17 else None,
             'c17Mode':c17.get('mode') if c17 else None}

    # Durability boundary: snapshot first, DB second.  If PostgreSQL is briefly
    # unavailable the pending queue keeps retrying while group20 is still absent.
    with _pending_ai_lock:
        _pending_ai_predictions.setdefault(key,payload)
    try:
        _persist_ai_payload(payload)
        with _pending_ai_lock:
            _pending_ai_predictions.pop(key,None)
        record_system_event('ai_locked_pre_result',key,{'source':'backend' if force_backend else 'ui','top3':top3})
    except Exception as exc:
        with _runtime_health_lock:
            _runtime_health['lastAiError']=str(exc)[:300]
        record_system_event('ai_persist_retry',key,{'error':str(exc)[:300]})
    with _ai_summary_cache_lock:
        _ai_summary_cache.update({'key':None,'at':0,'value':None})
    return {'single':ai,'scores':v7.get('scores') or {},'historicalSample':len(historical),'frozen':True,
            'period':int(period),'lockCountdown':countdown,
            'lockSource':'backend' if force_backend else 'ui'}


def verify_prediction(date_str, period, official):
    if not official:return False
    key=f'{date_str}:{int(period):04d}'
    conn=db_connect()
    if conn is None:return False
    updated=False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""UPDATE ai_predictions SET actual_single=%s,verified_at=COALESCE(verified_at,NOW())
                               WHERE period_key=%s AND target_block=%s AND actual_single IS NULL""",
                            (int(official['singleCount']),key,int(official['blockNumber'])))
                if cur.rowcount > 0:
                    updated=True
                else:
                    cur.execute("SELECT actual_single FROM ai_predictions WHERE period_key=%s AND target_block=%s",
                                (key,int(official['blockNumber'])))
                    row=cur.fetchone()
                    updated=bool(row and row[0] is not None)
    finally:
        db_release(conn)
    return updated


def historical_prediction_verified(prediction, block, target):
    """A history hit requires the same target, real result and pre-result lock."""
    if not prediction or not block:return False
    try:
        locked=prediction.get('locked_at'); published=block.get('block_time')
        return (int(prediction['target_block'])==int(target)
                and locked is not None and published is not None and locked < published
                and prediction.get('actual_single') is not None
                and int(prediction['actual_single'])==int(block['single_count']))
    except (TypeError,ValueError,KeyError):
        return False


def ai_audit_summary(rows, snapshot=None, current_version=None):
    """Audit locked predictions against chain rows without rewriting history.

    The platform's own result is independent evidence; matching our expected
    target only proves internal consistency, not platform alignment.
    """
    reasons={'targetMismatch':0,'blockMissing':0,'notLockedBeforeBlock':0,
             'notVerified':0,'actualMismatch':0}
    samples=[]; issues=[]; late_seconds=[]; recent_rows=[]; version_counts={}
    for r in rows:
        ds=r['period_date'].isoformat() if hasattr(r['period_date'],'isoformat') else str(r['period_date'])
        p=int(r['period_no']); key=str(r.get('period_key') or f'{ds}:{p:04d}')
        version=str(r.get('model_version') or '未标记版本')
        bucket=version_counts.setdefault(version,{'saved':0,'verified':0,'late':0,'otherInvalid':0})
        bucket['saved']+=1
        expected=period_target_block(ds,p)
        reason=None
        if int(r['target_block'])!=expected:reason='targetMismatch'
        elif r.get('block_number') is None:reason='blockMissing'
        elif not r.get('locked_at') or not r.get('block_time') or r['locked_at']>=r['block_time']:
            reason='notLockedBeforeBlock'
        elif r.get('actual_single') is None:reason='notVerified'
        elif int(r['actual_single'])!=int(r['chain_single']):reason='actualMismatch'
        if reason:
            reasons[reason]+=1
            bucket['late' if reason=='notLockedBeforeBlock' else 'otherInvalid']+=1
            if reason=='notLockedBeforeBlock' and r.get('locked_at') and r.get('block_time'):
                late_seconds.append((r['locked_at']-r['block_time']).total_seconds())
            if len(issues)<12:
                issues.append({'period':key,'reason':reason,'savedTarget':int(r['target_block']),
                               'expectedTarget':expected})
        else:
            bucket['verified']+=1
        if len(recent_rows)<12:
            locked=r.get('locked_at'); published=r.get('block_time')
            recent_rows.append({'period':key,'version':version,'status':reason or 'verified',
                'deltaSeconds':round((locked-published).total_seconds(),2) if locked and published else None})
        if reason:continue
        top=r.get('prediction_top3') or []
        if isinstance(top,str):
            try:top=json.loads(top)
            except (TypeError,ValueError):top=[]
        try:top=[int(x) for x in top][:3]
        except (TypeError,ValueError):top=[]
        actual=int(r['chain_single'])
        samples.append({'period':key,'targetBlock':expected,'top1':int(r['ai_analysis']),
                        'top3':top,'actual':actual,'modelVersion':r.get('model_version')})
    windows={}
    for n in (20,100,500,1000):
        recent=samples[:n]
        size=len(recent)
        top1=sum(x['top1']==x['actual'] for x in recent)
        top3=sum(x['actual'] in x['top3'] for x in recent)
        baseline=sum(x['actual']==3 for x in recent)
        windows[str(n)]={'verified':size,'top1Hits':top1,'top3Hits':top3,
                         'baseline3Hits':baseline,
                         'top1Rate':round(top1/size*100,2) if size else None,
                         'top3Rate':round(top3/size*100,2) if size else None}
    actual_counts={str(i):sum(x['actual']==i for x in samples) for i in range(8)}
    predicted_counts={str(i):sum(x['top1']==i for x in samples) for i in range(8)}
    versions={}
    for x in samples:
        name=str(x['modelVersion'] or '未标记版本')
        bucket=versions.setdefault(name,{'verified':0,'top1Hits':0})
        bucket['verified']+=1
        bucket['top1Hits']+=int(x['top1']==x['actual'])
    snap=snapshot if isinstance(snapshot,dict) else {}
    late_seconds.sort()
    return {'predictionRows':len(rows),'validSamples':len(samples),
            'currentVersionStats':version_counts.get(current_version,{'saved':0,'verified':0,'late':0,'otherInvalid':0}),
            'recentRows':recent_rows,
            'lateLockSeconds':{'count':len(late_seconds),
                'median':round(late_seconds[len(late_seconds)//2],2) if late_seconds else None,
                'min':round(late_seconds[0],2) if late_seconds else None,
                'max':round(late_seconds[-1],2) if late_seconds else None},
            'actualCounts':actual_counts,'predictedCounts':predicted_counts,
            'modelVersions':versions,
            'latestSavedVersion':str(rows[0].get('model_version') or '未标记版本') if rows else None,
            'invalidReasons':reasons,'windows':windows,'examples':issues,
            'hashModel':{k:snap.get(k) for k in ('status','sample','trainSample','tuneSample',
            'testSample','active','alpha','baselineLoss','modelLoss','baselineTop1','modelTop1','trainedThrough',
            'selectedFamily','featureFamilies','holdoutHalves')},
            'platformAlignment':'未核对平台原始开奖；本诊断仅核对区块与已存预测'}


def prediction_summary(date_str, period, groups, target20, official, ui_countdown=None):
    key=f'{date_str}:{int(period):04d}'
    now=time.time()
    # Explicit UI=10 lock must be handled BEFORE any summary-cache return.
    # Otherwise a request arriving within the cache TTL can silently miss the only lock event.
    # V7.1: verification is backend-worker responsibility. Never block /api/draw on DB verification.
    if not official:
        save_prediction_if_ready(date_str,period,groups,target20,ui_countdown)
    with _ai_summary_cache_lock:
        cached=_ai_summary_cache.get('value') if _ai_summary_cache.get('key')==key else None
        cached_at=_ai_summary_cache.get('at',0)
    if cached is not None and not official and ui_countdown != 10 and now-cached_at < 1.0:
        return dict(cached)
    # V5 second guarantee: summary itself self-heals a missing prediction row.
    # A current-period 17/17 snapshot is enough; never create after group20.
    if not official and all(str(i) in groups for i in range(1,18)):
        try:
            save_prediction_if_ready(date_str, period, groups, target20, force_backend=True)
            flush_pending_ai_predictions()
        except Exception:
            pass
    conn=db_connect(); row=None; agg=None; latest_verified=None; verified_rows=[]
    if conn is not None:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM ai_predictions WHERE period_key=%s",(key,)); row=cur.fetchone()
                cur.execute("""SELECT p.ai_analysis,p.prediction_top3,p.actual_single,p.ensemble_detail
                    FROM ai_predictions p JOIN tron_blocks b ON b.block_number=p.target_block
                    WHERE p.actual_single IS NOT NULL AND p.actual_single=b.single_count
                      AND p.model_version LIKE 'v9.%%'
                      AND p.locked_at IS NOT NULL AND b.block_time IS NOT NULL
                      AND p.locked_at < b.block_time
                    ORDER BY p.period_date DESC,p.period_no DESC LIMIT 500""")
                verified_rows=cur.fetchall()
                cur.execute("""SELECT COUNT(*) FILTER (WHERE actual_single IS NOT NULL) verified,
                    COUNT(*) FILTER (WHERE actual_single IS NOT NULL AND ai_analysis=actual_single) ai_hits,
                    COUNT(*) FILTER (WHERE actual_single IS NOT NULL AND prediction_top3 @> to_jsonb(ARRAY[actual_single])) top3_hits,
                    COUNT(*) FILTER (WHERE actual_single IS NOT NULL AND data_conclusion IS NOT NULL) data_verified,
                    COUNT(*) FILTER (WHERE actual_single IS NOT NULL AND data_conclusion=actual_single) data_hits
                    FROM ai_predictions p JOIN tron_blocks b ON b.block_number=p.target_block
                    WHERE p.model_version LIKE 'v9.%%' AND p.locked_at IS NOT NULL
                      AND p.actual_single=b.single_count
                      AND b.block_time IS NOT NULL AND p.locked_at < b.block_time"""); agg=cur.fetchone()
                cur.execute("""SELECT p.period_no,p.prediction_top3,p.actual_single
                    FROM ai_predictions p JOIN tron_blocks b ON b.block_number=p.target_block
                    WHERE p.actual_single IS NOT NULL AND p.actual_single=b.single_count
                      AND p.prediction_top3 IS NOT NULL
                      AND p.model_version LIKE 'v9.%%' AND p.locked_at IS NOT NULL
                      AND b.block_time IS NOT NULL AND p.locked_at < b.block_time
                    ORDER BY p.verified_at DESC NULLS LAST,p.period_date DESC,p.period_no DESC LIMIT 1""")
                latest_verified=cur.fetchone()
        finally: db_release(conn)
    historical=get_historical_official_singles(date_str,period)
    relation=group20_relation_model(date_str, period)
    live_ai,scores=ai_analysis_from_data(groups,historical,relation)
    pending=None
    with _pending_ai_lock:
        pending=_pending_ai_predictions.get(key)
    ai_single=int(row['ai_analysis']) if row else (int(pending['ai']) if pending else live_ai)
    frozen_top3=[]
    if row and row.get('prediction_top3') is not None:
        frozen_top3=row.get('prediction_top3') or []
        if isinstance(frozen_top3,str):
            try: frozen_top3=json.loads(frozen_top3)
            except Exception: frozen_top3=[]
        frozen_top3=[int(x) for x in frozen_top3][:3]
    live_top3=sorted(range(8), key=lambda i:(-float(scores.get(str(i),0)),i))[:3] if scores else []
    pending_top3=[int(x) for x in pending.get('top3',[])][:3] if pending else []
    display_top3=frozen_top3 if frozen_top3 else (pending_top3 if pending_top3 else live_top3)
    verified=int(agg['verified'] or 0) if agg else 0; hits=int(agg['ai_hits'] or 0) if agg else 0
    dv=int(agg['data_verified'] or 0) if agg else 0; dh=int(agg['data_hits'] or 0) if agg else 0
    # Current-period values for the three historically highest-related positions.
    # Strictly read from this period's already collected groups; never backfill from another period.
    relation_top3=[]
    for item in (relation.get('ranking') or [])[:3]:
        x=dict(item)
        gno=int(x.get('group') or 0)
        grow=groups.get(str(gno)) if isinstance(groups, dict) else None
        x['currentSingle'] = int(grow.get('singleCount')) if grow and grow.get('singleCount') is not None else None
        relation_top3.append(x)
    top3_hits=int(agg.get('top3_hits') or 0) if agg else 0
    latest_result=None
    if latest_verified:
        pred=latest_verified.get('prediction_top3') or []
        if isinstance(pred, str):
            try: pred=json.loads(pred)
            except Exception: pred=[]
        pred=[int(x) for x in pred][:3]
        actual=int(latest_verified['actual_single'])
        hit_position=(pred.index(actual)+1) if actual in pred else None
        latest_result={'period':int(latest_verified['period_no']), 'top3':pred, 'actual':actual, 'hit':bool(hit_position), 'hitPosition':hit_position}
    # V9.1 decision dashboard: only verified, pre-result snapshots count.
    windows={20:{'n':0,'hit':0,'top3':0},100:{'n':0,'hit':0,'top3':0},500:{'n':0,'hit':0,'top3':0}}
    for idx,vrw in enumerate(verified_rows or []):
        try:
            actual=int(vrw['actual_single']); pred=int(vrw['ai_analysis']); tp=vrw.get('prediction_top3') or []
            if isinstance(tp,str): tp=json.loads(tp)
            tp=[int(x) for x in tp]
        except Exception: continue
        for w in windows:
            if idx < w:
                windows[w]['n']+=1; windows[w]['hit']+=int(pred==actual); windows[w]['top3']+=int(actual in tp[:3])
    for w,v in windows.items():
        v['top1Rate']=round(v['hit']/v['n']*100,2) if v['n'] else None
        v['top3Rate']=round(v['top3']/v['n']*100,2) if v['n'] else None
    ensemble_detail={}
    if row and row.get('ensemble_detail') is not None:
        ensemble_detail=row.get('ensemble_detail') or {}
        if isinstance(ensemble_detail,str):
            try: ensemble_detail=json.loads(ensemble_detail)
            except Exception: ensemble_detail={}
    elif pending: ensemble_detail=pending.get('ensemble') or {}
    if not ensemble_detail and all(str(i) in groups for i in range(1,18)):
        try: ensemble_detail=v9_research_ensemble(groups,historical,relation,get_hash_model_snapshot(key))
        except Exception: ensemble_detail={}
    component_view=[]
    label_map={'long':'长期分布','short':'短期趋势','structure17':'17组结构','transition':'状态转移','omission':'遗漏条件','hash_context':'哈希结构','hash_learned':'历史哈希模型','relation':'位置关联','binomial':'基础分布'}
    comps=ensemble_detail.get('components') or {}; weights=ensemble_detail.get('weights') or {}; perf=(ensemble_detail.get('research') or {}).get('performance') or {}
    for name in ['hash_learned','structure17','hash_context','relation','short','long','transition','omission','binomial']:
        if name=='hash_learned' and name not in comps:continue
        sm=comps.get(name) or {}
        try: rank=_candidate_rank({int(k):float(v) for k,v in sm.items()})
        except Exception: rank=[]
        st=perf.get(name) or {}
        component_view.append({'key':name,'label':label_map[name],'single':rank[0] if rank else None,
                               'weight':round(float(weights.get(name,0))*100,1),'sample':int(st.get('n') or 0),
                               'top1Rate':st.get('top1Rate'),'top3Rate':st.get('top3Rate')})
    research=ensemble_detail.get('research') or {}
    if (row or pending) and isinstance(ensemble_detail.get('scores'),dict):
        scores=ensemble_detail['scores']
    decision={'edgeStatus':research.get('edgeStatus','NO_EDGE'),'confidence':ensemble_detail.get('confidence'),
              'components':component_view,'windows':{str(k):v for k,v in windows.items()},
              'weightDecisions':research.get('weightDecisions') or {},
              'outsideCandidate':outside_candidate(ensemble_detail.get('scores')) if (row or pending) else None,
              'hashModel':research.get('hashModel'),
              'baselineTop1':research.get('baselineTop1'),'bestComplexTop1':research.get('bestComplexTop1'),
              'note':'综合排序含固定的探索性权重；Top3百分比是相对模型分数，并非实际命中概率。经同批样本验证优于基础分布的模型才获得额外权重；仅统计开奖前锁定记录。'}
    result={'single':ai_single,'scores':scores,'historicalSample':len(historical),
            'frozen':bool(row or pending),'period':int(row['period_no']) if row else int(period),
            'top3':display_top3,'verifiedSample':verified,'hits':hits,
            'top3Hits':top3_hits,'top3HitRate':round(top3_hits/verified*100,2) if verified else None,'latestVerified':latest_result,
            'hitRate':round(hits/verified*100,2) if verified else None,
            'dataVerifiedSample':dv,'dataHits':dh,'dataHitRate':round(dh/dv*100,2) if dv else None,
            'relationTop3':relation_top3,'relationSample':relation.get('samplePeriods',0),'decision':decision}
    with _ai_summary_cache_lock:
        _ai_summary_cache.update({'key':key,'at':time.time(),'value':dict(result)})
    return result

def read_state():
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text(encoding='utf-8'))
    except Exception:
        pass
    return None


def write_state(data):
    tmp = STATE_FILE.with_suffix('.tmp')
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding='utf-8')
    tmp.replace(STATE_FILE)


def update_result_history(state, official):
    """Keep the latest 20 confirmed platform results for the small history panel."""
    history = state.get('resultHistory') if isinstance(state.get('resultHistory'), list) else []
    key = str(official.get('platformPeriod') or '')
    if not key:
        key = f"{state.get('date','')}{state.get('period','')}"
    item = {
        'platformPeriod': key,
        'period': str(official.get('period') or state.get('period') or ''),
        'blockNumber': official.get('blockNumber'),
        'singleCount': official.get('singleCount'),
    }
    history = [x for x in history if str(x.get('platformPeriod','')) != key]
    history.append(item)
    history.sort(key=lambda x: str(x.get('platformPeriod','')))
    state['resultHistory'] = history[-20:]
    return state['resultHistory']


def update_omission(state, official):
    omission = state.get('omission') if isinstance(state.get('omission'), dict) else {}
    omission = {str(i): int(omission.get(str(i), 0) or 0) for i in range(8)}
    key = f"{state.get('date','')}:{state.get('period','')}"
    if state.get('lastOmissionPeriod') == key:
        return omission
    single = official.get('singleCount')
    if single is None or not 0 <= int(single) <= 7:
        return omission
    for i in range(8):
        omission[str(i)] = 0 if i == int(single) else omission[str(i)] + 1
    state['lastOmissionPeriod'] = key
    return omission


def target_block_number(date_str, period, state, latest_number):
    """Map a platform period to its confirmed group-20/result block.

    Uses the confirmed anchors and the estimated 360-period tail schedule.
    """
    try:
        return period_target_block(date_str, int(period))
    except Exception:
        # Last-resort fallback for an uncalibrated date/range.
        return int(latest_number) - ((int(latest_number) - 6) % 20)


@app.get('/')
def index():
    
    resp = Response((BASE_DIR / 'index.html').read_text(encoding='utf-8'), mimetype='text/html; charset=utf-8')
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


@app.get('/style.css')
def style():
    return Response((BASE_DIR / 'style.css').read_text(encoding='utf-8'), mimetype='text/css; charset=utf-8')


@app.get('/api/draw')
def draw():
    """Serve live data from PostgreSQL, with last-good-memory fallback.

    A transient DB timeout must not blank the frontend. The last successful
    payload stays available while the backend reconnects automatically.
    """
    global _draw_cache
    date_str, period, period_str, platform_period = current_period()
    state = read_state() or {}
    try:
        if not _omission_bootstrapped:
            try:
                restore_omission_from_db(date_str, period)
                state = read_state() or state
            except Exception:
                pass
        if not DATABASE_URL:
            raise RuntimeError('DATABASE_URL 未配置')
        groups, target20 = collect_groups_live(date_str, period)
        official = groups.get('20')
        if not RESEARCH_ONLY_MODE:
            try:
                persist_period_groups(date_str,period,groups,target20)
            except Exception as exc:
                with _runtime_health_lock:_runtime_health['lastDbError']=str(exc)[:300]
        if official:
            prior = dict(state)
            state = {
                **prior, 'date': date_str, 'period': period_str,
                'platformPeriod': platform_period, 'block': official['block'],
                'blockNumber': official['blockNumber'], 'numbers': official['numbers'],
                'singleCount': official['singleCount'], 'groups': groups,
                'groupPeriodKey': f'{date_str}:{period_str}', 'source': 'live memory + PostgreSQL persistence'
            }
            state['omission'] = update_omission(state, official)
            write_state(state)
        result_obj = official
        if result_obj is None and isinstance(state.get('numbers'), list) and len(state.get('numbers')) == 7:
            result_obj = {'numbers': state['numbers'], 'singleCount': state.get('singleCount'), 'blockNumber': state.get('blockNumber'), 'block': state.get('block'), 'platformPeriod': state.get('platformPeriod')}
        if result_obj is None:
            try: result_obj=previous_official_result(date_str,period)
            except Exception: result_obj=None
        if result_obj and isinstance(result_obj.get('numbers'),list) and len(result_obj['numbers'])==7:
            result_obj['singleCount']=calc_single_count(result_obj['numbers'])
        durable_omission = None
        try: durable_omission = get_omission_runtime()
        except Exception: durable_omission = None
        if durable_omission and isinstance(durable_omission.get('omission'),dict):
            omission = durable_omission['omission']
            state['omission'] = omission
            state['lastOmissionPeriod'] = durable_omission.get('last_period_key')
            state['omissionSource'] = durable_omission.get('source') or 'independent omission engine'
            state['omissionHistorySample'] = int(durable_omission.get('processed_count') or 0)
        else:
            omission = state.get('omission') if isinstance(state.get('omission'), dict) else {}
            omission = {str(i): int(omission.get(str(i), 0) or 0) for i in range(8)}
        history = build_recent_official_history(date_str, period, 20)
        if RESEARCH_ONLY_MODE:
            ai_info={'period':int(period),'frozen':False,'single':None,
                     'scores':{},'top3':[],'researchOnly':True}
            ai17=None
        else:
            ui_countdown = request.args.get('ai_lock_countdown', type=int)
            ai_info = prediction_summary(date_str, period, groups, target20, official, ui_countdown)
            historical_for_17 = get_historical_official_singles(date_str, period)
            ai17 = ai_conclusion17_from_data(groups, historical_for_17, date_str, period, target20)
            if ai17 and not official:
                try:save_prediction_if_ready(date_str, period, groups, target20, force_backend=True)
                except Exception:pass
            if ai17:
                ai17 = {**ai17, 'period': period_str, 'platformPeriod': platform_period,
                        'periodKey': f'{date_str}:{period_str}', 'sourceGroups': list(range(1,18)),
                        'sourceBlocks': [int(groups[str(i)]['blockNumber']) for i in range(1,18)]}
        payload = {
            **state, 'platformPeriod': platform_period, 'currentPeriod': period_str, 'groupPeriodKey': f'{date_str}:{period_str}',
            'targetResultBlock': target20, 'tailSchedule': tail_schedule(date_str, period), 'officialReady': bool(official),
            'resultNumbers': result_obj.get('numbers', []) if result_obj else [],
            'resultSingleCount': result_obj.get('singleCount') if result_obj else None,
            'resultBlockNumber': result_obj.get('blockNumber') if result_obj else None,
            'resultPlatformPeriod': result_obj.get('platformPeriod') if result_obj and result_obj.get('platformPeriod') else state.get('platformPeriod'),
            'result': result_obj, 'omission': omission, 'omissionSource': state.get('omissionSource', 'realtime state'), 'omissionHistorySample': state.get('omissionHistorySample', 0), 'resultHistory': history,
            'aiAnalysis': ai_info, 'aiConclusion17': ai17,
            'groups': sorted(groups.values(), key=lambda x: x.get('group', 0)),
            'dataStats': stats_from_groups(groups), 'ok': True, 'isNew': bool(official),
            'databaseStatus': 'connected', 'stale': False,
            'storage': {'database': True, 'retentionDays': DB_RETENTION_DAYS, 'frontendSource': 'PostgreSQL + memory fallback'},
            'debug': {'targetBlock': target20, 'fastTarget': dict(_fast_diag),
                      'aiPending': len(_pending_ai_predictions),
                      'ai17Ready': bool(ai17),
                      'aiFrozen': bool(ai_info.get('frozen')) if isinstance(ai_info, dict) else False,
                      'modelVersion': MODEL_VERSION, 'smartDb': dict(_runtime_health), 'aiEngine': 'multi-model-ensemble', 'resultFastPath': True, 'serverOwnedLifecycle': True}
        }
        with _draw_cache_lock:
            _draw_cache = dict(payload)
        return jsonify(payload)
    except Exception as exc:
        with _draw_cache_lock:
            cached = dict(_draw_cache) if isinstance(_draw_cache, dict) else None
        if cached:
            cached_period = str(cached.get('currentPeriod') or '').zfill(4)
            crossed_period = bool(cached_period and cached_period != period_str)
            cached.update({
                'ok': True, 'databaseStatus': 'reconnecting', 'stale': True,
                'databaseError': type(exc).__name__,
                'currentPeriod': period_str, 'platformPeriod': platform_period
            })
            if crossed_period:
                # Keep the last confirmed official result visible, but never
                # masquerade prior-period live groups/statistics/AI as the new period.
                cached['officialReady'] = False
                cached['groupPeriodKey'] = f'{date_str}:{period_str}'
                cached['groups'] = []
                cached['dataStats'] = stats_from_groups({})
                cached['aiConclusion17'] = None
                cached['aiAnalysis'] = {
                    'period': period_str, 'frozen': False, 'single': None,
                    'scores': {}, 'top3': [], 'relationTop3': []
                }
            return jsonify(cached)
        # First boot with no in-memory payload: fall back to the last persisted
        # state file rather than returning a 502 page to the browser.
        if state:
            groups_obj = state.get('groups') if isinstance(state.get('groups'), dict) else {}
            payload = {
                **state, 'ok': True, 'databaseStatus': 'reconnecting', 'stale': True,
                'databaseError': type(exc).__name__, 'currentPeriod': period_str,
                'platformPeriod': platform_period,
                'groups': sorted(groups_obj.values(), key=lambda x: x.get('group', 0)),
                'dataStats': stats_from_groups(groups_obj)
            }
            return jsonify(payload)
        return jsonify({'ok': False, 'databaseStatus': 'reconnecting', 'error': '数据库暂时不可用，正在自动重连'}), 503


@app.get('/api/result-fast')
def result_fast():
    ds,p,ps,platform=current_period()
    target=period_target_block(ds,p)
    row=None
    with _live_blocks_lock:
        row=_live_blocks.get(int(target))
    if row is None:
        try:
            dbrows=get_db_blocks([target]); row=dbrows.get(target)
        except Exception:
            row=None
    if not row:
        previous=None
        if request.args.get('previous','1')!='0':
            try: previous=previous_official_result(ds,p)
            except Exception: previous=None
        if previous:
            return jsonify({'ready':True,'period':str(previous['platformPeriod'])[-4:],
                            'platformPeriod':previous['platformPeriod'],
                            'targetBlock':previous['blockNumber'],
                            'numbers':previous['numbers'],
                            'single':previous['singleCount'],
                            'previousResult':True})
        return jsonify({'ready':False,'period':ps,'platformPeriod':platform,'targetBlock':target})
    nums=row.get('numbers')
    if isinstance(nums,str):
        try: nums=json.loads(nums)
        except Exception: nums=[]
    if not isinstance(nums,list) or len(nums)!=7:
        return jsonify({'ready':False,'period':ps,'platformPeriod':platform,'targetBlock':target})
    single=calc_single_count(nums)
    return jsonify({'ready':True,'period':ps,'platformPeriod':platform,'targetBlock':target,
                    'blockHash':row.get('block',row.get('block_hash')),
                    'numbers':nums,'single':single,'singleText':f'单{single}',
                    'blockTime':row['block_time'].isoformat() if row.get('block_time') and hasattr(row['block_time'],'isoformat') else row.get('timestamp')})


@app.get('/api/system-health')
def system_health():
    ds,p,ps,platform=current_period()
    conn=None; counts={'periodGroups':0,'predictions':0,'events':0,'runtimePeriods':0,'omissionProcessed':0}
    try:
        conn=db_connect()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS n FROM period_groups"); counts['periodGroups']=int(cur.fetchone()['n'])
            cur.execute("SELECT COUNT(*) AS n FROM ai_predictions"); counts['predictions']=int(cur.fetchone()['n'])
            cur.execute("SELECT COUNT(*) AS n FROM system_events"); counts['events']=int(cur.fetchone()['n'])
            cur.execute("SELECT COUNT(*) AS n FROM period_runtime"); counts['runtimePeriods']=int(cur.fetchone()['n'])
            cur.execute("SELECT processed_count FROM omission_runtime WHERE singleton=1"); _or=cur.fetchone(); counts['omissionProcessed']=int(_or['processed_count']) if _or else 0
            cur.execute("SELECT state,groups_seen,updated_at,last_error,prediction_locked_at,result_ready_at,verified_at,g17_ready_at,lock_attempts,lifecycle_trace FROM period_runtime WHERE period_key=%s",(f'{ds}:{int(p):04d}',))
            pr=cur.fetchone()
            cur.execute("SELECT service_name,service_role,instance_id,model_version,current_period,current_period_key,status,detail,started_at,updated_at FROM service_heartbeats ORDER BY updated_at DESC")
            services=[dict(r) for r in cur.fetchall()]
        now=datetime.now(timezone.utc)
        service_status={}
        for row in services:
            stamp=row.get('updated_at')
            age=999999.0
            if stamp is not None:
                try:
                    if stamp.tzinfo is None: stamp=stamp.replace(tzinfo=timezone.utc)
                    age=max(0.0,(now-stamp.astimezone(timezone.utc)).total_seconds())
                except Exception: pass
            detail=row.get('detail') or {}
            service_status[row['service_name']]={
                'role':row.get('service_role'),'instanceId':row.get('instance_id'),
                'modelVersion':row.get('model_version'),'currentPeriod':row.get('current_period'),
                'currentPeriodKey':row.get('current_period_key'),'status':row.get('status'),
                'lastHeartbeat':stamp.isoformat() if stamp else None,'ageSeconds':round(age,1),
                'healthy':age < 20.0,'detail':detail
            }
        runtime=dict(_runtime_health)
        try:
            _os=get_omission_runtime()
            if _os:
                _stamp=_os.get('updated_at')
                runtime['omissionEngine']={'omission':_os.get('omission'),'lastPeriodKey':_os.get('last_period_key'),
                    'processedCount':_os.get('processed_count'),'lastSingle':_os.get('last_single'),'source':_os.get('source'),
                    'lastError':_os.get('last_error'),'updatedAt':_stamp.isoformat() if hasattr(_stamp,'isoformat') else str(_stamp)}
        except Exception as _oe: runtime['omissionEngine']={'lastError':str(_oe)[:200]}
        runtime['services']=service_status
        runtime['workerOnline']=any(v.get('role')=='worker' and v.get('healthy') for v in service_status.values())
        runtime['currentPeriodState']=dict(pr) if pr else None
        runtime['workerRestarts']=dict(_worker_restarts)
        with _tron_http_lock:
            runtime['tronRateLimit']=dict(_tron_rate_diag)
            with _barrier_diag_lock: runtime['eventDrivenLifecycle']=dict(_barrier_diag)
        return jsonify({'ok':True,'period':ps,'platformPeriod':platform,'modelVersion':MODEL_VERSION,'counts':counts,'runtime':runtime})
    except Exception as exc:
        return jsonify({'ok':False,'error':type(exc).__name__,'message':str(exc)[:200],'runtime':dict(_runtime_health)}),503
    finally:
        if conn is not None: db_release(conn)


def _timeline_runtime_row(row):
    """Compare wall-clock observations with original on-chain block timestamps."""
    trace=row.get('lifecycle_trace') or {}
    if isinstance(trace,str):
        try: trace=json.loads(trace)
        except (TypeError,ValueError): trace={}
    def at(event):
        stamp=(trace.get(event) or {}).get('at')
        try: return datetime.fromisoformat(stamp) if stamp else None
        except (TypeError,ValueError): return None
    detail=(trace.get('event_lock_attempt') or {}).get('detail') or {}
    try: first=datetime.fromisoformat(detail['firstSeenAt']) if detail.get('firstSeenAt') else None
    except (TypeError,ValueError): first=None
    g17=row.get('g17_block_time'); target=row.get('block_time')
    attempt=at('event_lock_attempt') or at('lock_attempt')
    locked=row.get('saved_locked_at')
    def delta(later,earlier):
        if not later or not earlier:return None
        try:return round((later-earlier).total_seconds(),2)
        except (TypeError,ValueError):return None
    return {'period':row['period_key'],'state':row['state'],
            'groupsSeen':row.get('groups_seen'),'lastError':row.get('last_error'),
            'hasPrediction':locked is not None,
            'g17ToTargetSeconds':delta(target,g17),
            'g17FetchSeconds':delta(first,g17),
            'g17ToLockAttemptSeconds':delta(attempt,g17),
            'attemptToSavedSeconds':delta(locked,attempt),
            'deltaSeconds':delta(locked,target),
            'lockSource':detail.get('source')}


def history_shadow_audit(rows):
    """Evaluate immutable research forecasts against original chain timestamps."""
    valid=hits=baseline3=late=missing=mismatch=0
    for r in rows:
        ds=r['period_date'].isoformat() if hasattr(r['period_date'],'isoformat') else str(r['period_date'])
        if int(r['target_block'])!=period_target_block(ds,int(r['period_no'])):
            mismatch+=1;continue
        block=r.get('block_time'); locked=r.get('locked_at')
        if block is None:missing+=1;continue
        if locked is None or locked>=block:late+=1;continue
        actual=int(r['chain_single']);valid+=1
        hits+=int(int(r['prediction'])==actual);baseline3+=int(actual==3)
    return {'saved':len(rows),'valid':valid,'hits':hits,'baseline3Hits':baseline3,
            'late':late,'missingResult':missing,'targetMismatch':mismatch,
            'top1Rate':round(100*hits/valid,2) if valid else None}


@app.get('/api/ai-audit')
def ai_audit():
    """On-demand evidence breakdown; never recalculates an old prediction."""
    limit=request.args.get('limit',1000,type=int)
    if limit not in (100,500,1000):limit=1000
    conn=None
    try:
        conn=db_connect(retries=0)
        if conn is None:
            return jsonify({'ok':False,'error':'数据库暂不可用'}),503
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT p.period_key,p.period_date,p.period_no,p.target_block,
                          p.ai_analysis,p.prediction_top3,p.actual_single,p.locked_at,p.model_version,
                          b.block_number,b.block_time,b.single_count AS chain_single
                          FROM ai_predictions p LEFT JOIN tron_blocks b ON b.block_number=p.target_block
                          ORDER BY p.period_date DESC,p.period_no DESC LIMIT %s""",(limit,))
            rows=cur.fetchall()
            cur.execute("SELECT snapshot FROM hash_model_runtime WHERE singleton=1")
            stored=cur.fetchone()
            cur.execute("""SELECT model_version,updated_at FROM service_heartbeats
                           WHERE service_role='worker' ORDER BY updated_at DESC LIMIT 1""")
            worker=cur.fetchone()
            cur.execute("""SELECT r.period_key,r.state,r.groups_seen,r.last_error,
                          r.lifecycle_trace,r.g17_ready_at,
                          g17.block_time AS g17_block_time,
                          r.prediction_locked_at,b.block_time,p.locked_at AS saved_locked_at
                          FROM period_runtime r
                          LEFT JOIN tron_blocks b ON b.block_number=r.target_block
                          LEFT JOIN tron_blocks g17 ON g17.block_number=r.target_block-3
                          LEFT JOIN ai_predictions p ON p.period_key=r.period_key
                          ORDER BY r.period_date DESC,r.period_no DESC LIMIT 8""")
            runtime=cur.fetchall()
            cur.execute("""SELECT s.period_date,s.period_no,s.target_block,s.prediction,
                          s.locked_at,b.block_time,b.single_count AS chain_single
                          FROM research_predictions s
                          LEFT JOIN tron_blocks b ON b.block_number=s.target_block
                          WHERE s.candidate='history_only' AND s.model_version=%s
                          ORDER BY s.period_date DESC,s.period_no DESC LIMIT 1000""",(MODEL_VERSION,))
            shadow_rows=cur.fetchall()
        snapshot=stored['snapshot'] if stored else None
        if isinstance(snapshot,str):snapshot=json.loads(snapshot)
        recent_runtime=[_timeline_runtime_row(row) for row in runtime]
        worker_version=worker.get('model_version') if worker else None
        worker_age=(datetime.now(timezone.utc)-worker['updated_at']).total_seconds() if worker and worker.get('updated_at') else None
        return jsonify({'ok':True,'currentModelVersion':MODEL_VERSION,
                        'workerModelVersion':worker_version,'workerAgeSeconds':round(worker_age,1) if worker_age is not None else None,
                        'recentRuntime':recent_runtime,
                        'historyOnlyShadow':history_shadow_audit(shadow_rows),
                        'audit':ai_audit_summary(rows,snapshot,MODEL_VERSION)})
    except Exception as exc:
        return jsonify({'ok':False,'error':type(exc).__name__,'message':str(exc)[:140]}),503
    finally:
        if conn is not None:db_release(conn)


@app.get('/api/research-status')
def research_status():
    """Research progress only; never return a current-period prediction."""
    conn=None
    try:
        conn=db_connect(retries=0)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT trained_through,snapshot FROM hash_model_runtime WHERE singleton=1")
            model=cur.fetchone()
            cur.execute("""SELECT COUNT(*) AS saved,
                COUNT(*) FILTER (WHERE verified_at IS NOT NULL) AS verified,
                COUNT(*) FILTER (WHERE valid_pre_result=TRUE) AS valid,
                COUNT(*) FILTER (WHERE valid_pre_result=FALSE) AS late
                FROM research_predictions WHERE model_version=%s""",(RESEARCH_VERSION,))
            counts=cur.fetchone()
            cur.execute("SELECT last_verified_period,status,updated_at FROM research_selection WHERE singleton=1")
            selection=cur.fetchone()
            cur.execute("""SELECT model_version,updated_at FROM service_heartbeats
                WHERE service_role='worker' ORDER BY updated_at DESC LIMIT 1""")
            worker=cur.fetchone()
        snapshot=(model or {}).get('snapshot') or {}
        if isinstance(snapshot,str):snapshot=json.loads(snapshot)
        age=(datetime.now(timezone.utc)-worker['updated_at']).total_seconds() if worker else None
        return jsonify({'ok':True,'mode':'background_research',
                        'researchVersion':RESEARCH_VERSION,
                        'completeHistoricalPeriods':snapshot.get('sample',0),
                        'trainedThrough':(model or {}).get('trained_through'),
                        'savedRows':counts['saved'],'verifiedRows':counts['verified'],
                        'validRows':counts['valid'],'lateRows':counts['late'],
                        'lastVerifiedPeriod':selection['last_verified_period'] if selection else None,
                        'researchStatus':selection['status'] if selection else 'collecting',
                        'workerModelVersion':worker['model_version'] if worker else None,
                        'workerAgeSeconds':round(age,1) if age is not None else None})
    except Exception as exc:
        return jsonify({'ok':False,'error':type(exc).__name__,'message':str(exc)[:160]}),503
    finally:
        if conn is not None:db_release(conn)


@app.get('/api/research-report')
def research_report():
    """Retrospective leaderboard and independent genuinely prospective trials."""
    conn=None
    try:
        conn=db_connect(retries=0)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT report,updated_at FROM research_replay_runtime WHERE singleton=1")
            replay_row=cur.fetchone()
            cur.execute("""SELECT candidate,research_method,
                COUNT(*) AS valid_periods,
                COUNT(*) FILTER (WHERE prediction=actual_single) AS top1_hits,
                MIN(period_key) AS first_period,MAX(period_key) AS last_period
                FROM research_predictions
                WHERE model_version=%s AND valid_pre_result=TRUE
                  AND actual_single IS NOT NULL
                GROUP BY candidate,research_method ORDER BY candidate,research_method""",
                (RESEARCH_VERSION,))
            rows=cur.fetchall()
            cur.execute("""SELECT candidate,COUNT(*) AS late_rows
                FROM research_predictions WHERE model_version=%s AND valid_pre_result=FALSE
                GROUP BY candidate""",(RESEARCH_VERSION,))
            late={row['candidate']:row['late_rows'] for row in cur.fetchall()}
            cur.execute("""SELECT COUNT(*) AS compared_periods,
                COUNT(*) FILTER (WHERE first.prediction=first.actual_single) AS first_hits,
                COUNT(*) FILTER (WHERE second.prediction=second.actual_single) AS second_hits,
                MIN(first.period_key) AS first_period,MAX(first.period_key) AS last_period
                FROM research_predictions first JOIN research_predictions second
                  ON first.period_key=second.period_key AND second.candidate='trial_second'
                WHERE first.candidate='trial_first' AND first.model_version=%s
                  AND second.model_version=%s AND first.valid_pre_result=TRUE
                  AND second.valid_pre_result=TRUE AND first.actual_single IS NOT NULL
                  AND first.actual_single=second.actual_single""",
                (RESEARCH_VERSION,RESEARCH_VERSION))
            common=cur.fetchone()
            cur.execute("""SELECT model_version,updated_at FROM service_heartbeats
                WHERE service_role='worker' ORDER BY updated_at DESC LIMIT 1""")
            worker=cur.fetchone()
        replay=(replay_row or {}).get('report') or {}
        if isinstance(replay,str):replay=json.loads(replay)
        trials=[];methods=[]
        for row in rows:
            record={'candidate':row['candidate'],'method':row['research_method'],
                    'validPeriods':row['valid_periods'],'top1Hits':row['top1_hits'],
                    'top1Rate':round(100*row['top1_hits']/row['valid_periods'],2),
                    'firstPeriod':row['first_period'],'lastPeriod':row['last_period'],
                    'lateExcluded':late.get(row['candidate'],0)}
            (trials if row['candidate'].startswith('trial_') else methods).append(record)
        n=int(common['compared_periods'] or 0)
        common_result={'comparedPeriods':n,'firstHits':int(common['first_hits'] or 0),
                       'secondHits':int(common['second_hits'] or 0),
                       'firstRate':round(100*common['first_hits']/n,2) if n else None,
                       'secondRate':round(100*common['second_hits']/n,2) if n else None,
                       'firstPeriod':common['first_period'],'lastPeriod':common['last_period']}
        return jsonify({'ok':True,'version':MODEL_VERSION,'researchVersion':RESEARCH_VERSION,
                        'retrospective':replay,'replayUpdatedAt':replay_row['updated_at'].isoformat() if replay_row else None,
                        'prospectiveTrials':trials,'prospectiveMethods':methods,
                        'prospectiveCommonPeriods':common_result,
                        'workerVersion':worker['model_version'] if worker else None,
                        'workerUpdatedAt':worker['updated_at'].isoformat() if worker else None,
                        'methodology':'Oldest 1000 of latest 2500 train; last up to 1500 replay. Future trials count only forecasts saved before the target block chain time. Backtest top two selection is exploratory and must be assessed on future trials.'})
    except Exception as exc:
        return jsonify({'ok':False,'error':type(exc).__name__,'message':str(exc)[:160]}),503
    finally:
        if conn is not None:db_release(conn)


@app.get('/research-report')
def research_report_page():
    return Response((BASE_DIR/'research-report.html').read_text(encoding='utf-8'),
                    mimetype='text/html; charset=utf-8',headers={'Cache-Control':'no-store'})


@app.get('/api/db-check')
def db_check():
    """Safe database connectivity diagnostics. Never returns credentials."""
    result = {
        'ok': False,
        'databaseUrlConfigured': bool(DATABASE_URL),
        'driverAvailable': psycopg2 is not None,
        'dns': {'ok': False},
        'tcp5432': {'ok': False},
        'postgresql': {'ok': False},
    }
    if not DATABASE_URL:
        result['error'] = 'DATABASE_URL is not configured'
        return jsonify(result), 503
    if psycopg2 is None:
        result['error'] = 'psycopg2 is not available'
        return jsonify(result), 503

    try:
        parsed = urlparse(DATABASE_URL)
        host = parsed.hostname
        port = parsed.port or 5432
        result['host'] = host
        result['port'] = port
        result['database'] = (parsed.path or '').lstrip('/') or None
    except Exception as exc:
        result['error'] = 'DATABASE_URL parse failed: ' + type(exc).__name__
        return jsonify(result), 500

    try:
        ip = socket.gethostbyname(host)
        result['dns'] = {'ok': True, 'ip': ip}
    except Exception as exc:
        result['dns'] = {'ok': False, 'error': f'{type(exc).__name__}: {exc}'}
        result['error'] = 'DNS lookup failed'
        return jsonify(result), 502

    try:
        with socket.create_connection((host, port), timeout=5):
            pass
        result['tcp5432'] = {'ok': True}
    except Exception as exc:
        result['tcp5432'] = {'ok': False, 'error': f'{type(exc).__name__}: {exc}'}
        result['error'] = 'TCP connection to PostgreSQL failed'
        return jsonify(result), 502

    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute('SELECT 1')
            cur.fetchone()
        result['postgresql'] = {'ok': True}
        result['ok'] = True
        return jsonify(result)
    except Exception as exc:
        # Do not echo DATABASE_URL or credentials.
        msg = str(exc).replace(DATABASE_URL, '[DATABASE_URL]')
        result['postgresql'] = {'ok': False, 'error': msg}
        result['error'] = 'PostgreSQL login/query failed'
        return jsonify(result), 502
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass



def threshold_gap_stats(rows):
    """Analyze recurrence gaps from chronological official period rows."""
    thresholds = {0:200, 1:30, 2:10, 3:7, 4:7, 5:10, 6:30, 7:200}
    seq=[]
    for r in reversed(rows):
        v=r.get('actual_single')
        if v is None: continue
        seq.append({'date': r['period_date'].isoformat() if r.get('period_date') else None,
                    'period': int(r['period_no']), 'single': int(v)})
    result={}
    for target in range(8):
        th=thresholds[target]; positions=[i for i,x in enumerate(seq) if x['single']==target]
        events=[]; total_intervals=max(0,len(positions)-1)
        for a,b in zip(positions,positions[1:]):
            gap=b-a-1
            if gap>th:
                middle=seq[a+1:b]
                events.append({'fromDate':seq[a]['date'],'fromPeriod':seq[a]['period'],
                               'toDate':seq[b]['date'],'toPeriod':seq[b]['period'],
                               'gap':gap,'middle':[x['single'] for x in middle]})
        active=None
        if positions:
            gap=len(seq)-1-positions[-1]
            if gap>th:
                middle=seq[positions[-1]+1:]
                active={'fromDate':seq[positions[-1]]['date'],'fromPeriod':seq[positions[-1]]['period'],
                        'toDate':None,'toPeriod':None,'gap':gap,'middle':[x['single'] for x in middle]}
        result[str(target)]={'threshold':th,'intervals':total_intervals,'exceeded':len(events),
                             'rate':round((len(events)/total_intervals*100),2) if total_intervals else 0,
                             'events':list(reversed(events)),'active':active}
    return {'samplePeriods':len(seq),'items':result}


@app.get('/api/history-summary')
def history_summary():
    """Official history/gaps are based on confirmed group-20 blocks; AI rows are optional metadata."""
    allowed_limits={30,50,100,500,1000,5000}
    try: requested_limit=int(request.args.get('limit',30))
    except Exception: requested_limit=30
    history_limit=requested_limit if requested_limit in allowed_limits else 30
    date_str,period,_,_=current_period(); idx_now=period_index(date_str,period)
    def period_targets(n):
        out=[]
        for off in range(1,n+1):
            idx=idx_now-off; ordinal,zero=divmod(idx,1440); d=date.fromordinal(ordinal); pno=zero+1
            ds=d.strftime('%Y-%m-%d'); out.append((ds,pno,period_target_block(ds,pno)))
        return out
    periods=period_targets(history_limit); gap_periods=period_targets(5000)
    try: block_map=get_db_blocks(list(dict.fromkeys([x[2] for x in gap_periods])))
    except Exception: block_map={}
    pred_by_key={}; conn=db_connect()
    if conn is not None:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""SELECT period_key,period_date,period_no,target_block,data_conclusion,ai_analysis,
                              prediction_top3,actual_single,created_at,locked_at,verified_at,conclusion17,conclusion17_mode
                              FROM ai_predictions ORDER BY period_date DESC,period_no DESC LIMIT 5000""")
                for pr in cur.fetchall(): pred_by_key[str(pr.get('period_key') or '')]=pr
        finally: db_release(conn)
    out=[]
    for ds,pno,target in periods:
        br=block_map.get(target); pr=pred_by_key.get(f'{ds}:{pno:04d}')
        if not br and not pr: continue
        top=(pr or {}).get('prediction_top3') or []
        if isinstance(top,str):
            try: top=json.loads(top)
            except Exception: top=[]
        nums=(br or {}).get('numbers') or []
        if isinstance(nums,str):
            try: nums=json.loads(nums)
            except Exception: nums=[]
        official_single=int(br['single_count']) if br and br.get('single_count') is not None else None
        actual=official_single
        verified=historical_prediction_verified(pr,br,target)
        stale_prediction=bool(pr and int(pr['target_block'])!=int(target))
        top=[int(x) for x in top][:3]
        out.append({'date':ds,'period':pno,'targetBlock':int(target),'dataConclusion':(pr or {}).get('data_conclusion'),
                    'aiAnalysis':(pr or {}).get('ai_analysis'),'conclusion17':(pr or {}).get('conclusion17'),
                    'conclusion17Mode':(pr or {}).get('conclusion17_mode'),'top3':top,'actualSingle':actual,'numbers':nums,
                    'hit':bool(verified and actual in top),'verified':verified,
                    'predictionStale':stale_prediction,
                    'predictionSaved':bool(pr),'officialSaved':bool(br)})
    gap_rows=[]
    for ds,pno,target in gap_periods:
        br=block_map.get(target)
        if br and br.get('single_count') is not None:
            gap_rows.append({'period_date':date.fromisoformat(ds),'period_no':pno,'actual_single':int(br['single_count'])})
    gap_stats=threshold_gap_stats(gap_rows)
    patterns=pattern_insights(date_str,period,1000)
    return jsonify({'ok':True,'rows':out,'patterns':patterns,'gapStats':gap_stats,'historyLimit':history_limit,
                    'historyMax':5000,'officialCount':sum(1 for x in out if x['officialSaved']),
                    'predictionCount':sum(1 for x in out if x['predictionSaved'])})


@app.get('/api/history')
def history():
    """Raw saved block history. Seven days of blocks are retained so 5000 one-minute periods remain available."""
    try:
        rows = get_db_recent_rows(5000)
        out = []
        for r in rows:
            out.append({
                'blockNumber': int(r['block_number']), 'block': r['block_hash'],
                'timestamp': r['block_time'].isoformat() if r.get('block_time') else None,
                'numbers': r['numbers'] if isinstance(r['numbers'], list) else json.loads(r['numbers']),
                'singleCount': int(r['single_count']),
                'savedAt': r['fetched_at'].isoformat() if r.get('fetched_at') else None
            })
        return jsonify({'ok': True, 'retentionDays': DB_RETENTION_DAYS, 'count': len(out), 'rows': out})
    except Exception as exc:
        return jsonify({'ok': False, 'error': str(exc)}), 502


# V8.1 Production: web process is read/API only. Background engine runs in worker.py.
if os.environ.get('RUN_EMBEDDED_WORKERS','0') == '1':
    start_worker_once()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
