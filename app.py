from pathlib import Path
from flask import Flask, Response, jsonify, request
from urllib.request import urlopen, Request
import json
import os
import time
import threading
import queue
import socket
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed

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
DB_RETENTION_DAYS = 3
_worker_started = False
_worker_lock = threading.Lock()
_db_pool = None
_db_pool_lock = threading.Lock()
_draw_cache = None
_draw_cache_lock = threading.Lock()
_live_blocks = {}
_live_blocks_lock = threading.Lock()
_db_write_queue = queue.Queue(maxsize=1000)
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
_historical_singles_cache = {'key': None, 'at': 0, 'value': None}
_historical_singles_cache_lock = threading.Lock()


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
    data = json.dumps(payload).encode('utf-8') if payload is not None else None
    req = Request(url, data=data, headers={'Content-Type': 'application/json', 'User-Agent': 'TornMonitor/2.2'}, method=method)
    with urlopen(req, timeout=4) as resp:
        return json.loads(resp.read().decode('utf-8'))

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
    """Race non-solidity providers for the target block; first valid block wins."""
    number = int(number)
    started = time.perf_counter()
    errors = {}

    def via_trongrid():
        data = _get_json(TRON_BLOCK_BY_NUM, method='POST', payload={'num': number})
        block_id = data.get('blockID')
        if not block_id:
            raise RuntimeError('block not found')
        return 'TRONGrid-FullNode', {
            'block': block_id,
            'number': int(data.get('block_header', {}).get('raw_data', {}).get('number', number)),
            'timestamp': data.get('block_header', {}).get('raw_data', {}).get('timestamp')
        }

    def via_tronscan():
        data = _get_json(f'{TRONSCAN_BLOCK}?number={number}')
        rows = data.get('data') if isinstance(data, dict) else None
        row = rows[0] if isinstance(rows, list) and rows else None
        if not row or not row.get('hash'):
            raise RuntimeError('block not found')
        return 'TRONScan', {'block': row['hash'], 'number': int(row.get('number', number)), 'timestamp': row.get('timestamp')}

    pool = ThreadPoolExecutor(max_workers=2)
    futures = [pool.submit(via_trongrid), pool.submit(via_tronscan)]
    try:
        for future in as_completed(futures, timeout=3.2):
            try:
                provider, block = future.result()
                if int(block.get('number', -1)) == number and block.get('block'):
                    elapsed = round((time.perf_counter() - started) * 1000, 1)
                    for f in futures:
                        if f is not future:
                            f.cancel()
                    pool.shutdown(wait=False, cancel_futures=True)
                    return block, provider, elapsed, errors
            except Exception as exc:
                errors[str(len(errors)+1)] = str(exc)
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    raise RuntimeError('target block not yet available: ' + ' | '.join(errors.values()))


def target_result_fast_worker():
    """Directly watch the known group-20 block, bypassing latest-block and DB paths."""
    last_target = None
    while True:
        sleep_for = 0.20
        try:
            date_str, period, _, _ = current_period()
            target = period_target_block(date_str, period)
            with _live_blocks_lock:
                ready = target in _live_blocks
            if target != last_target:
                last_target = target
                with _fast_diag_lock:
                    _fast_diag.update({'target': target, 'provider': None, 'firstSeenAt': None, 'latencyMs': None, 'errors': {}})
            if not ready:
                try:
                    block, provider, latency_ms, errors = fetch_block_fast(target)
                    publish_block_live(block)
                    enqueue_block_for_db(block)
                    with _fast_diag_lock:
                        _fast_diag.update({
                            'target': target, 'provider': provider,
                            'firstSeenAt': datetime.now(CN_TZ).isoformat(timespec='milliseconds'),
                            'latencyMs': latency_ms, 'errors': errors
                        })
                except Exception as exc:
                    with _fast_diag_lock:
                        _fast_diag['errors'] = {'last': str(exc)[:300]}
            else:
                sleep_for = 0.35
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


def period_target_block(date_str, period):
    """Return the best-known platform group-20 block from confirmed anchors.

    Important: the platform has occasional +18 transitions. We only encode
    transitions whose location is known; we never invent the location of the
    observed two-block shift between period 0551 and 1001. Current/live periods
    are anchored from 1001, with the confirmed 1200->1201 +18 transition.
    """
    idx=period_index(date_str,int(period))
    i480=period_index('2026-09-24',480)
    i481=period_index('2026-09-24',481)
    i551=period_index('2026-09-24',551)
    i1001=period_index('2026-09-24',1001)
    i1201=period_index('2026-09-24',1201)
    if idx <= i480:
        return 86511888 + (idx-i480)*20
    if i481 <= idx <= i551:
        return 86511906 + (idx-i481)*20
    # The exact +18 transition between 0551 and 1001 was not observed. For
    # that historical gap, use the nearest confirmed anchor rather than claim
    # a fabricated transition point.
    if idx < i1001:
        if idx-i551 <= i1001-idx:
            return 86513306 + (idx-i551)*20
        return 86522304 + (idx-i1001)*20
    block=86522304 + (idx-i1001)*20
    if idx >= i1201:
        block -= 2
    return block

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
            _db_pool = ThreadedConnectionPool(1, 4, DATABASE_URL, connect_timeout=5)
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
        return True
    finally:
        db_release(conn)


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

def enqueue_block_for_db(block):
    try:
        _db_write_queue.put_nowait(dict(block))
    except queue.Full:
        pass

def db_writer_worker():
    """Database writes are deliberately off the realtime result path."""
    while True:
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
                cur.execute("DELETE FROM tron_blocks WHERE fetched_at < NOW() - INTERVAL '3 days'")
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
            cur.execute("SELECT block_number, block_hash, numbers, single_count FROM tron_blocks WHERE block_number = ANY(%s)", (nums,))
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
        try:
            latest = fetch_latest_block()
            end = int(latest['number'])
            if last_seen is None:
                # Warm a small window once; afterwards only new blocks are fetched.
                try:
                    db_latest = get_db_latest_number()
                except Exception:
                    db_latest = None
                last_seen = db_latest if db_latest is not None else max(end - 79, 0)
            if end > last_seen:
                start_n = max(last_seen + 1, end - 79)
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
        # 0.5 s detection loop reduces our own polling latency without tying
        # result publication to database latency.
        time.sleep(0.5)

def start_worker_once():
    global _worker_started
    if not DATABASE_URL or psycopg2 is None:
        return
    with _worker_lock:
        if _worker_started:
            return
        _worker_started = True
        threading.Thread(target=db_writer_worker, name='db-writer', daemon=True).start()
        threading.Thread(target=tron_ingest_worker, name='tron-ingest', daemon=True).start()
        threading.Thread(target=target_result_fast_worker, name='target-result-fast', daemon=True).start()


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
                'group': g, 'blockNumber': bn, 'block': row['block_hash'],
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



def get_historical_official_singles(date_str, period, limit=720):
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

def ai_conclusion17_from_data(groups, historical):
    """17-group conclusion model. Returns a model tendency, not a guaranteed probability."""
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
    model=ai_conclusion17_from_data(groups,historical)
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


def save_prediction_if_ready(date_str, period, groups, target20, ui_countdown=None):
    # The official AI decision is made exactly once in the final 10 seconds.
    # Before that we may calculate live signals internally, but they are not persisted as the prediction.
    # The browser countdown is the single source of truth for the 10-second lock.
    # Only an explicit UI trigger at displayed 10 may create the official prediction.
    countdown = int(ui_countdown) if ui_countdown is not None else None
    if countdown != 10:
        return None
    stats=stats_from_groups(groups)
    # Lock with whatever current-period evidence is available at UI 10.
    # Do not miss the lock merely because one live group arrived late.
    if int(stats.get('sampleSize') or 0) < 1 or groups.get('20'):
        return None
    historical=get_historical_official_singles(date_str, period)
    relation=group20_relation_model(date_str, period)
    ai, scores=ai_analysis_from_data(groups,historical,relation)
    if ai is None: return None
    ranked_top3 = sorted(range(8), key=lambda i: (-float(scores.get(str(i), 0)), i))[:3]
    highest=stats.get('highest') or []
    # A tied data conclusion is not forced into a false single choice.
    data_conclusion=int(highest[0]['single']) if len(highest)==1 else None
    key=f'{date_str}:{int(period):04d}'
    conn=db_connect()
    if conn is None: return None
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_predictions(period_key,period_date,period_no,target_block,data_conclusion,ai_analysis,prediction_top3,sample_size)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(period_key) DO NOTHING
                """,(key,date_str,int(period),int(target20),data_conclusion,int(ai),json.dumps(ranked_top3),int(stats.get('sampleSize') or 0)))
    finally: db_release(conn)
    return {'single':ai,'scores':scores,'historicalSample':len(historical),'frozen':True,'period':int(period),'lockCountdown':countdown}


def verify_prediction(date_str, period, official):
    if not official: return
    key=f'{date_str}:{int(period):04d}'
    conn=db_connect()
    if conn is None:return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""UPDATE ai_predictions SET actual_single=%s,verified_at=COALESCE(verified_at,NOW())
                               WHERE period_key=%s AND actual_single IS NULL""",
                            (int(official['singleCount']),key))
    finally: db_release(conn)


def prediction_summary(date_str, period, groups, target20, official, ui_countdown=None):
    key=f'{date_str}:{int(period):04d}'
    now=time.time()
    # Explicit UI=10 lock must be handled BEFORE any summary-cache return.
    # Otherwise a request arriving within the cache TTL can silently miss the only lock event.
    if official:
        verify_prediction(date_str,period,official)
    else:
        save_prediction_if_ready(date_str,period,groups,target20,ui_countdown)
    with _ai_summary_cache_lock:
        cached=_ai_summary_cache.get('value') if _ai_summary_cache.get('key')==key else None
        cached_at=_ai_summary_cache.get('at',0)
    if cached is not None and not official and ui_countdown != 10 and now-cached_at < 1.0:
        return dict(cached)
    conn=db_connect(); row=None; agg=None; latest_verified=None
    if conn is not None:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM ai_predictions WHERE period_key=%s",(key,)); row=cur.fetchone()
                cur.execute("""SELECT COUNT(*) FILTER (WHERE actual_single IS NOT NULL) verified,
                    COUNT(*) FILTER (WHERE actual_single IS NOT NULL AND ai_analysis=actual_single) ai_hits,
                    COUNT(*) FILTER (WHERE actual_single IS NOT NULL AND prediction_top3 @> to_jsonb(ARRAY[actual_single])) top3_hits,
                    COUNT(*) FILTER (WHERE actual_single IS NOT NULL AND data_conclusion IS NOT NULL) data_verified,
                    COUNT(*) FILTER (WHERE actual_single IS NOT NULL AND data_conclusion=actual_single) data_hits
                    FROM ai_predictions"""); agg=cur.fetchone()
                cur.execute("""SELECT period_no, prediction_top3, actual_single FROM ai_predictions
                    WHERE actual_single IS NOT NULL AND prediction_top3 IS NOT NULL
                    ORDER BY verified_at DESC NULLS LAST, period_date DESC, period_no DESC LIMIT 1""")
                latest_verified=cur.fetchone()
        finally: db_release(conn)
    historical=get_historical_official_singles(date_str,period)
    relation=group20_relation_model(date_str, period)
    live_ai,scores=ai_analysis_from_data(groups,historical,relation)
    ai_single=int(row['ai_analysis']) if row else live_ai
    frozen_top3=[]
    if row and row.get('prediction_top3') is not None:
        frozen_top3=row.get('prediction_top3') or []
        if isinstance(frozen_top3,str):
            try: frozen_top3=json.loads(frozen_top3)
            except Exception: frozen_top3=[]
        frozen_top3=[int(x) for x in frozen_top3][:3]
    live_top3=sorted(range(8), key=lambda i:(-float(scores.get(str(i),0)),i))[:3] if scores else []
    display_top3=frozen_top3 if frozen_top3 else live_top3
    verified=int(agg['verified'] or 0) if agg else 0; hits=int(agg['ai_hits'] or 0) if agg else 0
    dv=int(agg['data_verified'] or 0) if agg else 0; dh=int(agg['data_hits'] or 0) if agg else 0
    matches=[]
    if official:
        actual=int(official['singleCount'])
        matches=[i for i in range(1,20) if str(i) in groups and int(groups[str(i)].get('singleCount',-1))==actual]
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
    result={'single':ai_single,'scores':scores,'historicalSample':len(historical),
            'frozen':bool(row),'period':int(row['period_no']) if row else int(period),
            'top3':display_top3,'verifiedSample':verified,'hits':hits,
            'top3Hits':top3_hits,'top3HitRate':round(top3_hits/verified*100,2) if verified else None,'latestVerified':latest_result,
            'hitRate':round(hits/verified*100,2) if verified else None,
            'dataVerifiedSample':dv,'dataHits':dh,'dataHitRate':round(dh/dv*100,2) if dv else None,
            'sameAsGroup20':matches,'relationTop3':(relation.get('ranking') or [])[:3],'relationSample':relation.get('samplePeriods',0)}
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

    Uses the same piecewise calibration as period_target_block(), including
    the confirmed +18 transition from 2026-09-24 period 1200 to 1201.
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
        omission = state.get('omission') if isinstance(state.get('omission'), dict) else {}
        omission = {str(i): int(omission.get(str(i), 0) or 0) for i in range(8)}
        history = build_recent_official_history(date_str, period, 20)
        ui_countdown = request.args.get('ai_lock_countdown', type=int)
        ai_info = prediction_summary(date_str, period, groups, target20, official, ui_countdown)
        historical_for_17 = get_historical_official_singles(date_str, period)
        ai17 = ai_conclusion17_from_data(groups, historical_for_17)
        if ai17 and not official:
            try: save_conclusion17_if_ready(date_str, period, groups, target20)
            except Exception: pass
        payload = {
            **state, 'platformPeriod': platform_period, 'currentPeriod': period_str, 'groupPeriodKey': f'{date_str}:{period_str}',
            'targetResultBlock': target20, 'officialReady': bool(official),
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
            'debug': {'targetBlock': target20, 'fastTarget': dict(_fast_diag)}
        }
        with _draw_cache_lock:
            _draw_cache = dict(payload)
        return jsonify(payload)
    except Exception as exc:
        with _draw_cache_lock:
            cached = dict(_draw_cache) if isinstance(_draw_cache, dict) else None
        if cached:
            cached.update({
                'ok': True, 'databaseStatus': 'reconnecting', 'stale': True,
                'databaseError': type(exc).__name__,
                'currentPeriod': period_str, 'platformPeriod': platform_period
            })
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


@app.get('/api/history-summary')
def history_summary():
    """Compact period-level history for the History tab."""
    conn=db_connect()
    if conn is None:
        return jsonify({'ok':False,'error':'database unavailable'}),503
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT p.period_date,p.period_no,p.target_block,p.data_conclusion,p.ai_analysis,
                          p.prediction_top3,p.actual_single,p.created_at,p.verified_at,b.numbers,b.single_count
                          FROM ai_predictions p LEFT JOIN tron_blocks b ON b.block_number=p.target_block
                          ORDER BY p.period_date DESC,p.period_no DESC LIMIT 100""")
            rows=cur.fetchall()
        out=[]
        for r in rows:
            top=r.get('prediction_top3') or []
            if isinstance(top,str):
                try: top=json.loads(top)
                except Exception: top=[]
            nums=r.get('numbers') or []
            if isinstance(nums,str):
                try: nums=json.loads(nums)
                except Exception: nums=[]
            actual=r.get('actual_single')
            hit=bool(actual is not None and int(actual) in [int(x) for x in top])
            out.append({'date':r['period_date'].isoformat() if r.get('period_date') else None,
                        'period':int(r['period_no']),'targetBlock':int(r['target_block']),
                        'dataConclusion':r.get('data_conclusion'),'aiAnalysis':r.get('ai_analysis'),
                        'top3':[int(x) for x in top][:3],'actualSingle':int(actual) if actual is not None else None,
                        'numbers':nums,'hit':hit,'verified':actual is not None})
        date_str, period, _, _ = current_period()
        patterns=pattern_insights(date_str,period,1000)
        return jsonify({'ok':True,'rows':out,'patterns':patterns})
    finally:
        db_release(conn)


@app.get('/api/history')
def history():
    """Raw saved block history, limited to the retained three-day database window."""
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
        return jsonify({'ok': True, 'retentionDays': 3, 'count': len(out), 'rows': out})
    except Exception as exc:
        return jsonify({'ok': False, 'error': str(exc)}), 502


start_worker_once()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
