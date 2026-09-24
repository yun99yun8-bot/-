from pathlib import Path
from flask import Flask, Response, jsonify
from urllib.request import urlopen, Request
import json
import os
import time
import threading
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
_relation_cache = {'key': None, 'at': 0, 'value': None}
_relation_cache_lock = threading.Lock()


def current_period():
    now = datetime.now(CN_TZ)
    # Platform minute numbering: 08:01 is period 0481.
    # At 00:00, the previous day's 1440th period is still displayed;
    # 00:01 starts the new day at 0001.
    period = 1440 if (now.hour == 0 and now.minute == 0) else (now.hour * 60 + now.minute)
    date_for_period = now.date()
    if now.hour == 0 and now.minute == 0:
        # Keep the calendar date shown by the platform for the closing minute.
        date_for_period = now.date()
    date_str = date_for_period.strftime('%Y-%m-%d')
    period_str = f'{period:04d}'
    platform_period = now.strftime('%y%m%d') + period_str
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
    """Return the platform's group-20/result block for a period.

    Calibration recorded from the platform:
      2026-09-24 period 0481 -> 86511906.
      The platform can occasionally make a +18 adjustment instead of +20;
      the user reported one such adjustment around period 0480.
      Current live calibration from the supplied screenshot is:
      period 1001 -> block 86522304.

    For the current/live range we therefore anchor to 1001 and advance +20
    per period until the user reports another adjustment.
    """
    # Piecewise calibration from confirmed platform screenshots.
    # 1001 -> 86522304, then +20 through period 1200.
    # 1200 -> 86526284 and 1201 -> 86526302, so the 1200->1201 step is +18.
    # From 1201 onward we continue the normal +20 cadence until another
    # platform adjustment is observed.
    anchor_idx = period_index('2026-09-24', 1001)
    idx = period_index(date_str, int(period))
    block = 86522304 + (idx - anchor_idx) * 20
    adjustment_idx = period_index('2026-09-24', 1201)
    if idx >= adjustment_idx:
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
                        sample_size SMALLINT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        actual_single SMALLINT,
                        verified_at TIMESTAMPTZ
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_predictions_date ON ai_predictions(period_date DESC, period_no DESC)")
        return True
    finally:
        db_release(conn)


def save_block_to_db(block):
    nums = calc_numbers(block['block'])
    formatted = [f'{n:02d}' for n in nums]
    single_count = calc_single_count(nums)
    bn = int(block['number'])

    # Publish to live memory FIRST. The 20-group panel therefore advances as
    # soon as a TRON block is calculated; it does not wait for PostgreSQL.
    with _live_blocks_lock:
        _live_blocks[bn] = {
            'block_number': bn, 'block_hash': block['block'],
            'numbers': formatted, 'single_count': single_count
        }
        # Only a small rolling window is needed for the live panel.
        if len(_live_blocks) > 240:
            for old_bn in sorted(_live_blocks)[:-160]:
                _live_blocks.pop(old_bn, None)

    ts = block.get('timestamp')
    block_time = datetime.fromtimestamp(ts / 1000, timezone.utc) if ts else None
    conn = db_connect()
    if conn is None:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO tron_blocks(block_number, block_hash, block_time, numbers, single_count)
                    VALUES (%s,%s,%s,%s::jsonb,%s)
                    ON CONFLICT (block_number) DO UPDATE SET
                      block_hash=EXCLUDED.block_hash,
                      block_time=EXCLUDED.block_time,
                      numbers=EXCLUDED.numbers,
                      single_count=EXCLUDED.single_count
                """, (bn, block['block'], block_time, json.dumps(formatted), single_count))
    finally:
        db_release(conn)


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


def tron_ingest_worker():
    """Server-side collector. It runs even when no browser is open."""
    try:
        init_db()
    except Exception:
        pass
    last_cleanup = 0
    while True:
        try:
            latest = fetch_latest_block()
            db_latest = get_db_latest_number()
            # First start: backfill enough blocks to cover several platform rounds.
            start = max(int(latest['number']) - 79, 0) if db_latest is None else db_latest + 1
            end = int(latest['number'])
            # Avoid a huge catch-up burst after downtime; history is only 3 days and
            # future polls continue filling forward.
            if end - start > 399:
                start = end - 399
            for n in range(start, end + 1):
                try:
                    block = latest if n == end else fetch_block_by_number(n)
                    save_block_to_db(block)
                except Exception:
                    continue
            if time.time() - last_cleanup > 3600:
                cleanup_old_db_rows()
                last_cleanup = time.time()
        except Exception:
            pass
        time.sleep(1)


def start_worker_once():
    global _worker_started
    if not DATABASE_URL or psycopg2 is None:
        return
    with _worker_lock:
        if _worker_started:
            return
        _worker_started = True
        threading.Thread(target=tron_ingest_worker, name='tron-ingest', daemon=True).start()


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
    """Build the current 20-group frame from live memory + PostgreSQL.

    Memory wins for newly calculated blocks, so each new group can appear on
    the next frontend poll even if the database is briefly slow. PostgreSQL
    fills older/current rows after restarts.
    """
    target20 = period_target_block(date_str, period)
    wanted = [target20 - (20 - g) for g in range(1, 21)]
    try:
        rows = get_db_blocks(wanted)
    except Exception:
        rows = {}
    with _live_blocks_lock:
        live = {bn: dict(_live_blocks[bn]) for bn in wanted if bn in _live_blocks}
    rows.update(live)
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
    history = []
    idx_now = period_index(date_str, period)
    for offset in range(count - 1, -1, -1):
        idx = idx_now - offset
        ordinal, zero = divmod(idx, 1440)
        d = date.fromordinal(ordinal)
        p = zero + 1
        target = period_target_block(d.strftime('%Y-%m-%d'), p)
        rows = get_db_blocks([target])
        row = rows.get(target)
        if row:
            pp = d.strftime('%y%m%d') + f'{p:04d}'
            history.append({'platformPeriod': pp, 'period': f'{p:04d}', 'blockNumber': target, 'singleCount': int(row['single_count'])})
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
    """Return prior official group-20 single counts from retained DB history."""
    target_now = period_target_block(date_str, period)
    conn = db_connect()
    if conn is None:
        return []
    try:
        with conn.cursor() as cur:
            # Official result blocks follow the platform's 20-block cadence.
            cur.execute("""
                SELECT single_count FROM tron_blocks
                WHERE block_number < %s AND MOD((%s - block_number), 20) = 0
                ORDER BY block_number DESC LIMIT %s
            """, (target_now, target_now, int(limit)))
            return [int(r[0]) for r in cur.fetchall()]
    finally:
        db_release(conn)


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
            0.18 * relation_signal[i] +
            0.04 * omit_signal +
            0.02 * (0.5 + momentum)
        )

    total = sum(raw.values()) or 1.0
    scores = {i: raw[i] / total * 100.0 for i in range(8)}
    ranked = sorted(range(8), key=lambda i: (-scores[i], i))
    pick = ranked[0]
    return pick, {str(i): round(scores[i], 2) for i in range(8)}

def save_prediction_if_ready(date_str, period, groups, target20):
    stats=stats_from_groups(groups)
    if stats.get('sampleSize') != 18 or groups.get('20'):
        return None
    historical=get_historical_official_singles(date_str, period)
    relation=group20_relation_model(date_str, period)
    ai, scores=ai_analysis_from_data(groups,historical,relation)
    if ai is None: return None
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
                    INSERT INTO ai_predictions(period_key,period_date,period_no,target_block,data_conclusion,ai_analysis,sample_size)
                    VALUES(%s,%s,%s,%s,%s,%s,18)
                    ON CONFLICT(period_key) DO NOTHING
                """,(key,date_str,int(period),int(target20),data_conclusion,int(ai)))
    finally: db_release(conn)
    return {'single':ai,'scores':scores,'historicalSample':len(historical),'frozen':True}


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


def prediction_summary(date_str, period, groups, target20, official):
    key=f'{date_str}:{int(period):04d}'
    if official: verify_prediction(date_str,period,official)
    else: save_prediction_if_ready(date_str,period,groups,target20)
    conn=db_connect(); row=None; agg=None
    if conn is not None:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM ai_predictions WHERE period_key=%s",(key,)); row=cur.fetchone()
                cur.execute("""SELECT COUNT(*) FILTER (WHERE actual_single IS NOT NULL) verified,
                    COUNT(*) FILTER (WHERE actual_single IS NOT NULL AND ai_analysis=actual_single) ai_hits,
                    COUNT(*) FILTER (WHERE actual_single IS NOT NULL AND data_conclusion IS NOT NULL) data_verified,
                    COUNT(*) FILTER (WHERE actual_single IS NOT NULL AND data_conclusion=actual_single) data_hits
                    FROM ai_predictions"""); agg=cur.fetchone()
        finally: db_release(conn)
    historical=get_historical_official_singles(date_str,period)
    relation=group20_relation_model(date_str, period)
    live_ai,scores=ai_analysis_from_data(groups,historical,relation)
    ai_single=int(row['ai_analysis']) if row else live_ai
    verified=int(agg['verified'] or 0) if agg else 0; hits=int(agg['ai_hits'] or 0) if agg else 0
    dv=int(agg['data_verified'] or 0) if agg else 0; dh=int(agg['data_hits'] or 0) if agg else 0
    matches=[]
    if official:
        actual=int(official['singleCount'])
        matches=[i for i in range(1,20) if str(i) in groups and int(groups[str(i)].get('singleCount',-1))==actual]
    return {'single':ai_single,'scores':scores,'historicalSample':len(historical),
            'frozen':bool(row),'verifiedSample':verified,'hits':hits,
            'hitRate':round(hits/verified*100,2) if verified else None,
            'dataVerifiedSample':dv,'dataHits':dh,'dataHitRate':round(dh/dv*100,2) if dv else None,
            'sameAsGroup20':matches,'relationTop3':(relation.get('ranking') or [])[:3],'relationSample':relation.get('samplePeriods',0)}

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
                'groupPeriodKey': f'{date_str}:{period_str}', 'source': 'PostgreSQL / backend collector'
            }
            state['omission'] = update_omission(state, official)
            write_state(state)
        result_obj = official
        if result_obj is None and isinstance(state.get('numbers'), list) and len(state.get('numbers')) == 7:
            result_obj = {'numbers': state['numbers'], 'singleCount': state.get('singleCount'), 'blockNumber': state.get('blockNumber'), 'block': state.get('block'), 'platformPeriod': state.get('platformPeriod')}
        omission = state.get('omission') if isinstance(state.get('omission'), dict) else {}
        omission = {str(i): int(omission.get(str(i), 0) or 0) for i in range(8)}
        history = build_recent_official_history(date_str, period, 20)
        ai_info = prediction_summary(date_str, period, groups, target20, official)
        payload = {
            **state, 'platformPeriod': platform_period, 'currentPeriod': period_str,
            'targetResultBlock': target20, 'officialReady': bool(official),
            'resultNumbers': result_obj.get('numbers', []) if result_obj else [],
            'resultSingleCount': result_obj.get('singleCount') if result_obj else None,
            'resultBlockNumber': result_obj.get('blockNumber') if result_obj else None,
            'resultPlatformPeriod': result_obj.get('platformPeriod') if result_obj and result_obj.get('platformPeriod') else state.get('platformPeriod'),
            'result': result_obj, 'omission': omission, 'resultHistory': history,
            'aiAnalysis': ai_info,
            'groups': sorted(groups.values(), key=lambda x: x.get('group', 0)),
            'dataStats': stats_from_groups(groups), 'ok': True, 'isNew': bool(official),
            'databaseStatus': 'connected', 'stale': False,
            'storage': {'database': True, 'retentionDays': DB_RETENTION_DAYS, 'frontendSource': 'PostgreSQL + memory fallback'},
            'debug': {'databaseLatestBlock': get_db_latest_number(), 'targetBlock': target20}
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
