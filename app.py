from pathlib import Path
from flask import Flask, Response, jsonify
from urllib.request import urlopen, Request
import json
import os
import time
import threading
from datetime import datetime, timezone, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
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
    # Continuous +20 calibration from the confirmed anchor. If the platform
    # makes another exceptional +18 adjustment, update this calibration.
    anchor_idx = period_index('2026-09-24', 1001)
    return 86522304 + (period_index(date_str, int(period)) - anchor_idx) * 20


def db_connect():
    if not DATABASE_URL or psycopg2 is None:
        return None
    return psycopg2.connect(DATABASE_URL, connect_timeout=5)


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
        return True
    finally:
        conn.close()


def save_block_to_db(block):
    nums = calc_numbers(block['block'])
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
                """, (int(block['number']), block['block'], block_time, json.dumps([f'{n:02d}' for n in nums]), calc_single_count(nums)))
    finally:
        conn.close()


def cleanup_old_db_rows():
    conn = db_connect()
    if conn is None:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM tron_blocks WHERE fetched_at < NOW() - INTERVAL '3 days'")
    finally:
        conn.close()


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
        conn.close()


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
        conn.close()


def get_db_recent_rows(limit=2000):
    conn = db_connect()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT block_number, block_hash, block_time, numbers, single_count, fetched_at FROM tron_blocks ORDER BY block_number DESC LIMIT %s", (int(limit),))
            return cur.fetchall()
    finally:
        conn.close()


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

def collect_groups_from_db(date_str, period):
    target20 = period_target_block(date_str, period)
    wanted = [target20 - (20 - g) for g in range(1, 21)]
    rows = get_db_blocks(wanted)
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
    """Map platform periods to the corresponding TRON result block.

    Confirmed anchor supplied by the user:
      2609240481 -> 86511906
      2609240482 -> 86511926
    Every following platform period advances exactly 20 TRON blocks.
    The same cadence is used across a day boundary; only the platform
    period number resets to 0001.
    """
    # If we already have a confirmed prior period, continue from that exact
    # block. This also handles the 1440 -> next-day 0001 transition.
    if state and state.get('date') and state.get('period') and state.get('blockNumber') is not None:
        try:
            elapsed = period_index(date_str, int(period)) - period_index(state['date'], int(state['period']))
            if elapsed >= 1:
                return int(state['blockNumber']) + elapsed * 20
        except Exception:
            pass

    # Current-day live calibration from the supplied platform screenshot:
    # period 1001 is exactly block 86522304.
    if date_str == '2026-09-24':
        return 86522304 + (int(period) - 1001) * 20

    # Fresh install on another date: align the chain height to the same
    # 20-block cadence. Once a real period is confirmed, state becomes the
    # authoritative anchor for subsequent periods.
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
    """Frontend reads database only. TRON fetching is done by the server worker."""
    date_str, period, period_str, platform_period = current_period()
    state = read_state() or {}
    try:
        if DATABASE_URL:
            groups, target20 = collect_groups_from_db(date_str, period)
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
            return jsonify({
                **state, 'platformPeriod': platform_period, 'currentPeriod': period_str,
                'targetResultBlock': target20, 'officialReady': bool(official),
                'resultNumbers': result_obj.get('numbers', []) if result_obj else [],
                'resultSingleCount': result_obj.get('singleCount') if result_obj else None,
                'resultBlockNumber': result_obj.get('blockNumber') if result_obj else None,
                'resultPlatformPeriod': result_obj.get('platformPeriod') if result_obj and result_obj.get('platformPeriod') else state.get('platformPeriod'),
                'result': result_obj, 'omission': omission, 'resultHistory': history,
                'groups': sorted(groups.values(), key=lambda x: x.get('group', 0)),
                'dataStats': stats_from_groups(groups), 'ok': True, 'isNew': bool(official),
                'storage': {'database': True, 'retentionDays': DB_RETENTION_DAYS, 'frontendSource': 'PostgreSQL'},
                'debug': {'databaseLatestBlock': get_db_latest_number(), 'targetBlock': target20}
            })
        # Safe fallback for local runs without DATABASE_URL.
        return jsonify({'ok': False, 'error': 'DATABASE_URL 未配置'}), 503
    except Exception as exc:
        return jsonify({'ok': False, 'error': str(exc)}), 502


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
