from pathlib import Path
from flask import Flask, Response, jsonify
from urllib.request import urlopen, Request
import json
from datetime import datetime, timezone, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    if date_str == '2026-09-24':
        return 86522304 + (int(period) - 1001) * 20
    return None


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

def stats_from_groups(groups):
    """Statistics are based only on groups 1-18."""
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
    return Response((BASE_DIR / 'index.html').read_text(encoding='utf-8'), mimetype='text/html; charset=utf-8')


@app.get('/style.css')
def style():
    return Response((BASE_DIR / 'style.css').read_text(encoding='utf-8'), mimetype='text/css; charset=utf-8')


@app.get('/api/draw')
def draw():
    date_str, period, period_str, platform_period = current_period()
    state = read_state() or {}

    try:
        latest = fetch_latest_block()
        groups, target20 = collect_period_groups(date_str, period, latest['number'], state)

        # Official result = group 20. collect_period_groups prioritizes this
        # row, so do not make a second blocking network request here.
        official = None
        if target20 is not None and target20 <= latest['number']:
            key20 = '20'
            candidate20 = groups.get(key20)
            if isinstance(candidate20, dict) and candidate20.get('blockNumber') == target20 and candidate20.get('numbers'):
                official = candidate20

        # Preserve the last confirmed official result until the next one exists.
        if official:
            # Update omission exactly once when a new official period is confirmed.
            prior = dict(state)
            state = {
                **prior,
                'date': date_str,
                'period': period_str,
                'platformPeriod': platform_period,
                'block': official['block'],
                'blockNumber': official['blockNumber'],
                'numbers': official['numbers'],
                'singleCount': official['singleCount'],
                'groups': groups,
                'source': 'TRONGrid / getblockbynum'
            }
            state['omission'] = update_omission(state, official)
            write_state(state)
        else:
            # Keep same-period group cache even when group 20 is not ready.
            state.update({'date': date_str, 'period': period_str, 'platformPeriod': platform_period, 'groups': groups})
            write_state(state)

        # If there is no current official result, return the prior published result
        # fields while still exposing current-period statistics when available.
        current_stats = stats_from_groups(groups)
        # Return dedicated result fields as well as the legacy state fields.
        # This keeps the frontend stable even when the current period is still
        # waiting for group 20 or when an older confirmed result is being kept.
        # Build one authoritative result object. Prefer confirmed group 20, then
        # the persisted confirmed result. This prevents the UI from losing the
        # numbers when the current-period cache is refreshed.
        result_obj = None
        g20 = groups.get('20') if isinstance(groups, dict) else None
        if isinstance(g20, dict) and isinstance(g20.get('numbers'), list) and len(g20.get('numbers')) == 7:
            result_obj = g20
        elif isinstance(state.get('numbers'), list) and len(state.get('numbers')) == 7:
            result_obj = {
                'numbers': state.get('numbers'),
                'singleCount': state.get('singleCount'),
                'blockNumber': state.get('blockNumber'),
                'block': state.get('block'),
                'platformPeriod': state.get('platformPeriod'),
            }
        saved_numbers = result_obj.get('numbers', []) if result_obj else []
        saved_omission = state.get('omission') if isinstance(state.get('omission'), dict) else {}
        saved_omission = {str(i): int(saved_omission.get(str(i), 0) or 0) for i in range(8)}
        response = {
            **state,
            'platformPeriod': platform_period,
            'currentPeriod': period_str,
            'targetResultBlock': target20,
            'officialReady': bool(official),
            'resultNumbers': saved_numbers,
            'resultSingleCount': result_obj.get('singleCount') if result_obj else None,
            'resultBlockNumber': result_obj.get('blockNumber') if result_obj else None,
            'resultPlatformPeriod': result_obj.get('platformPeriod') if result_obj and result_obj.get('platformPeriod') else (state.get('platformPeriod') if saved_numbers else None),
            'result': result_obj,
            'omission': saved_omission,
            'groups': sorted(groups.values(), key=lambda x: x.get('group', 0)),
            'dataStats': current_stats,
            'ok': True,
            'isNew': bool(official),
            'debug': {'latestBlock': latest.get('number'), 'targetBlock': target20, 'officialBlock': official.get('blockNumber') if official else None, 'officialReady': bool(official)}
        }
        if not official and not state.get('numbers'):
            response['waitingForNewResult'] = True
        return jsonify(response)
    except Exception as exc:
        # Never erase a confirmed result on a transient API error.
        if state.get('numbers'):
            saved_omission = state.get('omission') if isinstance(state.get('omission'), dict) else {}
            saved_omission = {str(i): int(saved_omission.get(str(i), 0) or 0) for i in range(8)}
            response = {
                **state,
                'platformPeriod': platform_period,
                'currentPeriod': period_str,
                'resultNumbers': state.get('numbers', []),
                'resultSingleCount': state.get('singleCount'),
                'resultBlockNumber': state.get('blockNumber'),
                'resultPlatformPeriod': state.get('platformPeriod'),
                'result': {
                    'numbers': state.get('numbers', []),
                    'singleCount': state.get('singleCount'),
                    'blockNumber': state.get('blockNumber'),
                    'block': state.get('block'),
                    'platformPeriod': state.get('platformPeriod')
                },
                'omission': saved_omission,
                'dataStats': stats_from_groups(state.get('groups', {})),
                'ok': True,
                'isNew': False,
                'waitingForNewResult': True,
                'error': str(exc)
            }
            return jsonify(response)
        return jsonify({'ok': False, 'error': str(exc), 'debug': {'date': date_str, 'period': period_str, 'targetBlock': target20 if 'target20' in locals() else None}}), 502


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
