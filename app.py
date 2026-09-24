from pathlib import Path
from flask import Flask, Response, jsonify
from urllib.request import urlopen, Request
import json
from datetime import datetime, timezone, timedelta, date

BASE_DIR = Path(__file__).resolve().parent
app = Flask(__name__, static_folder=str(BASE_DIR), static_url_path='')

# Keep the same TRONGrid interface family already used by the previous version.
TRON_API = 'https://api.trongrid.io/wallet'
TRON_NOWBLOCK = f'{TRON_API}/getnowblock'
TRON_BLOCK_BY_NUM = f'{TRON_API}/getblockbynum'

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


def fetch_latest_block():
    req = Request(TRON_NOWBLOCK, headers={'User-Agent': 'TornMonitor/1.0'})
    with urlopen(req, timeout=8) as resp:
        data = json.loads(resp.read().decode('utf-8'))
    block_id = data.get('blockID')
    number = data.get('block_header', {}).get('raw_data', {}).get('number')
    timestamp = data.get('block_header', {}).get('raw_data', {}).get('timestamp')
    if not block_id or not isinstance(block_id, str) or number is None:
        raise RuntimeError('TRON 最新区块没有返回有效数据')
    return {'block': block_id, 'number': int(number), 'timestamp': timestamp}


def fetch_block_by_number(number):
    payload = json.dumps({'num': int(number)}).encode('utf-8')
    req = Request(
        TRON_BLOCK_BY_NUM,
        data=payload,
        headers={'Content-Type': 'application/json', 'User-Agent': 'TornMonitor/1.0'},
        method='POST'
    )
    with urlopen(req, timeout=8) as resp:
        data = json.loads(resp.read().decode('utf-8'))
    block_id = data.get('blockID')
    if not block_id or not isinstance(block_id, str):
        raise RuntimeError(f'目标区块 {number} 尚未可读取')
    return {
        'block': block_id,
        'number': int(data.get('block_header', {}).get('raw_data', {}).get('number', number)),
        'timestamp': data.get('block_header', {}).get('raw_data', {}).get('timestamp')
    }


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
    Confirmed anchor: 2026-09-24 period 0481 -> TRON block 86511906;
    each following platform period advances exactly 20 blocks.
    """
    if date_str == '2026-09-24':
        return 86511906 + (int(period) - 481) * 20
    # For dates without a supplied calibration, retain the existing state anchor.
    return None


def collect_period_groups(date_str, period, latest_number, state):
    """Collect the 20 block positions inside the current platform period.
    Group 20 is the official result block. Groups 1-18 feed the statistics;
    groups 18-19 are also retained in the period record as requested.
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

    # A group is the corresponding consecutive TRON block; group 20 is target20.
    for group_no in range(1, 20):
        block_number = target20 - (20 - group_no)
        if block_number > latest_number:
            continue
        key = str(group_no)
        if key in groups and groups[key].get('blockNumber') == block_number and groups[key].get('numbers'):
            continue
        try:
            block = fetch_block_by_number(block_number)
            nums = calc_numbers(block['block'])
            groups[key] = {
                'group': group_no,
                'blockNumber': block_number,
                'block': block['block'],
                'numbers': [f'{n:02d}' for n in nums],
                'singleCount': calc_single_count(nums)
            }
        except Exception:
            # Leave an unavailable group unfilled; later polling can retry it.
            continue

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

    # Current-day calibration: 0481 is exactly block 86511906.
    if date_str == '2026-09-24':
        return 86511906 + (int(period) - 481) * 20

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

        # Official result = group 20. Do not publish it until its target block exists.
        official = None
        if target20 is not None and target20 <= latest['number']:
            key20 = '20'
            if key20 in groups and groups[key20].get('blockNumber') == target20:
                official = groups[key20]
            else:
                try:
                    block20 = fetch_block_by_number(target20)
                    nums20 = calc_numbers(block20['block'])
                    official = {
                        'group': 20,
                        'blockNumber': target20,
                        'block': block20['block'],
                        'numbers': [f'{n:02d}' for n in nums20],
                        'singleCount': calc_single_count(nums20)
                    }
                    groups[key20] = official
                except Exception:
                    official = None

        # Preserve the last confirmed official result until the next one exists.
        if official:
            state = {
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
            write_state(state)
        else:
            # Keep same-period group cache even when group 20 is not ready.
            state.update({'date': date_str, 'period': period_str, 'platformPeriod': platform_period, 'groups': groups})
            write_state(state)

        # If there is no current official result, return the prior published result
        # fields while still exposing current-period statistics when available.
        current_stats = stats_from_groups(groups)
        response = {
            **state,
            'platformPeriod': platform_period,
            'currentPeriod': period_str,
            'targetResultBlock': target20,
            'officialReady': bool(official),
            'groups': sorted(groups.values(), key=lambda x: x.get('group', 0)),
            'dataStats': current_stats,
            'ok': True,
            'isNew': bool(official)
        }
        if not official and not state.get('numbers'):
            response['waitingForNewResult'] = True
        return jsonify(response)
    except Exception as exc:
        # Never erase a confirmed result on a transient API error.
        if state.get('numbers'):
            response = {
                **state,
                'platformPeriod': platform_period,
                'currentPeriod': period_str,
                'dataStats': stats_from_groups(state.get('groups', {})),
                'ok': True,
                'isNew': False,
                'waitingForNewResult': True,
                'error': str(exc)
            }
            return jsonify(response)
        return jsonify({'ok': False, 'error': str(exc)}), 502


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
