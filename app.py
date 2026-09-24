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
    state = read_state()

    # Same platform period: never recalculate or replace its published result.
    if state and state.get('date') == date_str and state.get('period') == period_str and state.get('numbers'):
        return jsonify({**state, 'platformPeriod': platform_period, 'singleCount': sum(int(n) % 2 for n in state['numbers']), 'ok': True, 'isNew': False})

    try:
        latest = fetch_latest_block()
        target_number = target_block_number(date_str, period, state, latest['number'])

        # The result block has not been produced yet. Keep the previous period's
        # result visible instead of clearing the screen.
        if target_number > latest['number']:
            if state and state.get('numbers'):
                return jsonify({**state, 'platformPeriod': platform_period, 'waitingForNewResult': True, 'ok': True, 'isNew': False})
            return jsonify({'ok': False, 'waitingForNewResult': True, 'error': '等待对应开奖区块'})

        target = fetch_block_by_number(target_number)
        numbers = calc_numbers(target['block'])
        single_count = sum(n % 2 for n in numbers)
        new_state = {
            'date': date_str,
            'period': period_str,
            'platformPeriod': platform_period,
            'block': target['block'],
            'blockNumber': target_number,
            'numbers': [f'{n:02d}' for n in numbers],
            'singleCount': single_count,
            'source': 'TRONGrid / getblockbynum'
        }
        write_state(new_state)
        return jsonify({**new_state, 'ok': True, 'isNew': True})
    except Exception as exc:
        if state and state.get('numbers'):
            return jsonify({**state, 'platformPeriod': platform_period, 'ok': True, 'isNew': False, 'waitingForNewResult': True, 'error': str(exc)})
        return jsonify({'ok': False, 'error': str(exc)}), 502


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
