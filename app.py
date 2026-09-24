from pathlib import Path
from flask import Flask, Response, jsonify
from urllib.request import urlopen, Request
import json
from datetime import datetime, timezone, timedelta

BASE_DIR = Path(__file__).resolve().parent
app = Flask(__name__, static_folder=str(BASE_DIR), static_url_path='')

TRON_NOWBLOCK = 'https://api.trongrid.io/wallet/getnowblock'

# Platform period: 0001-1440, one period per minute, reset daily.
CN_TZ = timezone(timedelta(hours=8))


def current_period():
    now = datetime.now(CN_TZ)
    period = now.hour * 60 + now.minute + 1
    return now.strftime('%Y-%m-%d'), period


def fetch_latest_block():
    req = Request(TRON_NOWBLOCK, headers={'User-Agent': 'TornMonitor/1.0'})
    with urlopen(req, timeout=8) as resp:
        data = json.loads(resp.read().decode('utf-8'))
    block_id = data.get('blockID') or data.get('block_header', {}).get('raw_data', {}).get('number')
    number = data.get('block_header', {}).get('raw_data', {}).get('number')
    timestamp = data.get('block_header', {}).get('raw_data', {}).get('timestamp')
    if not block_id or not isinstance(block_id, str):
        raise RuntimeError('TRON 最新区块没有返回有效 blockID')
    return {'block': block_id, 'number': number, 'timestamp': timestamp}


def calc_numbers(block_hash):
    """Apply the platform rule described by the user.
    Traverse the hash right-to-left; collect A-E letters and 0-9 digits separately,
    pair them by order, discard 00 and duplicate combinations, and continue until 7.
    """
    s = block_hash.upper()[::-1]
    letters = [c for c in s if c in 'ABCDE']
    digits = [c for c in s if c.isdigit()]
    li = di = 0
    used = set()
    result = []
    # There can be more than 7 source candidates because invalid/duplicate pairs shift.
    while len(result) < 7 and li < len(letters) and di < len(digits):
        value = int(letters[li], 16) * 10 + int(digits[di])
        li += 1
        di += 1
        if value == 0:
            continue
        if value < 1 or value > 49:
            continue
        if value in used:
            continue
        used.add(value)
        result.append(value)
    if len(result) != 7:
        raise RuntimeError('当前区块哈希按规则无法取得7个有效号码')
    return result


@app.get('/')
def index():
    return Response((BASE_DIR / 'index.html').read_text(encoding='utf-8'), mimetype='text/html; charset=utf-8')

@app.get('/style.css')
def style():
    return Response((BASE_DIR / 'style.css').read_text(encoding='utf-8'), mimetype='text/css; charset=utf-8')

@app.get('/api/draw')
def draw():
    try:
        date_str, period = current_period()
        latest = fetch_latest_block()
        numbers = calc_numbers(latest['block'])
        return jsonify({
            'ok': True,
            'date': date_str,
            'period': f'{period:04d}',
            'block': latest['block'],
            'blockNumber': latest['number'],
            'numbers': [f'{n:02d}' for n in numbers],
            'source': 'TRON latest block',
        })
    except Exception as exc:
        return jsonify({'ok': False, 'error': str(exc)}), 502

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
