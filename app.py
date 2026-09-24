from flask import Flask, jsonify, render_template_string
from datetime import datetime
from zoneinfo import ZoneInfo
import os, threading, requests

app = Flask(__name__)
APP_TZ = ZoneInfo(os.getenv("APP_TIMEZONE", "Asia/Shanghai"))
TRON_API = os.getenv("TRON_API", "https://api.trongrid.io/wallet/getnowblock")

# 当前期只锁定一次开奖区块，避免一分钟内区块不断变化导致开奖结果跳动。
_lock = threading.Lock()
_period_cache = {}

def period_info(dt=None):
    dt = dt or datetime.now(APP_TZ)
    idx = dt.hour * 60 + dt.minute + 1
    return dt.strftime("%Y%m%d"), f"{idx:04d}", idx

def get_latest_block():
    r = requests.post(TRON_API, json={}, timeout=8)
    r.raise_for_status()
    data = r.json()
    block = data.get("block_header", {}).get("raw_data", {}).get("number")
    block_id = data.get("blockID", "")
    if not block_id:
        raise ValueError("TRON返回缺少blockID")
    return int(block), block_id

def calculate_numbers(block_hash):
    # 按规则：从哈希右侧开始，分别取前7个A-E字母和前7个0-9数字。
    letters = []
    digits = []
    for ch in reversed(block_hash.upper()):
        if ch in "ABCDE" and len(letters) < 7:
            letters.append(ch)
        if ch.isdigit() and len(digits) < 7:
            digits.append(ch)
        if len(letters) == 7 and len(digits) == 7:
            break
    if len(letters) < 7 or len(digits) < 7:
        raise ValueError("哈希中无法取得足够的A-E字母或数字")

    # 同组合/00弃用：整体继续向后取下一组。
    # 由于规则要求“第一组丢弃后整体往后顺延”，这里以候选序列继续扫描，
    # 每个有效组合只保留一次；00同样丢弃。
    pairs = []
    used = set()
    li = di = 0
    # 首先使用已取出的7+7候选；若不够，再继续从右侧补充候选。
    rev = list(reversed(block_hash.upper()))
    all_letters = [c for c in rev if c in "ABCDE"]
    all_digits = [c for c in rev if c.isdigit()]
    max_i = min(len(all_letters), len(all_digits))
    i = 0
    while len(pairs) < 7 and i < max_i:
        n = int(all_letters[i], 15) - 10  # A=10..E=14 -> 0..4
        d = int(all_digits[i])
        value = n * 10 + d
        if value != 0 and value not in used:
            pairs.append(value)
            used.add(value)
        i += 1
    if len(pairs) < 7:
        raise ValueError("有效号码不足7个")
    return pairs

def get_result_for_current_period():
    day, period, idx = period_info()
    key = f"{day}-{period}"
    with _lock:
        if key in _period_cache:
            return _period_cache[key]
    block, block_hash = get_latest_block()
    numbers = calculate_numbers(block_hash)
    result = {"day": day, "period": period, "block": block, "hash": block_hash, "numbers": numbers}
    with _lock:
        _period_cache[key] = result
        # 只保留最近3期，避免长期运行内存增长。
        for k in list(_period_cache.keys())[:-3]:
            _period_cache.pop(k, None)
    return result

def state():
    day, display_period, idx = period_info()
    out = {"day": day, "display_period": display_period, "period_index": idx,
           "result": None, "error": None}
    try:
        r = get_result_for_current_period()
        out["result"] = r
    except Exception as e:
        out["error"] = str(e)
    return out

PAGE = r'''<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>TRON统计</title><style>
*{box-sizing:border-box}body{margin:0;background:#f4f5f8;color:#222;font:13px -apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif}.wrap{max-width:760px;margin:auto;padding:6px}.tabs{display:flex;gap:5px;position:sticky;top:0;background:#f4f5f8;padding:4px 0;z-index:5}button{flex:1;border:0;border-radius:10px;padding:10px;font-weight:800;background:#ddd;color:#555}button.on{background:#222;color:#fff}.card{background:#fff;border-radius:12px;padding:9px;margin:6px 0;box-shadow:0 2px 8px #0000000b}.grid{display:grid;grid-template-columns:1fr 1fr;gap:5px}.box{background:#f6f7fa;border-radius:9px;padding:8px}.label{color:#7d8390;font-size:11px}.big{font-size:20px;font-weight:800}.nums{font-size:23px;font-weight:900;margin-top:7px;letter-spacing:.5px}.center{text-align:center}.status{font-size:22px;font-weight:900;margin:3px}.section{display:none}.section.show{display:block}table{width:100%;border-collapse:collapse;font-size:11px;border:1px solid #dfe3ea}th,td{padding:6px 3px;border:1px solid #e7eaf0;text-align:left}th{background:#f5f6f8}.tablewrap{border:1px solid #dfe3ea;border-radius:10px;overflow:hidden}.statsgrid{display:grid;grid-template-columns:repeat(4,1fr);gap:5px;margin-top:7px}.statbox{background:#f6f7fa;border:1px solid #e5e8ee;border-radius:8px;padding:6px;text-align:center}.note{font-size:10px;color:#888;line-height:1.4}.hash{font-size:9px;word-break:break-all;color:#777;margin-top:6px}@media(max-width:500px){.nums{font-size:20px}.big{font-size:18px}}
</style></head><body><div class="wrap"><div class="tabs"><button id="b1" class="on" onclick="tab(1)">Torn计算</button><button id="b2" onclick="tab(2)">历史统计</button></div>
<section id="s1" class="section show"><div class="card center"><div class="label">当前期</div><div class="status" id="period">-</div><div class="label">平台对应期号开奖结果</div><div class="nums" id="nums">-</div><div class="note" id="block"></div><div class="hash" id="hash"></div></div>
<div class="card"><div class="grid"><div class="box"><div class="label">已完成预判</div><div class="big">-</div></div><div class="box"><div class="label">命中率</div><div class="big">-</div></div><div class="box"><div class="label">命中</div><div class="big">-</div></div><div class="box"><div class="label">未命中</div><div class="big">-</div></div></div></div>
<div class="card"><b>当期20组实际记录</b><div class="note" style="margin-top:5px">版面保留，功能暂不启用。</div><div class="tablewrap" style="margin-top:7px"><table><thead><tr><th>组</th><th>区块</th><th>7个号码</th><th>结果</th></tr></thead><tbody></tbody></table></div><div style="margin-top:9px"><b>当期20组单数统计</b></div><div class="note" style="margin-top:3px">版面保留，统计逻辑已清除。</div><div class="statsgrid"></div></div><div class="card note">每天固定1440期：0001～1440；1440期结束后次日重新从0001开始。</div></section>
<section id="s2" class="section"><div class="card"><b>历史统计独立预判</b><div class="note" style="margin-top:5px">版面保留，历史统计逻辑已清除。</div></div><div class="card"><b>最近60期：单0～单7出现次数</b></div><div class="card"><b>最近60期：单数统计</b></div><div class="card"><b>全部历史开奖记录</b></div></section></div>
<script>function tab(n){document.getElementById('s1').className='section '+(n==1?'show':'');document.getElementById('s2').className='section '+(n==2?'show':'');document.getElementById('b1').className=n==1?'on':'';document.getElementById('b2').className=n==2?'on':''}
async function refresh(){try{const d=await (await fetch('/api/state',{cache:'no-store'})).json();document.getElementById('period').textContent=d.display_period+'期';document.getElementById('block').textContent=d.result?'开奖区块：'+d.result.block:'';document.getElementById('hash').textContent=d.result?'区块哈希：'+d.result.hash:(d.error||'');if(d.result){document.getElementById('nums').textContent=d.result.numbers.slice(0,6).map(n=>String(n).padStart(2,'0')).join('  ')+'  +  '+String(d.result.numbers[6]).padStart(2,'0')}}catch(e){}}refresh();setInterval(refresh,5000);</script></body></html>'''

@app.get('/')
def index(): return render_template_string(PAGE)
@app.get('/api/state')
def api_state(): return jsonify(state())
if __name__ == '__main__': app.run(host='0.0.0.0',port=int(os.getenv('PORT','10000')))
