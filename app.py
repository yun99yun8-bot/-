import os
import time
import threading
from collections import Counter, deque
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

TRON_URL = "https://api.trongrid.io/wallet/getnowblock"
API_KEY = os.getenv("TRON_PRO_API_KEY", "")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "1"))
HISTORY_LIMIT = int(os.getenv("HISTORY_LIMIT", "5000"))

state_lock = threading.Lock()
latest = {
    "block": None, "hash": None, "numbers": None, "letters": None,
    "digits": None, "minute": None, "updated": None
}
history = deque(maxlen=HISTORY_LIMIT)
seen_blocks = set()


def calculate_hash(h):
    """按既定规则：从右向左取前7个A-E和前7个数字，再恢复顺序并配对。"""
    h = (h or "").lower()
    letters, digits = [], []

    for ch in reversed(h):
        if ch in "abcde" and len(letters) < 7:
            letters.append(ch)
        if ch.isdigit() and len(digits) < 7:
            digits.append(ch)
        if len(letters) == 7 and len(digits) == 7:
            break

    if len(letters) < 7 or len(digits) < 7:
        return None

    letters.reverse()
    digits.reverse()
    mp = {"a": 0, "b": 1, "c": 2, "d": 3, "e": 4}
    nums = [mp[a] * 10 + int(d) for a, d in zip(letters, digits)]

    return {
        "numbers": nums,
        "letters": "".join(letters).upper(),
        "digits": "".join(digits),
    }


def parity(numbers):
    odd = sum(n % 2 for n in numbers)
    return odd, len(numbers) - odd


def tail_parity(numbers):
    """尾数单双：取每个号码个位数判断单双。"""
    odd = sum((n % 10) % 2 for n in numbers)
    return odd, len(numbers) - odd


def add_record(block, block_hash, calc, block_time):
    if not calc or block is None:
        return

    block_key = str(block)

    with state_lock:
        if block_key in seen_blocks:
            return
        seen_blocks.add(block_key)

        try:
            dt = datetime.fromtimestamp(
                int(block_time) / 1000, tz=timezone.utc
            )
        except Exception:
            dt = datetime.now(timezone.utc)

        nums = calc["numbers"]
        odd, even = parity(nums)
        tail_odd, tail_even = tail_parity(nums)

        rec = {
            "block": block,
            "hash": block_hash,
            "numbers": nums,
            "letters": calc["letters"],
            "digits": calc["digits"],
            "minute": dt.strftime("%Y-%m-%d %H:%M"),
            "time": dt.strftime("%H:%M:%S"),
            "odd": odd,
            "even": even,
            "tail_odd": tail_odd,
            "tail_even": tail_even,
        }

        history.appendleft(rec)

        latest.update({
            "block": block,
            "hash": block_hash,
            "numbers": nums,
            "letters": calc["letters"],
            "digits": calc["digits"],
            "minute": rec["minute"],
            "updated": datetime.now(timezone.utc).isoformat(),
            "odd": odd,
            "even": even,
            "tail_odd": tail_odd,
            "tail_even": tail_even,
        })


def fetch_block():
    headers = {}
    if API_KEY:
        headers["TRON-PRO-API-KEY"] = API_KEY

    response = requests.get(TRON_URL, headers=headers, timeout=8)
    response.raise_for_status()
    return response.json()


def monitor():
    while True:
        try:
            data = fetch_block()
            block_hash = data.get("blockID")
            raw = data.get("block_header", {}).get("raw_data", {})

            if block_hash and raw.get("number") is not None:
                calc = calculate_hash(block_hash)
                add_record(
                    raw.get("number"),
                    block_hash,
                    calc,
                    raw.get("timestamp")
                )
        except Exception as exc:
            print("monitor error:", repr(exc), flush=True)

        time.sleep(POLL_SECONDS)


PAGE = r"""
<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>TRON 数据统计</title>
<style>
*{box-sizing:border-box}
body{
  margin:0;background:#f5f6fa;color:#20242b;
  font-family:-apple-system,BlinkMacSystemFont,"PingFang SC",
  "Microsoft YaHei",Arial,sans-serif;font-size:13px
}
.wrap{max-width:760px;margin:0 auto;padding:5px}
.card{
  background:#fff;border-radius:12px;margin-bottom:6px;padding:8px;
  box-shadow:0 2px 8px rgba(0,0,0,.045)
}
.current{display:grid;grid-template-columns:1fr 1fr;gap:5px}
.item{background:#f4f5f8;border-radius:9px;padding:7px}
.label{font-size:11px;color:#7b8190}
.value{font-size:17px;font-weight:700;margin-top:1px;word-break:break-all}
.numbers{font-size:22px;line-height:1.2;font-weight:800;letter-spacing:.5px}
.badge{
  display:inline-block;padding:3px 7px;border-radius:7px;
  background:#eef2ff;margin:3px 3px 0 0;font-weight:700
}
.ai{border:1px solid #e4e7ef}
.ai-title{font-size:16px;font-weight:800;margin-bottom:5px}
.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:4px}
.stat{background:#f6f7fa;border-radius:8px;padding:6px;text-align:center}
.stat b{font-size:15px}
.stat span{display:block;color:#777;font-size:10px}
.history-title{font-size:15px;font-weight:800;margin-bottom:4px}
.table{width:100%;border-collapse:collapse;font-size:11px}
.table th,.table td{
  border-bottom:1px solid #eceef3;padding:5px 2px;
  text-align:left;vertical-align:middle
}
.table th{color:#777;font-weight:600}
.recnums{font-weight:700;letter-spacing:.15px;white-space:nowrap}
.empty{text-align:center;color:#999;padding:16px}
.note{font-size:10px;color:#888;margin-top:4px}
.smallhash{font-size:10px;word-break:break-all;margin-top:3px;color:#333}
@media(max-width:500px){
  body{font-size:12px}
  .numbers{font-size:20px}
  .value{font-size:16px}
  .table{font-size:10px}
  .table th,.table td{padding:4px 1px}
}
</style>
</head>
<body>
<div class="wrap">

<div class="card">
  <div class="current">
    <div class="item">
      <div class="label">当前区块</div>
      <div class="value" id="block">-</div>
    </div>
    <div class="item">
      <div class="label">本分钟已抓取</div>
      <div class="value" id="minuteCount">0</div>
    </div>
    <div class="item">
      <div class="label">当前分钟</div>
      <div class="value" id="minute">-</div>
    </div>
    <div class="item">
      <div class="label">最新时间</div>
      <div class="value" id="time">-</div>
    </div>
  </div>
</div>

<div class="card">
  <div class="label">最新 Hash</div>
  <div class="smallhash" id="hash">-</div>
</div>

<div class="card">
  <div class="label">最新一组号码</div>
  <div class="numbers" id="numbers">-</div>
  <div id="latestParity"></div>
</div>

<div class="card ai">
  <div class="ai-title">最近60期单双统计</div>
  <div class="grid" id="summary"></div>

  <div style="margin-top:6px">
    <b>统计倾向：</b>
    <span id="suggestion">数据不足</span>
    <span class="badge" id="strength">-</span>
  </div>

  <div class="note">
    统计倾向仅根据历史数据计算，不代表下一期结果，也不是投注保证。
  </div>
</div>

<div class="card">
  <div class="history-title">最近60期历史记录</div>
  <div style="overflow-x:auto">
    <table class="table">
      <thead>
        <tr>
          <th>时间</th>
          <th>区块</th>
          <th>7个号码</th>
          <th>单双</th>
          <th>尾数</th>
        </tr>
      </thead>
      <tbody id="history"></tbody>
    </table>
  </div>
</div>

<div class="card">
  <div class="history-title">全部已抓取历史单双统计</div>
  <div class="grid" id="allSummary"></div>
</div>

</div>

<script>
function esc(s){
  return String(s ?? '').replace(/[&<>"']/g,m=>({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  }[m]));
}

function badge(text){
  return `<span class="badge">${esc(text)}</span>`;
}

async function refresh(){
  try{
    const response = await fetch('/api/state',{cache:'no-store'});
    const d = await response.json();
    const l = d.latest || {};
    const rows = d.history || [];

    document.getElementById('block').textContent = l.block ?? '-';
    document.getElementById('minute').textContent = l.minute ?? '-';
    document.getElementById('time').textContent =
      l.updated ? new Date(l.updated).toLocaleTimeString() : '-';
    document.getElementById('hash').textContent = l.hash ?? '-';

    document.getElementById('numbers').textContent =
      (l.numbers || []).map(x=>String(x).padStart(2,'0')).join('、') || '-';

    if(l.numbers){
      document.getElementById('latestParity').innerHTML =
        badge(`${l.odd}单${l.even}双`) +
        badge(`尾${l.tail_odd}单${l.tail_even}双`);
    }else{
      document.getElementById('latestParity').innerHTML = '';
    }

    const minuteCount = rows.filter(x=>x.minute === l.minute).length;
    document.getElementById('minuteCount').textContent = minuteCount;

    // 最近60期组合统计
    const counts = {};
    rows.slice(0,60).forEach(x=>{
      const k = `${x.odd}单${x.even}双`;
      counts[k] = (counts[k] || 0) + 1;
    });

    const entries = Object.entries(counts)
      .sort((a,b)=>b[1]-a[1]);

    document.getElementById('summary').innerHTML =
      entries.slice(0,6).map(([k,v])=>
        `<div class="stat"><b>${esc(k)}</b><span>${v}期</span></div>`
      ).join('') || '<div class="empty">暂无数据</div>';

    if(entries.length){
      document.getElementById('suggestion').textContent = entries[0][0];
      let strength = '弱';
      if(entries[0][1] >= 20) strength = '较强';
      else if(entries[0][1] >= 14) strength = '中';
      document.getElementById('strength').textContent = strength;
    }else{
      document.getElementById('suggestion').textContent = '数据不足';
      document.getElementById('strength').textContent = '-';
    }

    // 全部已抓取历史统计
    const all = {};
    rows.forEach(x=>{
      const k = `${x.odd}单${x.even}双`;
      all[k] = (all[k] || 0) + 1;
    });

    const allEntries = Object.entries(all)
      .sort((a,b)=>b[1]-a[1]);

    document.getElementById('allSummary').innerHTML =
      allEntries.slice(0,9).map(([k,v])=>
        `<div class="stat"><b>${esc(k)}</b><span>${v}期</span></div>`
      ).join('') || '<div class="empty">暂无数据</div>';

    document.getElementById('history').innerHTML =
      rows.slice(0,60).map(x=>{
        const nums = (x.numbers || [])
          .map(n=>String(n).padStart(2,'0')).join(' ');

        return `<tr>
          <td>${esc(x.time)}</td>
          <td>${esc(x.block)}</td>
          <td class="recnums">${esc(nums)}</td>
          <td>${esc(x.odd)}单${esc(x.even)}双</td>
          <td>${esc(x.tail_odd)}单${esc(x.tail_even)}双</td>
        </tr>`;
      }).join('') ||
      '<tr><td colspan="5" class="empty">等待新区块数据...</td></tr>';

  }catch(e){}
}

refresh();
setInterval(refresh,1000);
</script>
</body>
</html>
"""


@app.get("/")
def index():
    return render_template_string(PAGE)


@app.get("/api/state")
def api_state():
    with state_lock:
        return jsonify({
            "latest": dict(latest),
            "history": list(history),
            "history_count": len(history)
        })


if __name__ == "__main__":
    threading.Thread(target=monitor, daemon=True).start()
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
