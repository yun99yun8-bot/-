
import os, re, time, threading
from datetime import datetime, timezone
from collections import Counter
import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

TRONGRID_URL = os.getenv("TRONGRID_URL", "https://api.trongrid.io/wallet/getnowblock")
API_KEY = os.getenv("TRON_PRO_API_KEY", "")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "1"))

state = {
    "latest": None,
    "snapshots": [],
    "unique_blocks": [],
    "minute_key": None,
    "error": None,
}

HTML = r"""
<!doctype html>
<html lang="zh-CN">
<head>
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta charset="utf-8">
<title>TRON 实时区块计算</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,"SF Pro Text",Arial,sans-serif;margin:0;background:#f5f5f7;color:#111}
main{max-width:760px;margin:auto;padding:18px}
.card{background:#fff;border-radius:18px;padding:16px;margin:12px 0;box-shadow:0 1px 5px #0001}
h1{font-size:24px;margin:4px 0 16px}.muted{color:#666;font-size:13px}
.big{font-size:26px;font-weight:700;letter-spacing:2px;word-break:break-all}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.stat{padding:12px;background:#f2f2f7;border-radius:12px}
pre{white-space:pre-wrap;word-break:break-all;font-size:12px}
table{width:100%;border-collapse:collapse;font-size:13px}td,th{padding:7px;border-bottom:1px solid #eee;text-align:left}
</style>
</head>
<body><main>
<h1>TRON 实时区块计算</h1>
<div class="muted">每秒检查一次新区块；相同区块不会重复计入“唯一区块”。</div>

<div class="card">
  <div class="muted">状态</div>
  <div id="status" class="big">连接中…</div>
</div>

<div class="card">
  <div class="grid">
    <div class="stat"><div class="muted">当前区块</div><div id="height">-</div></div>
    <div class="stat"><div class="muted">本分钟快照</div><div id="snapshots">0</div></div>
    <div class="stat"><div class="muted">本分钟唯一区块</div><div id="unique">0</div></div>
    <div class="stat"><div class="muted">本分钟</div><div id="minute">-</div></div>
  </div>
</div>

<div class="card">
  <div class="muted">最新 Hash</div>
  <pre id="hash">-</pre>
</div>

<div class="card">
  <div class="muted">按已保存规则计算</div>
  <div id="result" class="big">-</div>
  <div class="muted" id="detail"></div>
</div>

<div class="card">
  <div class="muted">本分钟号码频率（仅统计已抓到的数据，不代表下一期预测）</div>
  <table><thead><tr><th>号码</th><th>次数</th></tr></thead><tbody id="freq"></tbody></table>
</div>
</main>
<script>
async function refresh(){
  try{
    const r=await fetch('/api/state',{cache:'no-store'}); const d=await r.json();
    document.getElementById('status').textContent=d.error?'接口错误':'运行中';
    document.getElementById('height').textContent=d.latest?.height ?? '-';
    document.getElementById('snapshots').textContent=d.snapshots;
    document.getElementById('unique').textContent=d.unique_blocks;
    document.getElementById('minute').textContent=d.minute_key ?? '-';
    document.getElementById('hash').textContent=d.latest?.hash ?? '-';
    document.getElementById('result').textContent=d.latest?.numbers?.join('、') ?? '-';
    document.getElementById('detail').textContent=d.latest?.detail ?? '';
    const tbody=document.getElementById('freq'); tbody.innerHTML='';
    (d.frequency||[]).forEach(x=>{const tr=document.createElement('tr'); tr.innerHTML=`<td>${x[0]}</td><td>${x[1]}</td>`; tbody.appendChild(tr)});
  }catch(e){document.getElementById('status').textContent='连接中…'}
}
setInterval(refresh,1000); refresh();
</script>
</body></html>
"""

def extract_and_calculate(h):
    # From right to left, take the first 7 A-E letters and first 7 decimal digits.
    letters = [c for c in reversed(h.lower()) if c in "abcde"][:7]
    digits = [c for c in reversed(h) if c.isdigit()][:7]
    if len(letters) < 7 or len(digits) < 7:
        return None
    # Restore left-to-right order among the seven selected characters.
    letters = list(reversed(letters))
    digits = list(reversed(digits))
    mp = {"a":"0","b":"1","c":"2","d":"3","e":"4"}
    nums = [mp[l] + d for l, d in zip(letters, digits)]
    return {
        "letters": "".join(letters).upper(),
        "digits": "".join(digits),
        "numbers": nums,
        "detail": f"字母：{''.join(letters).upper()}；数字：{''.join(digits)}"
    }

def minute_key():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")

def reset_if_needed():
    mk = minute_key()
    if state["minute_key"] != mk:
        state["minute_key"] = mk
        state["snapshots"] = []
        state["unique_blocks"] = []

def fetch_latest():
    headers = {"accept": "application/json"}
    if API_KEY:
        headers["TRON-PRO-API-KEY"] = API_KEY
    r = requests.get(TRONGRID_URL, headers=headers, timeout=8)
    r.raise_for_status()
    data = r.json()
    if "Error" in data:
        raise RuntimeError(data["Error"])
    block = data.get("block_header", {}).get("raw_data", {})
    height = block.get("number")
    block_id = data.get("blockID") or data.get("block_id")
    return height, block_id

def worker():
    last_height = None
    while True:
        try:
            reset_if_needed()
            height, block_id = fetch_latest()
            now = time.time()
            state["snapshots"].append({"ts": now, "height": height})
            # Keep at most the current minute's 60 seconds of snapshots.
            state["snapshots"] = state["snapshots"][-60:]

            if height != last_height and block_id:
                calc = extract_and_calculate(block_id)
                item = {"height": height, "hash": block_id, **(calc or {})}
                state["unique_blocks"].append(item)
                state["unique_blocks"] = state["unique_blocks"][-60:]
                state["latest"] = item
                last_height = height
            state["error"] = None
        except Exception as e:
            state["error"] = str(e)
        time.sleep(POLL_SECONDS)

@app.get("/")
def home():
    return render_template_string(HTML)

@app.get("/api/state")
def api_state():
    nums = []
    for x in state["unique_blocks"]:
        nums.extend(x.get("numbers", []))
    freq = sorted(Counter(nums).items(), key=lambda x:(-x[1], x[0]))
    return jsonify({
        "latest": state["latest"],
        "snapshots": len(state["snapshots"]),
        "unique_blocks": len(state["unique_blocks"]),
        "minute_key": state["minute_key"],
        "frequency": freq,
        "error": state["error"],
    })

if __name__ == "__main__":
    threading.Thread(target=worker, daemon=True).start()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
