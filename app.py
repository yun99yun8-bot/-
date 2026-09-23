import os, threading, time
from collections import Counter
from datetime import datetime, timezone
import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)
TRON_URL = 'https://api.trongrid.io/wallet/getnowblock'
API_KEY = os.getenv('TRON_PRO_API_KEY', '')
POLL_SECONDS = float(os.getenv('POLL_SECONDS', '1'))
HISTORY_LIMIT = 200
lock = threading.Lock()
seen = set()
state = dict(status='启动中', current_block=None, current_hash=None, current_result=None,
             current_letters=None, current_digits=None, current_minute=None,
             minute_snapshots=0, minute_unique_blocks=0, minute_frequency=Counter(),
             history=[], error=None)

def calc(h):
    lr, dr = [], []
    for ch in reversed(h.lower()):
        if ch in 'abcde' and len(lr) < 7: lr.append(ch)
        if ch.isdigit() and len(dr) < 7: dr.append(ch)
        if len(lr) == 7 and len(dr) == 7: break
    if len(lr) < 7 or len(dr) < 7: return None
    letters, digits = lr[::-1], dr[::-1]
    mp = {'a':'0','b':'1','c':'2','d':'3','e':'4'}
    nums = [f'{int(mp[a]+b):02d}' for a,b in zip(letters,digits)]
    return {'numbers': nums, 'letters': ''.join(x.upper() for x in letters), 'digits': ''.join(digits)}

def parity(nums):
    odd = sum(int(n[-1]) % 2 for n in nums)
    even = 7 - odd
    return {'odd': odd, 'even': even, 'label': f'{odd}单{even}双'}

def minute(): return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')

def fetch_block():
    headers = {'TRON-PRO-API-KEY': API_KEY} if API_KEY else {}
    r = requests.get(TRON_URL, headers=headers, timeout=10); r.raise_for_status()
    d = r.json(); raw = d.get('block_header',{}).get('raw_data',{})
    if not d.get('blockID'): raise RuntimeError(f'TRON API 未返回 blockID: {d}')
    return str(raw.get('number','-')), d['blockID']

def add(block, h, c, m):
    p = parity(c['numbers']); key = '|'.join(c['numbers'])
    with lock:
        if h in seen: return
        seen.add(h)
        dup = sum(x['numbers_key'] == key for x in state['history']) + 1
        item = {'time':datetime.now(timezone.utc).strftime('%H:%M:%S'),'minute':m,
                'block':block,'hash':h,'numbers':c['numbers'],'numbers_key':key,
                'letters':c['letters'],'digits':c['digits'], 'odd':p['odd'],'even':p['even'],
                'parity':p['label'],'duplicate_count':dup}
        state['history'].insert(0,item); state['history'] = state['history'][:HISTORY_LIMIT]
        state.update(current_block=block,current_hash=h,current_result=c['numbers'],
                     current_letters=c['letters'],current_digits=c['digits'],current_minute=m,
                     minute_unique_blocks=state['minute_unique_blocks']+1)
        for n in c['numbers']: state['minute_frequency'][n] += 1

def poll():
    with lock: state['status'] = '运行中'
    while True:
        try:
            m = minute()
            with lock:
                if state['current_minute'] != m:
                    state['current_minute']=m; state['minute_snapshots']=0; state['minute_unique_blocks']=0; state['minute_frequency']=Counter()
            block,h=fetch_block()
            with lock: state['minute_snapshots'] += 1; state['error']=None
            c=calc(h)
            if c: add(block,h,c,m)
        except Exception as e:
            with lock: state['error']=str(e)
        time.sleep(POLL_SECONDS)

HTML = '''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>TRON 实时区块计算</title><style>
*{box-sizing:border-box}body{margin:0;background:#f5f5f7;color:#111;font-family:-apple-system,BlinkMacSystemFont,"PingFang SC",Arial,sans-serif}.wrap{max-width:900px;margin:auto;padding:20px 14px 50px}h1{font-size:30px;margin:10px 0 8px}.sub,.meta,.small{color:#666}.card{background:#fff;border-radius:22px;padding:20px;margin:14px 0;box-shadow:0 2px 12px #0001}.label{color:#666;font-size:15px}.big{font-size:36px;font-weight:800;margin-top:5px}.grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.box{background:#f1f1f5;border-radius:18px;padding:16px}.value{font-size:25px;font-weight:700;margin-top:5px}.hash,.small{word-break:break-all}.hash{font-family:monospace;line-height:1.45}.nums{font-size:29px;font-weight:800;line-height:1.5}.badge{display:inline-block;padding:6px 10px;border-radius:10px;background:#eee;margin:3px 5px 3px 0;font-weight:700}.parity{background:#eef5ff}.dup{background:#fff3d8}.ok{color:#138a42}.err{color:#c33}table{width:100%;border-collapse:collapse;font-size:14px}th,td{text-align:left;padding:11px 7px;border-bottom:1px solid #eee;vertical-align:top}th{color:#666;position:sticky;top:0;background:#fff}.numline{font-weight:800;font-size:18px;line-height:1.45}.controls{display:flex;gap:8px;flex-wrap:wrap}button{border:0;border-radius:12px;padding:10px 14px;background:#eee;font-weight:700}button.active{background:#111;color:#fff}@media(max-width:650px){.nums{font-size:25px}table{font-size:13px}th:nth-child(4),td:nth-child(4){display:none}}
</style></head><body><div class="wrap"><h1>TRON 实时区块计算</h1><div class="sub">每秒检查一次新区块；相同区块不会重复计入。历史记录包含每组号码及尾数单双。</div>
<div class="card"><div class="label">状态</div><div id="status" class="big">加载中</div><div id="error" class="meta err"></div></div>
<div class="grid"><div class="box"><div class="label">当前区块</div><div id="block" class="value">-</div></div><div class="box"><div class="label">本分钟快照</div><div id="snapshots" class="value">0</div></div><div class="box"><div class="label">本分钟唯一区块</div><div id="unique" class="value">0</div></div><div class="box"><div class="label">本分钟</div><div id="minute" class="value">-</div></div></div>
<div class="card"><div class="label">最新 Hash</div><div id="hash" class="hash">-</div></div><div class="card"><div class="label">最新一组计算</div><div id="latestNums" class="nums">-</div><div id="latestParity" class="meta"></div><div id="latestMeta" class="meta"></div></div>
<div class="card"><div class="label">本分钟号码频率（仅统计已抓取数据，不代表下一期预测）</div><div id="freq" class="meta">-</div></div>
<div class="card"><div style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap"><div><div class="label">历史记录</div><div class="meta">每一组都标记尾数单双；相同的完整七号码组会显示重复次数。</div></div><div class="controls"><button id="allBtn" class="active" onclick="setFilter('all')">全部</button><button id="dupBtn" onclick="setFilter('dup')">只看重复组</button></div></div><div style="overflow:auto;margin-top:12px"><table><thead><tr><th>时间/区块</th><th>7个号码</th><th>尾数单双</th><th>重复</th><th>Hash</th></tr></thead><tbody id="history"></tbody></table></div></div></div>
<script>let filter='all';function esc(s){return String(s??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]) )}function setFilter(f){filter=f;document.getElementById('allBtn').classList.toggle('active',f==='all');document.getElementById('dupBtn').classList.toggle('active',f==='dup');load()}function render(d){document.getElementById('status').innerHTML=d.status==='运行中'?'<span class="ok">运行中</span>':esc(d.status);document.getElementById('error').textContent=d.error?'错误：'+d.error:'';document.getElementById('block').textContent=d.current_block??'-';document.getElementById('snapshots').textContent=d.minute_snapshots??0;document.getElementById('unique').textContent=d.minute_unique_blocks??0;document.getElementById('minute').textContent=d.current_minute??'-';document.getElementById('hash').textContent=d.current_hash??'-';if(d.current_result){document.getElementById('latestNums').textContent=d.current_result.join('、');let p=d.current_parity;document.getElementById('latestParity').innerHTML='<span class="badge parity">尾数：'+esc(p.label)+'</span><span class="badge">单 '+p.odd+'</span><span class="badge">双 '+p.even+'</span>';document.getElementById('latestMeta').textContent='字母：'+d.current_letters+'；数字：'+d.current_digits}else{document.getElementById('latestNums').textContent='-';document.getElementById('latestParity').textContent='';document.getElementById('latestMeta').textContent=''}let f='';Object.entries(d.minute_frequency||{}).sort((a,b)=>Number(a[0])-Number(b[0])).forEach(([n,c])=>f+='<span class="badge">'+esc(n)+' × '+c+'</span>');document.getElementById('freq').innerHTML=f||'-';let rows=(d.history||[]).filter(x=>filter==='all'||x.duplicate_count>1);document.getElementById('history').innerHTML=rows.map(x=>'<tr><td>'+esc(x.time)+'<br><span class="small">区块 '+esc(x.block)+'</span></td><td><div class="numline">'+esc(x.numbers.join('、'))+'</div><span class="small">'+esc(x.minute)+'</span></td><td><span class="badge parity">'+esc(x.parity)+'</span><br><span class="small">单'+x.odd+' / 双'+x.even+'</span></td><td>'+(x.duplicate_count>1?'<span class="badge dup">第 '+x.duplicate_count+' 次</span>':'—')+'</td><td><span class="small">'+esc(x.hash)+'</span></td></tr>').join('')||'<tr><td colspan="5">暂无记录</td></tr>'}async function load(){try{let r=await fetch('/api/state',{cache:'no-store'});render(await r.json())}catch(e){document.getElementById('status').textContent='页面连接中'}}load();setInterval(load,1000);</script></body></html>'''

@app.route('/')
def index(): return render_template_string(HTML)
@app.route('/api/state')
def api_state():
    with lock:
        d=dict(state); d['minute_frequency']=dict(state['minute_frequency']); d['history']=list(state['history']); d['current_parity']=parity(state['current_result']) if state['current_result'] else None
    return jsonify(d)

if __name__ == '__main__':
    threading.Thread(target=poll, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.getenv('PORT','10000')), debug=False)
