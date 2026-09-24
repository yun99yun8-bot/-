import os
import time
import threading
import sqlite3
from datetime import datetime, timezone, timedelta
from collections import Counter

import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

TRON_URL = "https://api.trongrid.io/wallet/getnowblock"
TRON_BLOCK_URL = "https://api.trongrid.io/wallet/getblockbynum"
API_KEY = os.getenv("TRON_PRO_API_KEY", "")
DB_PATH = os.getenv("DB_PATH", "tron_history.db")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "1"))
HISTORY_PATTERN_LEN = max(2, int(os.getenv("HISTORY_PATTERN_LEN", "2")))
DEFAULT_ANCHOR_DATE = "2026-09-24"
DEFAULT_ANCHOR_BLOCK = int(os.getenv("BLOCK_ANCHOR", "86502288"))

lock = threading.Lock()
latest = {}
last_seen_block = None


def db():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


with lock:
    c = db()
    c.execute("""CREATE TABLE IF NOT EXISTS records(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        period TEXT,
        period_index INTEGER,
        block INTEGER UNIQUE,
        hash TEXT,
        numbers TEXT,
        letters TEXT,
        digits TEXT,
        ts TEXT,
        odd INTEGER,
        even INTEGER,
        tail_odd INTEGER,
        tail_even INTEGER
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS minute_blocks(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        day TEXT NOT NULL,
        period_index INTEGER NOT NULL,
        position INTEGER NOT NULL,
        block INTEGER NOT NULL,
        hash TEXT NOT NULL,
        odd INTEGER,
        even INTEGER,
        numbers TEXT,
        ts TEXT,
        UNIQUE(day,period_index,position),
        UNIQUE(block)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS king_predictions(
        day TEXT NOT NULL,
        period_index INTEGER NOT NULL,
        period TEXT NOT NULL,
        prediction TEXT,
        predicted_at TEXT,
        actual TEXT,
        hit INTEGER,
        resolved_at TEXT,
        PRIMARY KEY(day,period_index)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS anchors(
        day TEXT PRIMARY KEY,
        anchor_block INTEGER
    )""")
    c.execute("INSERT OR IGNORE INTO anchors(day,anchor_block) VALUES(?,?)",
              (DEFAULT_ANCHOR_DATE, DEFAULT_ANCHOR_BLOCK))
    c.commit()
    c.close()


def get_anchor(day):
    with lock:
        c = db(); row = c.execute("SELECT anchor_block FROM anchors WHERE day=?", (day,)).fetchone(); c.close()
    return int(row["anchor_block"]) if row else None


def set_anchor(day, block):
    with lock:
        c = db(); c.execute("INSERT OR REPLACE INTO anchors(day,anchor_block) VALUES(?,?)", (day,int(block))); c.commit(); c.close()


def local_now():
    now = datetime.now(timezone.utc)
    try:
        from zoneinfo import ZoneInfo
        return now.astimezone(ZoneInfo(os.getenv("APP_TIMEZONE", "Asia/Shanghai")))
    except Exception:
        return now + timedelta(hours=8)


def period_info(dt):
    # 平台每天0001～1440期；期号后四位与当天分钟序号一致：00:01=0001，07:28=0448。
    idx = dt.hour * 60 + dt.minute
    idx = max(1, idx)
    period = dt.strftime("%y%m%d") + f"{idx:04d}"
    return period, idx


def period_day(period):
    p = str(period or "")
    return f"20{p[:2]}-{p[2:4]}-{p[4:6]}" if len(p) >= 6 and p[:6].isdigit() else ""


def calculate_numbers(block_hash):
    h = (block_hash or "").lower()
    letters = [ch for ch in reversed(h) if ch in "abcde"]
    digits = [ch for ch in reversed(h) if ch.isdigit()]
    mp = {"a":0,"b":1,"c":2,"d":3,"e":4}
    numbers=[]; used=set(); used_pairs=set(); i=0
    while len(numbers)<7 and i<min(len(letters),len(digits)):
        pair=(letters[i],digits[i]); n=mp[pair[0]]*10+int(pair[1]); i+=1
        if n==0 or n<1 or n>49 or pair in used_pairs or n in used: continue
        used_pairs.add(pair); used.add(n); numbers.append(n)
    if len(numbers)<7: return None
    odd=sum(n%2 for n in numbers); tail_odd=sum((n%10)%2 for n in numbers)
    return {"numbers":numbers,"letters":"".join(letters[:i]).upper(),"digits":"".join(digits[:i]),
            "odd":odd,"even":7-odd,"tail_odd":tail_odd,"tail_even":7-tail_odd}


def fetch_block_by_number(number):
    headers={"TRON-PRO-API-KEY":API_KEY} if API_KEY else {}
    r=requests.post(TRON_BLOCK_URL,headers=headers,json={"num":int(number)},timeout=8)
    r.raise_for_status(); return r.json()


def target_block_for_datetime(dt):
    day=dt.strftime("%Y-%m-%d"); idx=dt.hour*60+dt.minute
    anchor=get_anchor(day)
    if anchor is None:
        with lock: current=latest.get("chain_block")
        if current is None: return None,False
        # anchor定义为当天00:00对应的第一个分钟槽基准；0001期目标块=anchor+20。
        anchor=int(current)-idx*20
        set_anchor(day,anchor)
        return anchor+idx*20,False
    return anchor+idx*20,True


def current_position(target, chain_block):
    if target is None or chain_block is None: return 0
    return max(0,min(20,int(chain_block)-int(target)+20))


def save_minute_block(day, idx, pos, block, block_hash, calc, ts):
    with lock:
        c=db(); c.execute("""INSERT OR IGNORE INTO minute_blocks(day,period_index,position,block,hash,odd,even,numbers,ts)
             VALUES(?,?,?,?,?,?,?,?,?)""",(day,idx,pos,int(block),block_hash,calc["odd"],calc["even"],",".join(map(str,calc["numbers"])),ts)); c.commit(); c.close()


def get_minute_blocks(day, idx, upto=None):
    q="SELECT * FROM minute_blocks WHERE day=? AND period_index=?"
    args=[day,idx]
    if upto is not None: q+=" AND position<=?"; args.append(int(upto))
    q+=" ORDER BY position"
    with lock:
        c=db(); rows=c.execute(q,args).fetchall(); c.close()
    return rows


def save_record(period, idx, block, block_hash, calc, ts):
    with lock:
        c=db(); c.execute("""INSERT OR IGNORE INTO records(period,period_index,block,hash,numbers,letters,digits,ts,odd,even,tail_odd,tail_even)
             VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",(period,idx,int(block),block_hash,",".join(map(str,calc["numbers"])),calc["letters"],calc["digits"],ts,calc["odd"],calc["even"],calc["tail_odd"],calc["tail_even"])); c.commit(); c.close()


def king_prediction(day, idx, period, rows):
    if len(rows)<17: return None
    with lock:
        c=db(); existing=c.execute("SELECT prediction FROM king_predictions WHERE day=? AND period_index=?",(day,idx)).fetchone()
        if existing and existing["prediction"]: c.close(); return existing["prediction"]
    counts=Counter(f"{r['odd']}单{r['even']}双" for r in rows[:17])
    if not counts: return None
    mx=max(counts.values()); candidates=[k for k,v in counts.items() if v==mx]
    pred=candidates[0]
    if len(candidates)>1:
        for r in reversed(rows[:17]):
            k=f"{r['odd']}单{r['even']}双"
            if k in candidates: pred=k; break
    with lock:
        c=db(); c.execute("""INSERT OR IGNORE INTO king_predictions(day,period_index,period,prediction,predicted_at)
             VALUES(?,?,?,?,?)""",(day,idx,period,pred,datetime.now(timezone.utc).isoformat())); c.commit(); c.close()
    return pred


def resolve_king(day,idx,period,rows):
    if len(rows)<20: return
    pred=king_prediction(day,idx,period,rows)
    if not pred: return
    actual=f"{rows[19]['odd']}单{rows[19]['even']}双"; hit=int(pred==actual)
    with lock:
        c=db(); c.execute("UPDATE king_predictions SET actual=?,hit=?,resolved_at=? WHERE day=? AND period_index=? AND actual IS NULL",(actual,hit,datetime.now(timezone.utc).isoformat(),day,idx)); c.commit(); c.close()


def process_current_minute():
    dt=local_now(); period,idx=period_info(dt); day=dt.strftime("%Y-%m-%d")
    target,confirmed=target_block_for_datetime(dt)
    with lock: chain=latest.get("chain_block")
    if target is None or chain is None: return
    pos=current_position(target,chain)
    # 只读取当前分钟已经产生的区块：position 1对应target-19，position20对应target。
    existing={r["position"] for r in get_minute_blocks(day,idx)}
    for p in range(1,pos+1):
        if p in existing: continue
        block=target-20+p
        try: data=fetch_block_by_number(block)
        except Exception as ex:
            print("minute block fetch error",block,repr(ex),flush=True); continue
        raw=data.get("block_header",{}).get("raw_data",{}); bh=data.get("blockID"); actual=raw.get("number")
        if not bh or actual is None or int(actual)!=block: continue
        calc=calculate_numbers(bh)
        if not calc: continue
        ts=datetime.fromtimestamp(int(raw.get("timestamp",int(time.time()*1000)))/1000,tz=timezone.utc).isoformat()
        save_minute_block(day,idx,p,block,bh,calc,ts)
    rows=get_minute_blocks(day,idx,20)
    pred=king_prediction(day,idx,period,rows) if len(rows)>=17 else None
    if len(rows)>=20:
        # 正式开奖记录只保存第20组，避免把20个内部区块误当成20个开奖期。
        r=rows[19]
        nums=list(map(int,r["numbers"].split(",")))
        calc={"numbers":nums,"odd":r["odd"],"even":r["even"],"tail_odd":sum((n%10)%2 for n in nums),"tail_even":0,"letters":"","digits":""}
        calc["tail_even"]=7-calc["tail_odd"]
        save_record(period,idx,r["block"],r["hash"],calc,r["ts"])
        resolve_king(day,idx,period,rows)
    latest.update({"period":period,"block":target,"target_block":target,"chain_block":chain,"progress":pos,"hash":rows[-1]["hash"] if rows else None,
                   "numbers":list(map(int,rows[-1]["numbers"].split(","))) if rows else None,
                   "odd":rows[-1]["odd"] if rows else None,"even":rows[-1]["even"] if rows else None,"confirmed_anchor":confirmed,
                   "updated":datetime.now(timezone.utc).isoformat()})


def monitor():
    global last_seen_block
    while True:
        try:
            headers={"TRON-PRO-API-KEY":API_KEY} if API_KEY else {}
            response=requests.get(TRON_URL,headers=headers,timeout=8); response.raise_for_status(); data=response.json()
            raw=data.get("block_header",{}).get("raw_data",{}); bh=data.get("blockID"); bn=raw.get("number")
            if bn is not None: latest["chain_block"]=int(bn); latest["chain_hash"]=bh
            if bn is not None and bn!=last_seen_block: last_seen_block=bn
            process_current_minute()
        except Exception as ex: print("monitor error",repr(ex),flush=True)
        time.sleep(POLL_SECONDS)


def historical_prediction(all_rows, target_period):
    """独立历史模式：默认用最近2期的单双个数序列，寻找历史相同序列后的下一期分布。"""
    completed=[r for r in all_rows if r["period"] < target_period]
    n=HISTORY_PATTERN_LEN
    if len(completed)<=n: return None
    latest_seq=tuple(int(r["odd"]) for r in completed[-n:])
    counts=Counter()
    matches=[]
    for i in range(n,len(completed)):
        seq=tuple(int(completed[j]["odd"]) for j in range(i-n,i))
        if seq==latest_seq:
            nxt=int(completed[i]["odd"]); counts[nxt]+=1; matches.append(completed[i]["period"])
    total=sum(counts.values())
    probs={str(i):round((counts[i]*100/total),2) if total else 0 for i in range(1,8)}
    return {"pattern":[f"单{x}" for x in latest_seq],"pattern_len":n,"matches":total,"probabilities":probs,
            "match_periods":matches[-20:],"target_period":target_period,"basis":"历史正式开奖数据"}


def state():
    dt=local_now(); period,idx=period_info(dt); day=dt.strftime("%Y-%m-%d")
    target,confirmed=target_block_for_datetime(dt)
    with lock:
        c=db();
        all_rows=c.execute("SELECT * FROM records ORDER BY id ASC").fetchall()
        kp=c.execute("SELECT * FROM king_predictions WHERE day=? AND period_index=?",(day,idx)).fetchone()
        done=c.execute("SELECT COUNT(*) n,COALESCE(SUM(hit),0) h FROM king_predictions WHERE actual IS NOT NULL").fetchone()
        cycles=c.execute("SELECT * FROM king_predictions WHERE actual IS NOT NULL ORDER BY day DESC,period_index DESC LIMIT 60").fetchall()
        c.close()
    chain=latest.get("chain_block"); progress=current_position(target,chain)
    pred=kp["prediction"] if kp else (king_prediction(day,idx,period,get_minute_blocks(day,idx,17)) if progress>=17 else None)
    actual=kp["actual"] if kp else None
    current_rows=get_minute_blocks(day,idx,20)
    actual_numbers=None
    actual_odd=None
    actual_even=None
    actual_block=None
    actual_hash=None
    if len(current_rows)>=20:
        r20=current_rows[19]
        actual_numbers=list(map(int,r20["numbers"].split(",")))
        actual_odd=int(r20["odd"]); actual_even=int(r20["even"])
        actual_block=int(r20["block"]); actual_hash=r20["hash"]
    recent60=all_rows[-60:]
    single_counts={str(i):sum(1 for r in recent60 if int(r["odd"])==i) for i in range(1,8)}
    combo_counts=Counter(f"{r['odd']}单{r['even']}双" for r in recent60)
    histpred=historical_prediction(all_rows,period)
    latest_copy=dict(latest); latest_copy.update({"target_block":target,"chain_block":chain,"progress":progress})
    return {"latest":latest_copy,"day":day,"current_time":day,"period_index":idx,"display_period":f"{idx:04d}",
            "progress":progress,"prediction":pred,"actual":actual,"actual_numbers":actual_numbers,"actual_odd":actual_odd,"actual_even":actual_even,"actual_block":actual_block,"actual_hash":actual_hash,"prediction_period":period,
            "prediction_total":int(done["n"] or 0),"hits":int(done["h"] or 0),"misses":int(done["n"] or 0)-int(done["h"] or 0),
            "hit_rate":round(int(done["h"] or 0)*100/int(done["n"]),1) if int(done["n"] or 0) else 0,
            "single_counts":single_counts,"combo_counts":dict(combo_counts.most_common()),
            "king_history":[dict(x) for x in cycles],"history":[dict(x) for x in recent60[::-1]],"history_all":[dict(x) for x in all_rows[::-1]],
            "history_count":len(all_rows),"historical_prediction":histpred}


PAGE = r"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>TRON统计</title>
<style>
*{box-sizing:border-box}
body{margin:0;background:#f4f5f8;color:#222;font:13px -apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif}
.wrap{max-width:760px;margin:auto;padding:6px}
.tabs{display:flex;gap:5px;position:sticky;top:0;background:#f4f5f8;padding:4px 0;z-index:5}
button{flex:1;border:0;border-radius:10px;padding:10px;font-weight:800;background:#ddd;color:#555}
button.on{background:#222;color:#fff}
.card{background:#fff;border-radius:12px;padding:9px;margin:6px 0;box-shadow:0 2px 8px #0000000b}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:5px}
.box{background:#f6f7fa;border-radius:9px;padding:8px}
.label{color:#7d8390;font-size:11px}
.big{font-size:20px;font-weight:800}
.nums{font-size:23px;font-weight:900;margin-top:7px;letter-spacing:.5px}
.center{text-align:center}
.status{font-size:22px;font-weight:900;margin:3px}
.badge{display:inline-block;padding:4px 7px;background:#eef2ff;border-radius:7px;margin:2px;font-weight:700}
.section{display:none}
.section.show{display:block}
table{width:100%;border-collapse:collapse;font-size:11px}
th,td{padding:5px 2px;border-bottom:1px solid #eee;text-align:left;vertical-align:middle}
.note{font-size:10px;color:#888;line-height:1.4}
.hash{font-size:10px;word-break:break-all;color:#555}
.statrow{display:flex;justify-content:space-between;border-bottom:1px solid #eee;padding:5px 0}
@media(max-width:500px){
 .nums{font-size:20px}.big{font-size:18px}table{font-size:10px}
 th,td{padding:4px 1px}
}
</style>
</head>
<body>
<div class="wrap">

<div class="tabs">
<button id="b1" class="on" onclick="tab(1)">王者归来</button>
<button id="b2" onclick="tab(2)">历史统计</button>
</div>

<section id="s1" class="section show">

<div class="card">
<div class="box"><div class="label">当前时间</div><div class="big" id="currentTime">-</div></div>
<div class="box"><div class="label">当天期数（1～1440）</div><div class="big" id="period">-</div></div>
<div class="box"><div class="label">20组进度</div><div class="big" id="pos">-</div></div>
<div class="box"><div class="label">开奖区块</div><div class="big" id="block">-</div></div>
<div class="box"><div class="label">链上当前区块</div><div class="big" id="chain">-</div></div>
</div>
</div>

<div class="card center">
<div class="label" id="cycleLabel">当前20组状态</div>
<div class="status" id="status">等待中</div>

<div class="label">第17组统计预判第20组</div>
<div class="status" id="pred">等待第17组</div>

<div class="label">第20组实际开奖结果</div>
<div class="status" id="actual">等待第20组</div>

<div class="label">第20组出的7个号码</div>
<div class="nums" id="nums">-</div>
<div id="parity"></div>
<div class="hash" id="hash"></div>
</div>

<div class="card">
<div class="grid">
<div class="box"><div class="label">已完成预判</div><div class="big" id="total">0</div></div>
<div class="box"><div class="label">命中率</div><div class="big" id="rate">0%</div></div>
<div class="box"><div class="label">命中</div><div class="big" id="hits">0</div></div>
<div class="box"><div class="label">未命中</div><div class="big" id="miss">0</div></div>
</div>
</div>

<div class="card">
<b>最近60期预判记录</b>
<table>
<thead><tr><th>期号</th><th>预判</th><th>实际</th><th>结果</th></tr></thead>
<tbody id="cycles"></tbody>
</table>
</div>

<div class="card note">
每天固定1440期：0001～1440；1440期结束后次日重新从0001开始。每一期内部有20组TRON区块进度，17/20时生成第20组的统计预判，20/20结算。下一分钟进入下一期，20组进度重新从1/20开始。
</div>
</section>

<section id="s2" class="section">

<div class="card">
<b>历史统计独立预判</b>
<div class="note" style="margin-top:5px">只读取已经完成的历史正式开奖数据，与「王者归来」算法完全独立。</div>
<div class="grid" style="margin-top:7px">
<div class="box"><div class="label">预判期</div><div class="big" id="histPredPeriod">-</div></div>
<div class="box"><div class="label">历史匹配次数</div><div class="big" id="histMatches">0</div></div>
</div>
<div class="box" style="margin-top:6px"><div class="label">当前历史模式</div><div class="big" id="histPattern">-</div></div>
<div style="margin-top:7px" id="probabilities"></div>
<div class="note" id="histBasis" style="margin-top:6px"></div>
</div>

<div class="card">
<b>最近60期：1～7单出现次数</b>
<div id="singleCounts" style="margin-top:5px"></div>
</div>

<div class="card">
<b>最近60期单双组合</b>
<div id="comboStats" style="margin-top:5px"></div>
</div>

<div class="card">
<b>全部历史开奖记录（共 <span id="historyCount">0</span> 期）</b>
<table>
<thead><tr><th>日期</th><th>期号</th><th>区块</th><th>7号码</th><th>单双</th></tr></thead>
<tbody id="hist"></tbody>
</table>
</div>

<div class="card note">概率仅由历史模式匹配次数计算：例如历史出现「单3→单4」后，统计下一期分别为单1～单7的次数，再换算成百分比。概率用于统计参考，由你自行选择。</div>
</section>

</div>

<script>
function tab(n){
 document.getElementById('s1').className='section '+(n==1?'show':'');
 document.getElementById('s2').className='section '+(n==2?'show':'');
 document.getElementById('b1').className=n==1?'on':'';
 document.getElementById('b2').className=n==2?'on':'';
}
function esc(x){
 return String(x??'').replace(/[&<>"]/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[m]));
}
async function refresh(){
 try{
  const d=await (await fetch('/api/state',{cache:'no-store'})).json();
  const l=d.latest||{};

  document.getElementById('currentTime').textContent=d.day||'-';
  document.getElementById('period').textContent=d.display_period+'期';
  document.getElementById('pos').textContent=d.progress+'/20';
  document.getElementById('cycleLabel').textContent=d.day+'｜当前期：'+d.display_period+'｜20组进度：'+d.progress+'/20';
  document.getElementById('block').textContent=l.target_block||l.block||'-';
  document.getElementById('chain').textContent=l.chain_block||'-';

  document.getElementById('pred').textContent=d.prediction ? ('预判期：'+d.display_period+'期｜'+d.prediction) : '等待17/20预判';
  document.getElementById('actual').textContent=d.actual ? ('开奖期：'+d.display_period+'期｜第20组：'+d.actual) : '等待20/20开奖';
  document.getElementById('status').textContent=d.actual?'已开奖':'等待中';

  const actualNums=d.actual_numbers||[];
  document.getElementById('nums').textContent=actualNums.map(x=>String(x).padStart(2,'0')).join('、')||'-';

  document.getElementById('parity').innerHTML=d.actual ?
   '<span class="badge">'+d.actual_odd+'单'+d.actual_even+'双</span>' : '';

  document.getElementById('hash').textContent=d.actual_hash||'';

  document.getElementById('total').textContent=d.prediction_total;
  document.getElementById('hits').textContent=d.hits;
  document.getElementById('miss').textContent=d.misses;
  document.getElementById('rate').textContent=d.hit_rate+'%';

  document.getElementById('cycles').innerHTML=d.king_history.map(x=>
   '<tr><td>'+esc(String(x.period||'').slice(-4))+'</td><td>'+esc(x.prediction||'-')+'</td><td>'+esc(x.actual||'-')+'</td><td>'+(x.hit?'✅':'❌')+'</td></tr>'
  ).join('');

  const sc=d.single_counts||{};
  document.getElementById('singleCounts').innerHTML=
   [1,2,3,4,5,6,7].map(i=>
    '<span class="badge">'+i+'单：'+(sc[String(i)]||0)+'次</span>'
   ).join('');

  const combo=Object.entries(d.combo_counts||{});
  document.getElementById('comboStats').innerHTML=
   combo.map(x=>'<span class="badge">'+esc(x[0])+' × '+x[1]+'期</span>').join('')||'暂无';

  const hp=d.historical_prediction;
  document.getElementById('histPredPeriod').textContent=hp ? (hp.target_period.slice(-4)+'期') : '-';
  document.getElementById('histMatches').textContent=hp ? hp.matches : '0';
  document.getElementById('histPattern').textContent=hp ? hp.pattern.join(' → ') : '历史数据不足';
  document.getElementById('histBasis').textContent=hp ? ('匹配历史模式后统计下一期1～7单的分布；当前使用'+hp.pattern_len+'期模式。') : '至少需要足够的历史正式开奖数据。';
  document.getElementById('probabilities').innerHTML=hp ? [1,2,3,4,5,6,7].map(i=>{
    const v=Number(hp.probabilities[String(i)]||0);
    return '<div class="statrow"><b>单'+i+'</b><span>'+v.toFixed(2)+'%</span></div>';
  }).join('') : '暂无历史模式匹配';
  document.getElementById('historyCount').textContent=d.history_count||0;

  document.getElementById('hist').innerHTML=d.history_all.map(x=>
   '<tr><td>'+esc((x.period||'').slice(0,6))+'</td><td>'+esc((x.period||'').slice(-4))+'</td><td>'+x.block+'</td><td>'+
   x.numbers.split(',').map(n=>String(n).padStart(2,'0')).join(' ')+'</td><td>'+
   x.odd+'单'+x.even+'双</td></tr>'
  ).join('');
 }catch(e){}
}
refresh();setInterval(refresh,1000);
</script>
</body></html>"""


@app.get("/")
def index():
    return render_template_string(PAGE)


@app.get("/api/state")
def api_state():
    return jsonify(state())


if __name__ == "__main__":
    threading.Thread(target=monitor, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
