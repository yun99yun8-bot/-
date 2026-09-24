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
    try:
        c.execute("ALTER TABLE minute_blocks ADD COLUMN source_version INTEGER NOT NULL DEFAULT 1")
    except sqlite3.OperationalError:
        pass
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
    c.execute("""CREATE TABLE IF NOT EXISTS period_targets(
        day TEXT NOT NULL,
        period_index INTEGER NOT NULL,
        target_block INTEGER NOT NULL,
        PRIMARY KEY(day,period_index)
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


def get_period_target(day, idx):
    with lock:
        c=db(); row=c.execute("SELECT target_block FROM period_targets WHERE day=? AND period_index=?", (day,int(idx))).fetchone(); c.close()
    return int(row["target_block"]) if row else None


def save_period_target(day, idx, block):
    with lock:
        c=db(); c.execute("INSERT OR IGNORE INTO period_targets(day,period_index,target_block) VALUES(?,?,?)", (day,int(idx),int(block))); c.commit(); c.close()


def block_timestamp(block):
    try:
        data=fetch_block_by_number(block)
        raw=data.get("block_header",{}).get("raw_data",{})
        ts=raw.get("timestamp")
        return int(ts) if ts is not None else None
    except Exception:
        return None


def fetch_block_checked(number):
    try:
        data=fetch_block_by_number(number)
        raw=data.get("block_header",{}).get("raw_data",{})
        bh=data.get("blockID"); bn=raw.get("number"); ts=raw.get("timestamp")
        if bh and bn is not None and int(bn)==int(number) and ts is not None:
            return data
    except Exception:
        pass
    return None


def find_latest_block_before(boundary_ms, lower_ms=None, center=None, radius=28, chain=None):
    """Find the highest real TRON block whose timestamp is before the boundary.
    Heights are only a search aid; timestamps decide the result. This handles
    missed slots, so a period may advance by 18/19/20+ heights rather than a
    hard-coded +20.
    """
    if center is None:
        center = int(chain or 0)
    if not center:
        return None
    hi=int(chain or center)
    lo=max(1, center-int(radius))
    best=None
    # Scan from high to low so the first valid block is the latest one.
    for b in range(min(hi,center+int(radius)), lo-1, -1):
        data=fetch_block_checked(b)
        if not data: continue
        ts=int(data["block_header"]["raw_data"]["timestamp"])
        if ts < boundary_ms and (lower_ms is None or ts >= lower_ms):
            return data
        if ts >= boundary_ms:
            continue
    return best


def target_block_for_datetime(dt):
    """Return the platform's official开奖区块 for the current period.

    Calibration supplied from the platform for 2026-09-24:
      0480期 -> 86511888 (exception)
      0481期 -> 86511906
    From 0481期 onward the platform target advances by 20 heights per period.
    We intentionally do not infer the reason for the 0480/0481 transition; the
    platform mapping is treated as the source of truth for this date.
    """
    day=dt.strftime("%Y-%m-%d"); idx=max(1,dt.hour*60+dt.minute)
    if day == "2026-09-24":
        if idx == 480:
            return (86511888, True)
        if idx >= 481:
            return (86511906 + (idx-481)*20, True)
    # Outside the calibrated range, retain the existing timestamp-based lookup.
    rows=get_minute_blocks(day,idx,20)
    r20=next((r for r in rows if int(r["position"])==20),None)
    return (int(r20["block"]),True) if r20 else (None,False)

def current_position(target, chain_block):
    # One platform minute contains twenty 3-second progress slots.
    now=local_now(); sec=now.second + now.microsecond/1_000_000
    return max(1,min(20,int(sec/3)+1))


def group_block_for_position(day, idx, position, target=None):
    """Resolve the real TRON block for one of the 20 three-second slots.

    The platform's period is a minute and its 20 groups are the 20 time slots
    inside that minute.  We therefore use the block timestamp as the source of
    truth.  Block height is used only to narrow the API search window; it is
    never used as ``previous + 20``.
    """
    from zoneinfo import ZoneInfo
    tz=ZoneInfo(os.getenv("APP_TIMEZONE", "Asia/Shanghai"))
    base=datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=tz)+timedelta(minutes=int(idx))
    slot_start=base+timedelta(seconds=(int(position)-1)*3)
    slot_end=slot_start+timedelta(seconds=3)
    start_ms=int(slot_start.timestamp()*1000)
    end_ms=int(slot_end.timestamp()*1000)

    with lock:
        chain=latest.get("chain_block")
    if chain is None:
        return None

    # Estimate the height from the chain head and the slot midpoint.  Search
    # broadly enough to tolerate missed TRON slots.  The timestamp filter below
    # is authoritative, so a +18/+19/+20 height jump is handled naturally.
    now_utc=datetime.now(timezone.utc)
    slot_mid=slot_start+timedelta(seconds=1.5)
    age=(now_utc-slot_mid.astimezone(timezone.utc)).total_seconds()
    center=int(round(int(chain)-age/3.0))
    center=max(1,center)
    lo=max(1,center-32)
    hi=min(int(chain)+32,center+32)

    candidates=[]
    for b in range(lo,hi+1):
        data=fetch_block_checked(b)
        if not data:
            continue
        raw=data.get("block_header",{}).get("raw_data",{})
        ts=raw.get("timestamp")
        if ts is None:
            continue
        ts=int(ts)
        if start_ms <= ts < end_ms:
            candidates.append((ts,int(raw.get("number",b)),data))

    if not candidates:
        return None

    # A slot normally has one block. If more than one is returned around a
    # boundary, use the block whose timestamp is closest to the slot midpoint.
    mid_ms=(start_ms+end_ms)//2
    candidates.sort(key=lambda x:(abs(x[0]-mid_ms),x[1]))
    return candidates[0][2]

def save_minute_block(day, idx, pos, block, block_hash, calc, ts):
    with lock:
        c=db(); c.execute("""INSERT OR IGNORE INTO minute_blocks(day,period_index,position,block,hash,odd,even,numbers,ts,source_version)
             VALUES(?,?,?,?,?,?,?,?,?,3)""",(day,idx,pos,int(block),block_hash,calc["odd"],calc["even"],",".join(map(str,calc["numbers"])),ts)); c.commit(); c.close()


def get_minute_blocks(day, idx, upto=None):
    q="SELECT * FROM minute_blocks WHERE day=? AND period_index=? AND source_version=3"
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
    # 第17组完成后，用当前期前17组的“单几”频率统计来确定第20组统计倾向。
    # 规则：单0～单6分别统计出现次数，出现次数最多者作为统计结论。
    if len(rows)<17: return None
    with lock:
        c=db(); existing=c.execute("SELECT prediction FROM king_predictions WHERE day=? AND period_index=?",(day,idx)).fetchone()
        if existing and existing["prediction"]: c.close(); return existing["prediction"]
    counts={i:0 for i in range(7)}
    for r in rows[:17]:
        odd=int(r["odd"])
        if 0 <= odd <= 7:
            counts[odd]+=1
    mx=max(counts.values())
    if mx <= 0: return None
    # 同频时不人为制造概率差异：保留最先出现的并列最大值。
    pred=f"单{next(i for i in range(7) if counts[i]==mx)}"
    with lock:
        c=db(); c.execute("""INSERT OR IGNORE INTO king_predictions(day,period_index,period,prediction,predicted_at)
             VALUES(?,?,?,?,?)""",(day,idx,period,pred,datetime.now(timezone.utc).isoformat())); c.commit(); c.close()
    return pred


def resolve_king(day,idx,period,rows):
    if len(rows)<20: return
    pred=king_prediction(day,idx,period,rows)
    if not pred: return
    actual=f"单{rows[19]['odd']}"; hit=int(pred==actual)
    with lock:
        c=db(); c.execute("UPDATE king_predictions SET actual=?,hit=?,resolved_at=? WHERE day=? AND period_index=? AND actual IS NULL",(actual,hit,datetime.now(timezone.utc).isoformat(),day,idx)); c.commit(); c.close()


def process_current_minute():
    dt=local_now(); period,idx=period_info(dt); day=dt.strftime("%Y-%m-%d")
    with lock: chain=latest.get("chain_block")
    if chain is None: return
    pos=current_position(None,chain)

    rows=get_minute_blocks(day,idx,20)
    existing={int(r["position"]) for r in rows}
    # Fill the current slot and retry any immediately preceding missing slots.
    # We never fabricate a group from a block-height formula.
    for p in range(max(1,pos-2),pos+1):
        if p in existing: continue
        data=group_block_for_position(day,idx,p,None)
        if not data: continue
        raw=data.get("block_header",{}).get("raw_data",{})
        block=int(raw.get("number")); bh=data.get("blockID")
        calc=calculate_numbers(bh)
        if not calc: continue
        ts=datetime.fromtimestamp(int(raw.get("timestamp"))/1000,tz=timezone.utc).isoformat()
        save_minute_block(day,idx,p,block,bh,calc,ts)

    rows=get_minute_blocks(day,idx,20)
    pred=king_prediction(day,idx,period,rows) if len([r for r in rows if int(r["position"])<=17])>=17 else None
    r20=next((r for r in rows if int(r["position"])==20),None)
    if r20:
        nums=list(map(int,r20["numbers"].split(",")))
        calc={"numbers":nums,"odd":r20["odd"],"even":r20["even"],
              "tail_odd":sum((n%10)%2 for n in nums),"tail_even":0,"letters":"","digits":""}
        calc["tail_even"]=7-calc["tail_odd"]
        save_record(period,idx,r20["block"],r20["hash"],calc,r20["ts"])
        if len(rows)>=20:
            resolve_king(day,idx,period,rows)

    target=int(r20["block"]) if r20 else (int(rows[-1]["block"]) if rows else None)
    last=rows[-1] if rows else None
    latest.update({"period":period,"block":target,"target_block":target,"chain_block":chain,
                   "progress":pos,"hash":last["hash"] if last else None,
                   "numbers":list(map(int,last["numbers"].split(","))) if last else None,
                   "odd":last["odd"] if last else None,"even":last["even"] if last else None,
                   "confirmed_anchor":bool(r20),"updated":datetime.now(timezone.utc).isoformat()})

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
    probs={str(i):round((counts[i]*100/total),2) if total else 0 for i in range(0,7)}
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
    single_counts={str(i):sum(1 for r in recent60 if int(r["odd"])==i) for i in range(0,7)}
    combo_counts=Counter(f"单{r['odd']}" for r in recent60)
    current20=[dict(r) for r in current_rows]
    ccounts={str(i):sum(1 for r in current_rows if int(r["odd"])==i) for i in range(0,7)}
    ctotal=len(current_rows)
    cprobs={k:round(v*100/ctotal,2) if ctotal else 0 for k,v in ccounts.items()}
    histpred=historical_prediction(all_rows,period)
    latest_copy=dict(latest); latest_copy.update({"target_block":target,"chain_block":chain,"progress":progress})
    return {"latest":latest_copy,"day":day,"current_time":day,"period_index":idx,"display_period":f"{idx:04d}",
            "progress":progress,"prediction":pred,"actual":actual,"actual_numbers":actual_numbers,"actual_odd":actual_odd,"actual_even":actual_even,"actual_block":actual_block,"actual_hash":actual_hash,"prediction_period":period,
            "prediction_total":int(done["n"] or 0),"hits":int(done["h"] or 0),"misses":int(done["n"] or 0)-int(done["h"] or 0),
            "hit_rate":round(int(done["h"] or 0)*100/int(done["n"]),1) if int(done["n"] or 0) else 0,
            "single_counts":single_counts,"combo_counts":dict(combo_counts.most_common()),
            "king_history":[dict(x) for x in cycles],"current20":[dict(x) for x in current20],
            "current20_single_counts":ccounts,"current20_single_probs":cprobs,
            "history":[dict(x) for x in recent60[::-1]],"history_all":[dict(x) for x in all_rows[::-1]],
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
table{width:100%;border-collapse:collapse;font-size:11px;border:1px solid #dfe3ea;border-radius:8px;overflow:hidden}
th,td{padding:6px 3px;border:1px solid #e7eaf0;text-align:left;vertical-align:middle}
th{background:#f5f6f8;font-weight:800}
.tablewrap{border:1px solid #dfe3ea;border-radius:10px;overflow:hidden}
.statsgrid{display:grid;grid-template-columns:repeat(4,1fr);gap:5px;margin-top:7px}
.statbox{background:#f6f7fa;border:1px solid #e5e8ee;border-radius:8px;padding:6px;text-align:center}
.statbox b{display:block;font-size:15px}.statbox span{font-size:10px;color:#777}
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
<button id="b1" class="on" onclick="tab(1)">Torn计算</button>
<button id="b2" onclick="tab(2)">历史统计</button>
</div>

<section id="s1" class="section show">

</div>

<div class="card center">
<div class="label" id="cycleLabel">当前20组状态</div>
<div class="status" id="status">等待中</div>

<div class="label">经统计结论为</div>
<div class="status" id="pred">等待第17组</div>

<div class="label">实际结果</div>
<div class="status" id="actual">等待第20组</div>

<div class="label" id="resultLabel">平台对应期号开奖结果</div>
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
<b>当期20组实际记录</b>
<div class="note" style="margin-top:5px">当前期已经实际读取到的20组区块数据；每组显示7个号码和单数个数。数据逐组实时增加。</div>
<div class="tablewrap" style="margin-top:7px">
<table>
<thead><tr><th>组</th><th>区块</th><th>7个号码</th><th>结果</th></tr></thead>
<tbody id="current20"></tbody>
</table>
</div>
<div style="margin-top:9px"><b>当期20组单数统计</b></div>
<div class="note" style="margin-top:3px">按当前已经出现的各组结果计算；20组全部完成后即为本期完整统计。</div>
<div class="statsgrid" id="current20Stats"></div>
</div>

<div class="card note">
每天固定1440期：0001～1440；1440期结束后次日重新从0001开始。每一期内部有20组TRON区块进度，17/20时生成第20组的统计预判，20/20结算。下一分钟进入下一期，20组进度重新从1/20开始。
</div>
</section>

<section id="s2" class="section">

<div class="card">
<b>历史统计独立预判</b>
<div class="note" style="margin-top:5px">只读取已经完成的历史正式开奖数据，与「Torn计算」算法完全独立。</div>
<div class="grid" style="margin-top:7px">
<div class="box"><div class="label">预判期</div><div class="big" id="histPredPeriod">-</div></div>
<div class="box"><div class="label">历史匹配次数</div><div class="big" id="histMatches">0</div></div>
</div>
<div class="box" style="margin-top:6px"><div class="label">当前历史模式</div><div class="big" id="histPattern">-</div></div>
<div style="margin-top:7px" id="probabilities"></div>
<div class="note" id="histBasis" style="margin-top:6px"></div>
</div>

<div class="card">
<b>最近60期：单0～单6出现次数</b>
<div id="singleCounts" style="margin-top:5px"></div>
</div>

<div class="card">
<b>最近60期：单数统计</b>
<div id="comboStats" style="margin-top:5px"></div>
</div>

<div class="card">
<b>全部历史开奖记录（共 <span id="historyCount">0</span> 期）</b>
<table>
<thead><tr><th>日期</th><th>期号</th><th>区块</th><th>7号码</th><th>单双</th></tr></thead>
<tbody id="hist"></tbody>
</table>
</div>

<div class="card note">概率仅由历史模式匹配次数计算；页面只显示单0～单6的统计概率。概率用于统计参考，由你自行选择。</div>
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

  document.getElementById('cycleLabel').textContent=d.day+'｜当前期：'+d.display_period+'｜20组进度：'+d.progress+'/20';

  document.getElementById('pred').textContent=d.prediction ? d.prediction : '等待17/20统计';
  document.getElementById('actual').textContent=d.actual ? (d.display_period+'期｜'+d.actual) : '等待20/20开奖';
  document.getElementById('resultLabel').textContent=d.actual ? ('平台'+d.display_period+'期开奖结果') : '平台对应期号开奖结果';
  document.getElementById('status').textContent=d.actual?'已开奖':'等待中';

  const actualNums=d.actual_numbers||[];
  document.getElementById('nums').textContent=actualNums.map(x=>String(x).padStart(2,'0')).join('、')||'-';

  document.getElementById('parity').innerHTML=d.actual ?
   '<span class="badge">单'+d.actual_odd+'</span>' : '';

  document.getElementById('hash').textContent=d.actual_hash||'';

  document.getElementById('total').textContent=d.prediction_total;
  document.getElementById('hits').textContent=d.hits;
  document.getElementById('miss').textContent=d.misses;
  document.getElementById('rate').textContent=d.hit_rate+'%';

  document.getElementById('current20').innerHTML=(d.current20||[]).map(x=>{
    const nums=(x.numbers||'').split(',').filter(Boolean).map(n=>String(n).padStart(2,'0')).join(' ');
    return '<tr><td>第'+x.position+'组</td><td>'+x.block+'</td><td>'+esc(nums)+'</td><td>单'+x.odd+'</td></tr>';
  }).join('');

  const cps=d.current20_single_probs||{};
  const ccs=d.current20_single_counts||{};
  document.getElementById('current20Stats').innerHTML=[0,1,2,3,4,5,6].map(i=>
    '<div class="statbox"><b>单'+i+'</b><span>'+Number(cps[String(i)]||0).toFixed(2)+'% · '+(ccs[String(i)]||0)+'组</span></div>'
  ).join('');

  const sc=d.single_counts||{};
  document.getElementById('singleCounts').innerHTML=
   [0,1,2,3,4,5,6].map(i=>
    '<span class="badge">单'+i+'：'+(sc[String(i)]||0)+'次</span>'
   ).join('');

  const combo=Object.entries(d.combo_counts||{});
  document.getElementById('comboStats').innerHTML=
   combo.map(x=>'<span class="badge">'+esc(x[0])+' × '+x[1]+'期</span>').join('')||'暂无';

  const hp=d.historical_prediction;
  document.getElementById('histPredPeriod').textContent=hp ? (hp.target_period.slice(-4)+'期') : '-';
  document.getElementById('histMatches').textContent=hp ? hp.matches : '0';
  document.getElementById('histPattern').textContent=hp ? hp.pattern.join(' → ') : '历史数据不足';
  document.getElementById('histBasis').textContent=hp ? ('匹配历史模式后统计下一期1～7单的分布；当前使用'+hp.pattern_len+'期模式。') : '至少需要足够的历史正式开奖数据。';
  document.getElementById('probabilities').innerHTML=hp ? [0,1,2,3,4,5,6].map(i=>{
    const v=Number(hp.probabilities[String(i)]||0);
    return '<div class="statrow"><b>单'+i+'</b><span>'+v.toFixed(2)+'%</span></div>';
  }).join('') : '暂无历史模式匹配';
  document.getElementById('historyCount').textContent=d.history_count||0;

  document.getElementById('hist').innerHTML=d.history_all.map(x=>
   '<tr><td>'+esc((x.period||'').slice(0,6))+'</td><td>'+esc((x.period||'').slice(-4))+'</td><td>'+x.block+'</td><td>'+
   x.numbers.split(',').map(n=>String(n).padStart(2,'0')).join(' ')+'</td><td>'+
   '单'+x.odd+'</td></tr>'
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
