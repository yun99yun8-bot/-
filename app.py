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

# 你的截图验证出的关系（2026-09-24）：
# 2609240448 -> 86511248
# 每期相差20个TRON区块。
# 0448 = 当天07:28的分钟序号（7*60+28）。
# 因此当天基准区块 = 86511248 - 448*20 = 86502288。
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
        tail_even INTEGER,
        cycle INTEGER,
        cycle_pos INTEGER
    )""")
    # 20组周期必须按“每天”重新从第1组开始。
    # 旧版本 cycles 只有 cycle 主键，无法容纳第二天的第1组；这里自动迁移。
    cycle_cols = [r[1] for r in c.execute("PRAGMA table_info(cycles)").fetchall()]
    if cycle_cols and "day" not in cycle_cols:
        c.execute("ALTER TABLE cycles RENAME TO cycles_legacy")

    c.execute("""CREATE TABLE IF NOT EXISTS cycles(
        day TEXT NOT NULL,
        cycle INTEGER NOT NULL,
        prediction TEXT,
        actual TEXT,
        hit INTEGER,
        predicted_at TEXT,
        resolved_at TEXT,
        PRIMARY KEY(day, cycle)
    )""")

    # 把旧版本已经完成的周期尽量迁移回来。
    legacy_exists = c.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='cycles_legacy'"
    ).fetchone()
    if legacy_exists:
        old_rows = c.execute("SELECT * FROM cycles_legacy ORDER BY cycle ASC").fetchall()
        old_records = c.execute("SELECT * FROM records ORDER BY id ASC").fetchall()
        for cr in old_rows:
            old_cycle = int(cr["cycle"])
            idx = old_cycle * 20 - 1
            if 0 <= idx < len(old_records):
                rr = old_records[idx]
                p0 = str(rr["period"] or "")
                day = f"20{p0[:2]}-{p0[2:4]}-{p0[4:6]}" if len(p0) >= 6 else ""
                local_cycle = int(rr["period_index"]) // 20 + 1
                c.execute("""INSERT OR IGNORE INTO cycles(
                    day,cycle,prediction,actual,hit,predicted_at,resolved_at
                ) VALUES(?,?,?,?,?,?,?)""", (
                    day, local_cycle, cr["prediction"], cr["actual"], cr["hit"],
                    cr["predicted_at"], cr["resolved_at"]
                ))
        c.execute("DROP TABLE cycles_legacy")
    c.execute("""CREATE TABLE IF NOT EXISTS anchors(
        day TEXT PRIMARY KEY,
        anchor_block INTEGER
    )""")
    c.execute(
        "INSERT OR IGNORE INTO anchors(day,anchor_block) VALUES(?,?)",
        (DEFAULT_ANCHOR_DATE, DEFAULT_ANCHOR_BLOCK)
    )
    c.commit()
    c.close()


def get_anchor(day):
    with lock:
        c = db()
        row = c.execute(
            "SELECT anchor_block FROM anchors WHERE day=?", (day,)
        ).fetchone()
        c.close()
    if row:
        return int(row["anchor_block"])
    return None


def set_anchor(day, block):
    with lock:
        c = db()
        c.execute(
            "INSERT OR REPLACE INTO anchors(day,anchor_block) VALUES(?,?)",
            (day, int(block))
        )
        c.commit()
        c.close()


def calculate_numbers(block_hash):
    """
    正式规则：
    1) 从哈希右向左，取A-E字母流和0-9数字流。
    2) A=0,B=1,C=2,D=3,E=4，字母为十位、数字为个位。
    3) 00无效，弃用并整体顺延。
    4) 相同组合/号码再次出现时，第二个弃用并顺延。
    5) 一直继续取，直到得到7个不同的01-49号码。
    """
    h = (block_hash or "").lower()

    letters = [ch for ch in reversed(h) if ch in "abcde"]
    digits = [ch for ch in reversed(h) if ch.isdigit()]

    # 必须把“右到左”的取值顺序作为计算顺序。
    # 每次消耗一组 letter+digit；无效/重复就继续消耗下一组。
    mp = {"a": 0, "b": 1, "c": 2, "d": 3, "e": 4}

    numbers = []
    used = set()
    used_pairs = set()
    i = 0

    while len(numbers) < 7 and i < min(len(letters), len(digits)):
        pair = (letters[i], digits[i])
        n = mp[pair[0]] * 10 + int(pair[1])
        i += 1

        # 00不属于01-49，直接弃用
        if n == 0:
            continue

        # 01-49以外不取
        if n < 1 or n > 49:
            continue

        # 相同组合/相同号码再次出现，弃用
        if pair in used_pairs or n in used:
            continue

        used_pairs.add(pair)
        used.add(n)
        numbers.append(n)

    if len(numbers) < 7:
        return None

    # 记录实际使用到的字母/数字，便于核对
    used_letters = "".join(letters[:i]).upper()
    used_digits = "".join(digits[:i])

    odd = sum(n % 2 for n in numbers)
    tail_odd = sum((n % 10) % 2 for n in numbers)

    return {
        "numbers": numbers,
        "letters": used_letters,
        "digits": used_digits,
        "odd": odd,
        "even": 7 - odd,
        "tail_odd": tail_odd,
        "tail_even": 7 - tail_odd,
    }


def period_day(period):
    """从平台期号 YYMMDDxxxx 取出本地日期 YYYY-MM-DD。"""
    p = str(period or "")
    if len(p) >= 6 and p[:6].isdigit():
        yy, mm, dd = p[:2], p[2:4], p[4:6]
        return f"20{yy}-{mm}-{dd}"
    return ""


def day_records(day):
    """只取某一天的1440期，按当天分钟序号排序。"""
    with lock:
        c = db()
        rows = c.execute(
            "SELECT * FROM records WHERE period LIKE ? ORDER BY period_index ASC",
            (day[2:4] + day[5:7] + day[8:10] + "%",)
        ).fetchall()
        c.close()
    return rows


def cycle_records(day, cycle):
    start = (int(cycle) - 1) * 20
    end = start + 20
    with lock:
        c = db()
        rows = c.execute(
            "SELECT * FROM records WHERE period LIKE ? AND period_index>=? AND period_index<? ORDER BY period_index ASC",
            (day[2:4] + day[5:7] + day[8:10] + "%", start, end)
        ).fetchall()
        c.close()
    return rows


def get_cycle_row(day, cycle):
    with lock:
        c = db()
        row = c.execute(
            "SELECT * FROM cycles WHERE day=? AND cycle=?", (day, int(cycle))
        ).fetchone()
        c.close()
    return row


def lock_prediction(day, cycle, rows):
    """第17期到达时锁定预判；之后第18/19期不会改变它。"""
    if len(rows) < 17:
        return None
    existing = get_cycle_row(day, cycle)
    if existing and existing["prediction"]:
        return existing["prediction"]

    first17 = rows[:17]
    counts = Counter(f"{r['odd']}单{r['even']}双" for r in first17)
    if not counts:
        return None
    max_count = max(counts.values())
    candidates = [k for k, v in counts.items() if v == max_count]
    prediction = candidates[0]
    if len(candidates) > 1:
        for r in reversed(first17):
            key = f"{r['odd']}单{r['even']}双"
            if key in candidates:
                prediction = key
                break

    with lock:
        c = db()
        c.execute("""INSERT INTO cycles(day,cycle,prediction,actual,hit,predicted_at,resolved_at)
                     VALUES(?,?,?,NULL,NULL,?,NULL)
                     ON CONFLICT(day,cycle) DO NOTHING""",
                  (day, int(cycle), prediction, first17[-1]["ts"]))
        c.commit()
        c.close()
    return prediction


def resolve_cycle(day, cycle, rows):
    if len(rows) < 20:
        return
    # 先锁定第17期预判，再在第20期到达时写入实际结果。
    prediction = lock_prediction(day, cycle, rows)
    if not prediction:
        return
    actual = f"{rows[19]['odd']}单{rows[19]['even']}双"
    hit = int(prediction == actual)
    with lock:
        c = db()
        c.execute("""UPDATE cycles SET actual=?, hit=?, resolved_at=?
                     WHERE day=? AND cycle=? AND actual IS NULL""",
                  (actual, hit, rows[19]["ts"], day, int(cycle)))
        c.commit()
        c.close()


def period_info(dt):
    # 平台截图验证：期号后四位 = 当天分钟序号，例如07:28 -> 0448。
    idx = dt.hour * 60 + dt.minute
    period = dt.strftime("%y%m%d") + f"{idx:04d}"
    return period, idx


def target_block_for_datetime(dt):
    day = dt.strftime("%Y-%m-%d")
    anchor = get_anchor(day)

    if anchor is None:
        # 新的一天如果还没有人工/历史锚点，按当前链高度估算。
        # 页面会显示“估算同步”，不会冒充平台已确认。
        with lock:
            current = latest.get("block")
        if current is None:
            return None, False
        idx = dt.hour * 60 + dt.minute
        anchor = int(current) - idx * 20
        set_anchor(day, anchor)
        return anchor + idx * 20, False

    idx = dt.hour * 60 + dt.minute
    return anchor + idx * 20, True


def fetch_block_by_number(number):
    headers = {"TRON-PRO-API-KEY": API_KEY} if API_KEY else {}
    r = requests.post(
        TRON_BLOCK_URL,
        headers=headers,
        json={"num": int(number)},
        timeout=8,
    )
    r.raise_for_status()
    return r.json()


def save_record(period, idx, block, block_hash, calc, dt, cycle, cycle_pos):
    with lock:
        c = db()
        c.execute("""INSERT OR IGNORE INTO records(
            period,period_index,block,hash,numbers,letters,digits,ts,
            odd,even,tail_odd,tail_even,cycle,cycle_pos
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            period, idx, int(block), block_hash,
            ",".join(map(str, calc["numbers"])),
            calc["letters"], calc["digits"], dt.isoformat(),
            calc["odd"], calc["even"], calc["tail_odd"], calc["tail_even"],
            cycle, cycle_pos
        ))
        c.commit()
        c.close()


def all_records():
    with lock:
        c = db()
        rows = c.execute(
            "SELECT * FROM records ORDER BY id ASC"
        ).fetchall()
        c.close()
    return rows


def predict_cycle(cycle, records):
    """兼容旧调用：records 应为当前日期的记录。"""
    first17 = records[:17]
    if len(first17) < 17:
        return None
    counts = Counter(f"{r['odd']}单{r['even']}双" for r in first17)
    max_count = max(counts.values())
    candidates = [k for k, v in counts.items() if v == max_count]
    if len(candidates) == 1:
        return candidates[0]
    for r in reversed(first17):
        key = f"{r['odd']}单{r['even']}双"
        if key in candidates:
            return key
    return candidates[0]


def sync_completed_cycles():
    """补齐历史数据中的已锁定/已完成20组周期。"""
    with lock:
        c = db()
        days = [r[0] for r in c.execute("SELECT DISTINCT substr(period,1,6) FROM records ORDER BY 1").fetchall()]
        c.close()
    for yymmdd in days:
        day = f"20{yymmdd[:2]}-{yymmdd[2:4]}-{yymmdd[4:6]}"
        for cycle in range(1, 73):
            rows = cycle_records(day, cycle)
            if len(rows) >= 17:
                lock_prediction(day, cycle, rows)
            if len(rows) >= 20:
                resolve_cycle(day, cycle, rows)


def process_target_period():
    """
    只把“平台当前分钟对应的开奖区块”作为正式一期。
    不再把每3秒一个TRON区块都当成一期开奖结果。
    """
    now = datetime.now(timezone.utc)
    # 平台如果以中国时间运行，设置 APP_TIMEZONE=Asia/Shanghai。
    # 当前默认使用 Asia/Shanghai。
    try:
        from zoneinfo import ZoneInfo
        tz_name = os.getenv("APP_TIMEZONE", "Asia/Shanghai")
        local_now = now.astimezone(ZoneInfo(tz_name))
    except Exception:
        local_now = now + timedelta(hours=8)

    period, idx = period_info(local_now)
    target_block, confirmed_anchor = target_block_for_datetime(local_now)

    if target_block is None:
        return

    # 不在区块出现之前请求
    with lock:
        current_block = latest.get("block")
    if current_block is not None and int(current_block) < int(target_block):
        return

    # 已经记录过就不重复处理
    with lock:
        c = db()
        exists = c.execute(
            "SELECT id FROM records WHERE period=?", (period,)
        ).fetchone()
        c.close()
    if exists:
        return

    try:
        data = fetch_block_by_number(target_block)
    except Exception as ex:
        print("target block fetch error:", repr(ex), flush=True)
        return

    block_hash = data.get("blockID")
    raw = data.get("block_header", {}).get("raw_data", {})
    actual_block = raw.get("number")

    if not block_hash or actual_block is None:
        return

    # 如果节点返回的高度不是目标高度，不保存。
    if int(actual_block) != int(target_block):
        return

    result = calculate_numbers(block_hash)
    if not result:
        print(
            "开奖数据不足，无法取得7个有效号码:",
            period, target_block, flush=True
        )
        return

    # 每天1440期固定分成72组，每组20期；次日重新从第1组开始。
    cycle_number = (idx // 20) + 1
    cycle_position = (idx % 20) + 1
    day = local_now.strftime("%Y-%m-%d")

    dt = datetime.fromtimestamp(
        int(raw.get("timestamp", int(time.time() * 1000))) / 1000,
        tz=timezone.utc
    )

    save_record(
        period, idx, target_block, block_hash, result, dt,
        cycle_number, cycle_position
    )

    rows = cycle_records(day, cycle_number)
    if len(rows) >= 17:
        lock_prediction(day, cycle_number, rows)
    if len(rows) >= 20:
        resolve_cycle(day, cycle_number, rows)

    latest.update({
        "period": period,
        "block": target_block,
        "hash": block_hash,
        "numbers": result["numbers"],
        "odd": result["odd"],
        "even": result["even"],
        "tail_odd": result["tail_odd"],
        "tail_even": result["tail_even"],
        "confirmed_anchor": confirmed_anchor,
        "target_block": target_block,
        "updated": datetime.now(timezone.utc).isoformat(),
    })


def monitor():
    global last_seen_block

    while True:
        try:
            headers = {"TRON-PRO-API-KEY": API_KEY} if API_KEY else {}
            response = requests.get(
                TRON_URL, headers=headers, timeout=8
            )
            response.raise_for_status()
            data = response.json()

            block_hash = data.get("blockID")
            raw = data.get("block_header", {}).get("raw_data", {})
            block_number = raw.get("number")

            if block_number is not None:
                latest["chain_block"] = int(block_number)
                latest["chain_hash"] = block_hash

            if block_number is not None and block_number != last_seen_block:
                last_seen_block = block_number

            process_target_period()

        except Exception as ex:
            print("monitor error:", repr(ex), flush=True)

        time.sleep(POLL_SECONDS)


def state():
    # 页面显示的当前期数严格按“当天分钟序号+1”，每天到1440后重置。
    now = datetime.now(timezone.utc)
    try:
        from zoneinfo import ZoneInfo
        tz_name = os.getenv("APP_TIMEZONE", "Asia/Shanghai")
        local_now = now.astimezone(ZoneInfo(tz_name))
    except Exception:
        local_now = now + timedelta(hours=8)

    period, idx = period_info(local_now)
    day = local_now.strftime("%Y-%m-%d")
    current_cycle = idx // 20 + 1
    current_pos = idx % 20 + 1

    rows = cycle_records(day, current_cycle)
    cycle_row = get_cycle_row(day, current_cycle)
    prediction = cycle_row["prediction"] if cycle_row and cycle_row["prediction"] else None
    if not prediction and len(rows) >= 17:
        prediction = lock_prediction(day, current_cycle, rows)
        cycle_row = get_cycle_row(day, current_cycle)

    actual = cycle_row["actual"] if cycle_row and cycle_row["actual"] else None

    with lock:
        c = db()
        done = c.execute(
            "SELECT COUNT(*) AS n, COALESCE(SUM(hit),0) AS h FROM cycles WHERE actual IS NOT NULL"
        ).fetchone()
        cycles = c.execute(
            "SELECT * FROM cycles WHERE actual IS NOT NULL ORDER BY day DESC, cycle DESC LIMIT 30"
        ).fetchall()
        all_rows = c.execute("SELECT * FROM records ORDER BY id ASC").fetchall()
        c.close()

    n = int(done["n"] or 0)
    hits = int(done["h"] or 0)

    recent60 = all_rows[-60:]
    single_counts = {str(i): 0 for i in range(1, 8)}
    combo_counts = Counter()
    for r in recent60:
        single_counts[str(r["odd"])] += 1
        combo_counts[f"{r['odd']}单{r['even']}双"] += 1

    daily = {}
    for r in all_rows:
        day_key = period_day(r["period"])
        daily.setdefault(day_key, Counter())
        daily[day_key][f"{r['odd']}单{r['even']}双"] += 1

    latest_copy = dict(latest)
    # 顶部“当前期数”使用当天1～1440期，而不是累计总期数。
    latest_copy["display_period"] = f"{idx + 1:04d}"
    latest_copy["display_day"] = day
    latest_copy["display_full_period"] = period

    return {
        "latest": latest_copy,
        "day": day,
        "current_time": local_now.strftime("%Y-%m-%d %H:%M:%S"),
        "period_index": idx,
        "display_period": f"{idx + 1:04d}",
        "current_cycle": current_cycle,
        "current_pos": current_pos,
        "prediction": prediction,
        "actual": actual,
        "prediction_total": n,
        "hits": hits,
        "misses": n - hits,
        "hit_rate": round(hits * 100 / n, 1) if n else 0,
        "single_counts": single_counts,
        "combo_counts": dict(combo_counts.most_common()),
        "cycles": [dict(x) for x in cycles],
        "history": [dict(x) for x in recent60[::-1]],
        "daily": {k: dict(v.most_common()) for k, v in sorted(daily.items(), reverse=True)},
        "history_count": len(all_rows),
    }


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
<button id="b1" class="on" onclick="tab(1)">20组预判</button>
<button id="b2" onclick="tab(2)">历史统计</button>
</div>

<section id="s1" class="section show">

<div class="card">
<div class="box"><div class="label">当前时间</div><div class="big" id="currentTime">-</div></div>
<div class="box"><div class="label">当天期数（1～1440）</div><div class="big" id="period">-</div></div>
<div class="box"><div class="label">20组进度（每天72组）</div><div class="big" id="pos">-</div></div>
<div class="box"><div class="label">开奖区块</div><div class="big" id="block">-</div></div>
<div class="box"><div class="label">链上当前区块</div><div class="big" id="chain">-</div></div>
</div>
</div>

<div class="card center">
<div class="label" id="cycleLabel">当前20组状态</div>
<div class="status" id="status">等待中</div>

<div class="label">第17组后统计预判第20组</div>
<div class="status" id="pred">等待第17组</div>

<div class="label">第20组实际结果</div>
<div class="status" id="actual">等待第20组</div>

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
<b>最近20组预判记录</b>
<table>
<thead><tr><th>日期/组</th><th>预判</th><th>实际</th><th>结果</th></tr></thead>
<tbody id="cycles"></tbody>
</table>
</div>

<div class="card note">
每天固定1440期：0001～1440；1440期结束后次日重新从0001开始。每天72个20组周期。开奖区块按已核对的平台“期数—区块”关系同步。
</div>
</section>

<section id="s2" class="section">

<div class="card">
<b>最近60期：1～7单出现次数</b>
<div id="singleCounts" style="margin-top:5px"></div>
</div>

<div class="card">
<b>最近60期单双组合</b>
<div id="comboStats" style="margin-top:5px"></div>
</div>

<div class="card">
<b>每日历史统计</b>
<div id="dailyStats"></div>
</div>

<div class="card">
<b>最近60期历史记录</b>
<table>
<thead><tr><th>期数</th><th>区块</th><th>7号码</th><th>单双</th></tr></thead>
<tbody id="hist"></tbody>
</table>
</div>

<div class="card note">
统计分析只描述已经产生的历史数据；历史频率不能保证下一期结果。
</div>
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

  document.getElementById('currentTime').textContent=d.current_time||'-';
  document.getElementById('period').textContent=d.display_period+'期';
  document.getElementById('pos').textContent=d.current_pos+'/20（第'+d.current_cycle+'组）';
  document.getElementById('cycleLabel').textContent=d.day+'｜第'+d.current_cycle+'组（'+d.current_pos+'/20）';
  document.getElementById('block').textContent=l.target_block||l.block||'-';
  document.getElementById('chain').textContent=l.chain_block||'-';

  document.getElementById('pred').textContent=d.prediction||'等待第17组';
  document.getElementById('actual').textContent=d.actual||'等待第20组';
  document.getElementById('status').textContent=d.actual?'已开奖':'等待中';

  document.getElementById('nums').textContent=
   (l.numbers||[]).map(x=>String(x).padStart(2,'0')).join('、')||'-';

  document.getElementById('parity').innerHTML=l.numbers
   ? '<span class="badge">'+l.odd+'单'+l.even+'双</span>'+
     '<span class="badge">尾数'+l.tail_odd+'单'+l.tail_even+'双</span>'
   : '';

  document.getElementById('hash').textContent=l.hash||'';

  document.getElementById('total').textContent=d.prediction_total;
  document.getElementById('hits').textContent=d.hits;
  document.getElementById('miss').textContent=d.misses;
  document.getElementById('rate').textContent=d.hit_rate+'%';

  document.getElementById('cycles').innerHTML=d.cycles.map(x=>
   '<tr><td>'+esc(x.day)+'<br>第'+x.cycle+'组</td><td>'+esc(x.prediction)+'</td><td>'+
   esc(x.actual)+'</td><td>'+(x.hit?'✅':'❌')+'</td></tr>'
  ).join('');

  const sc=d.single_counts||{};
  document.getElementById('singleCounts').innerHTML=
   [1,2,3,4,5,6,7].map(i=>
    '<span class="badge">'+i+'单：'+(sc[String(i)]||0)+'次</span>'
   ).join('');

  const combo=Object.entries(d.combo_counts||{});
  document.getElementById('comboStats').innerHTML=
   combo.map(x=>'<span class="badge">'+esc(x[0])+' × '+x[1]+'期</span>').join('')||'暂无';

  const days=Object.entries(d.daily||{});
  document.getElementById('dailyStats').innerHTML=days.slice(0,7).map(([day,obj])=>
   '<div class="statrow"><b>'+esc(day)+'</b><span>'+
   Object.entries(obj).map(x=>esc(x[0])+'×'+x[1]).join('　')+
   '</span></div>'
  ).join('')||'暂无';

  document.getElementById('hist').innerHTML=d.history.map(x=>
   '<tr><td>'+esc(x.period)+'</td><td>'+x.block+'</td><td>'+
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
