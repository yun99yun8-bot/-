"""v8的神0.0.1 — fixed-10k walk-forward research engine.
No future result is visible to a prediction. 20+ independent experts plus two ensembles
are evaluated chronologically; reported holdout accuracy is kept separate from fit accuracy.
"""
import json, math, time
from collections import Counter, defaultdict
from datetime import datetime
import collector_core as core

VERSION='v8god-0.0.5-ai-trainer'
USE_THRESHOLD=80.0
RESEARCH_TARGET=95.0
MIN_HISTORY=120
METHODS=[
 'freq20','freq50','freq100','freq300','transition1','transition2','omission_max','omission_ratio',
 'lag7','lag20','lag60','lag120','recent_reversal','recent_momentum','g19','g17','group_mode','group_weighted',
 'hash_ae','hash_tail','hash_prefix','hash_suffix','hash_delta','hybrid_context'
]

def ensure_schema():
    c=core.db_connect(retries=0)
    try:
      with c:
       with c.cursor() as q:
        q.execute("""CREATE TABLE IF NOT EXISTS shen_practice_runtime(
          singleton SMALLINT PRIMARY KEY CHECK(singleton=1), version TEXT NOT NULL, status TEXT NOT NULL,
          round_no INTEGER NOT NULL DEFAULT 0, dataset_start TEXT, dataset_end TEXT, dataset_size INTEGER NOT NULL DEFAULT 0,
          report JSONB NOT NULL DEFAULT '{}'::jsonb, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
        q.execute("""CREATE TABLE IF NOT EXISTS shen_practice_rounds(
          id BIGSERIAL PRIMARY KEY, version TEXT NOT NULL, round_no INTEGER NOT NULL, report JSONB NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(version,round_no))""")
        q.execute("""CREATE TABLE IF NOT EXISTS shen_ai_control(
          singleton SMALLINT PRIMARY KEY CHECK(singleton=1), training_enabled BOOLEAN NOT NULL DEFAULT TRUE,
          adopted_round INTEGER, adopted_report JSONB, best_accuracy DOUBLE PRECISION NOT NULL DEFAULT 0,
          best_round INTEGER, best_report JSONB, research_target DOUBLE PRECISION NOT NULL DEFAULT 95,
          use_threshold DOUBLE PRECISION NOT NULL DEFAULT 80, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
        q.execute("""INSERT INTO shen_ai_control(singleton) VALUES(1) ON CONFLICT(singleton) DO NOTHING""")
    finally: core.db_release(c)

def load_dataset(limit=10000):
    """Load the immutable practice set once it has been locked in runtime.
    After live catch-up begins, new periods must not slide the 10k training window.
    """
    c=core.db_connect(retries=0)
    try:
      locked_start=locked_end=None
      with c.cursor(cursor_factory=core.RealDictCursor) as q:
        try:
          q.execute("SELECT dataset_start,dataset_end,dataset_size FROM shen_practice_runtime WHERE singleton=1")
          rr=q.fetchone()
          if rr and int(rr.get('dataset_size') or 0)>=int(limit):
            locked_start,locked_end=str(rr['dataset_start']),str(rr['dataset_end'])
        except Exception:
          c.rollback()
        if locked_start and locked_end:
          q.execute("""SELECT period_key,period_date,period_no,group_no,block_hash,single_count
            FROM period_groups WHERE group_no BETWEEN 1 AND 20 AND period_key BETWEEN %s AND %s
            ORDER BY period_date ASC,period_no ASC,group_no ASC""",(locked_start,locked_end))
        else:
          q.execute("""SELECT period_key,period_date,period_no,group_no,block_hash,single_count
            FROM period_groups WHERE group_no BETWEEN 1 AND 20
            ORDER BY period_date ASC,period_no ASC,group_no ASC""")
        periods={}
        for r in q.fetchall():
          k=str(r['period_key']); periods.setdefault(k,{'key':k,'date':str(r['period_date']),'period':int(r['period_no']),'groups':{}})
          periods[k]['groups'][int(r['group_no'])]={'single':int(r['single_count']),'hash':str(r['block_hash'])}
        arr=[p for p in periods.values() if len(p['groups'])==20]
        return arr if locked_start else arr[-int(limit):]
    finally: core.db_release(c)

def mode(vals, default=3):
    if not vals:return default
    c=Counter(vals); return min(c, key=lambda x:(-c[x],x))

def omission(seq):
    out={i:len(seq) for i in range(8)}
    for d in range(8):
      for j,x in enumerate(reversed(seq)):
        if x==d: out[d]=j; break
    return out

def hash_feature(h, kind):
    h=(h or '').upper()
    if not h:return 3
    if kind=='ae': return sum(1 for x in h if x in 'ABCDE')%8
    if kind=='tail': return sum(int(x,16) for x in h[-8:])%8
    if kind=='prefix': return int(h[:8],16)%8
    if kind=='suffix': return int(h[-8:],16)%8
    return 3

def predict_all(history, p):
    seq=[x['groups'][20]['single'] for x in history]
    gs=[p['groups'][g]['single'] for g in range(1,20)]
    hs=[p['groups'][g]['hash'] for g in range(1,20)]
    pred={}
    for n in (20,50,100,300): pred[f'freq{n}']=mode(seq[-n:])
    last=seq[-1]
    followers=[seq[i+1] for i in range(len(seq)-1) if seq[i]==last]
    pred['transition1']=mode(followers,mode(seq[-100:]))
    pair=tuple(seq[-2:]); followers2=[seq[i+2] for i in range(len(seq)-2) if tuple(seq[i:i+2])==pair]
    pred['transition2']=mode(followers2,pred['transition1'])
    om=omission(seq); pred['omission_max']=max(om,key=om.get)
    rates=Counter(seq[-300:]); pred['omission_ratio']=max(range(8),key=lambda x:(om[x]+1)/(rates[x]+1))
    for n in (7,20,60,120): pred[f'lag{n}']=seq[-n] if len(seq)>=n else pred['freq100']
    pred['recent_reversal']=max(range(8),key=lambda x:-Counter(seq[-12:])[x])
    pred['recent_momentum']=mode(seq[-8:])
    pred['g19']=gs[-1]; pred['g17']=gs[16]; pred['group_mode']=mode(gs)
    pred['group_weighted']=max(range(8),key=lambda x:sum((i+1) for i,v in enumerate(gs) if v==x))
    pred['hash_ae']=mode([hash_feature(h,'ae') for h in hs[-7:]])
    pred['hash_tail']=mode([hash_feature(h,'tail') for h in hs[-7:]])
    pred['hash_prefix']=mode([hash_feature(h,'prefix') for h in hs[-7:]])
    pred['hash_suffix']=mode([hash_feature(h,'suffix') for h in hs[-7:]])
    prevh=history[-1]['groups'][20]['hash']; pred['hash_delta']=(hash_feature(hs[-1],'suffix')-hash_feature(prevh,'suffix'))%8
    pred['hybrid_context']=mode([pred['transition1'],pred['transition2'],pred['group_mode'],pred['group_weighted'],pred['omission_ratio']])
    return pred

ODDS={0:236.499,1:24.667,2:6.571,3:3.451,4:3.256,5:5.674,6:19.597,7:172.575}
PAIR_GROUPS=[(3,4),(2,5),(1,6),(0,7)]

def omission_probability_report(data):
    """Empirical P(target appears next | its current omission >= k). Never labels 90% unless observed."""
    seq=[p['groups'][20]['single'] for p in data]
    out={}
    for d in range(8):
      gap=0; rows=[]
      # threshold k -> [opportunities,next-hit]
      agg={k:[0,0] for k in range(1,61)}
      gaps=[]
      for x in seq:
        if x==d:
          gaps.append(gap); gap=0
        else: gap+=1
        # after observing x, ask whether next period is d
        # filled in second chronological pass below
      gap=0
      for i,x in enumerate(seq[:-1]):
        gap=0 if x==d else gap+1
        for k in range(1,min(gap,60)+1):
          agg[k][0]+=1; agg[k][1]+=int(seq[i+1]==d)
      for k,(n,h) in agg.items():
        if n>=20:
          rows.append({'omission':k,'samples':n,'hits':h,'nextProbability':round(100*h/n,2),'reaches90':h/n>=.90})
      best=max(rows,key=lambda r:(r['nextProbability'],r['samples'])) if rows else None
      first90=next((r for r in rows if r['reaches90']),None)
      out[str(d)]={'best':best,'first90':first90,'thresholds':rows,'recentThresholds':rows[-20:]}
    return out

def pair_plan_report(data):
    seq=[p['groups'][20]['single'] for p in data]
    n=len(seq); plans=[]
    for a,b in PAIR_GROUPS:
      ca=seq.count(a); cb=seq.count(b); pair=ca+cb
      # Equalize gross return if both selections are covered; scale so smallest stake is 1.
      wa=1/ODDS[a]; wb=1/ODDS[b]; scale=1/min(wa,wb)
      sa=round(wa*scale,3); sb=round(wb*scale,3); cost=sa+sb
      gross_a=round(sa*ODDS[a],3); gross_b=round(sb*ODDS[b],3)
      plans.append({'pair':f'单{a}+单{b}','a':a,'b':b,'oddsA':ODDS[a],'oddsB':ODDS[b],
        'historyCount':pair,'historyRate':round(100*pair/n,2) if n else 0,
        'stakeExample':{'a':sa,'b':sb,'total':round(cost,3),'grossIfA':gross_a,'grossIfB':gross_b},
        'note':'仅为按赔率平衡两边毛返还的数学示例，不代表正收益或提高开奖概率。'})
    return plans


def chase_recovery_plans(target_profit=1, max_steps=30, max_payout=3000):
    """
    Per-outcome recovery schedule. Assumes displayed decimal odds are gross return
    (stake * odds) and platform max payout is 3000. Each next integer stake is the
    minimum that would recover all earlier stakes plus target_profit if that step hits.
    This is a bounded simulation, NOT a no-loss guarantee: a long miss streak or the
    payout cap can terminate the schedule.
    """
    plans=[]
    for d in range(8):
      odds=float(ODDS[d]); prior=0; steps=[]; stop_reason='达到演示步数上限'
      for step in range(1,int(max_steps)+1):
        # hit net = stake*odds - (prior+stake) = stake*(odds-1)-prior
        stake=max(1, int(math.ceil((prior+target_profit)/(odds-1))))
        gross=stake*odds
        if gross>max_payout+1e-9:
          stop_reason=f'下一步预计返还 {gross:.2f} 超过平台最高中奖 {max_payout}'
          break
        total=prior+stake; net=gross-total
        steps.append({'step':step,'stake':stake,'totalStaked':round(total,3),
          'grossIfHit':round(gross,3),'netIfHit':round(net,3)})
        prior=total
      plans.append({'single':d,'odds':odds,'targetProfit':target_profit,'minStake':1,
        'maxPayout':max_payout,'supportedSteps':len(steps),'steps':steps,'stopReason':stop_reason,
        'warning':'这是按赔率与3000封顶计算的回本跟单表，不是不会亏保证；连续未中超过表格、赔率/规则变化或资金不足都会产生亏损。'})
    return plans

def run_round(data, round_no=1):
    hits=Counter(); total=Counter(); recent_hits={m:[] for m in METHODS}; records=[]
    ensA_hits=ensB_hits=0; eval_n=0
    # immutable chronological holdout: final 20%; weights at t only use results before t.
    hold_start=max(MIN_HISTORY,int(len(data)*.8)); holdA=holdB=holdN=0
    for i in range(MIN_HISTORY,len(data)):
      hist=data[:i]; p=data[i]; actual=p['groups'][20]['single']; preds=predict_all(hist,p)
      for m,v in preds.items():
        total[m]+=1; ok=int(v==actual); hits[m]+=ok; recent_hits[m].append(ok); recent_hits[m]=recent_hits[m][-300:]
      # Scheme A: rolling-accuracy weighted vote, no future leakage.
      scores=defaultdict(float)
      for m,v in preds.items():
        rh=recent_hits[m][:-1]  # exclude current outcome just appended above
        w=(sum(rh)+2)/(len(rh)+8) if rh else .125
        scores[v]+=w
      a=max(range(8),key=lambda x:(scores[x],-x))
      # Scheme B: consensus of the currently best 7 experts, diversified by family.
      ranked=sorted(METHODS,key=lambda m:((sum(recent_hits[m][:-1])+1)/(len(recent_hits[m][:-1])+4) if len(recent_hits[m])>1 else 0),reverse=True)
      b=mode([preds[m] for m in ranked[:7]])
      ensA_hits+=int(a==actual); ensB_hits+=int(b==actual); eval_n+=1
      if i>=hold_start: holdA+=int(a==actual); holdB+=int(b==actual); holdN+=1
      if i>=len(data)-50: records.append({'period':p['key'],'actual':actual,'schemeA':a,'schemeB':b,'aHit':a==actual,'bHit':b==actual})
    metrics=[]
    for m in METHODS:
      metrics.append({'method':m,'hits':hits[m],'total':total[m],'accuracy':round(100*hits[m]/total[m],2) if total[m] else 0})
    metrics.sort(key=lambda x:x['accuracy'],reverse=True)
    ideas=[
      '所有预测严格按时间向前回放；预测当前期时不可读取该期第20组结果。',
      '哈希类方法只读取当前期第1-19组哈希与过去已开奖哈希，不读取目标第20组哈希。',
      '方案A按各专家最近300次已揭晓表现动态加权；表现下降会自动降权。',
      '方案B每期重新选择近期表现较好的7个专家做共识，避免永久绑定单一规律。',
      '最终20%作为时间顺序样本外区间；综合方案优先看样本外命中率，不用训练段最高值冒充真实能力。',
      '每轮保留完整成绩；后续轮次可改变窗口、专家组合与权重，但不得利用未来答案。',
      '新增遗漏条件概率：逐个分析单0–单7在不同遗漏阈值后“下一期出现”的历史条件概率；只有样本实际达到90%才标记90%，不会人为设定。',
      '新增四组赔率研究：单3/4、单2/5、单1/6、单0/7；赔率只参与资金回报模拟，不参与伪造开奖概率。',
      '新增单0–单7逐项跟单回本表：按最低下注1、最高中奖3000和各自赔率计算每一步最低整数下注；达到3000封顶即停止，不把有限步骤方案标成“永不亏”。'
    ]
    return {'version':VERSION,'round':round_no,'datasetSize':len(data),'datasetStart':data[0]['key'],'datasetEnd':data[-1]['key'],
      'evaluated':eval_n,'methods':metrics,'methodCount':len(METHODS),'schemeA':{'name':'动态滚动加权综合','accuracy':round(100*ensA_hits/eval_n,2),'hits':ensA_hits,'total':eval_n,'holdoutAccuracy':round(100*holdA/holdN,2) if holdN else 0,'holdoutTotal':holdN},
      'schemeB':{'name':'自适应优选专家共识','accuracy':round(100*ensB_hits/eval_n,2),'hits':ensB_hits,'total':eval_n,'holdoutAccuracy':round(100*holdB/holdN,2) if holdN else 0,'holdoutTotal':holdN},
      'ideas':ideas,'recentTests':records,'omissionProbability':omission_probability_report(data),
      'pairPlans':pair_plan_report(data),'chasePlans':chase_recovery_plans(),'odds':{str(k):v for k,v in ODDS.items()},
      'hashAnalysis':{'enabled':True,'methods':['hash_ae','hash_tail','hash_prefix','hash_suffix','hash_delta'],'rule':'只使用目标开奖前已知哈希特征；目标第20组哈希禁止进入预测特征。'},
      'generatedAt':datetime.utcnow().isoformat(timespec='seconds')+'Z'}

def save_report(report):
    c=core.db_connect(retries=0)
    try:
      with c:
       with c.cursor() as q:
        q.execute("""INSERT INTO shen_practice_runtime(singleton,version,status,round_no,dataset_start,dataset_end,dataset_size,report,updated_at)
          VALUES(1,%s,'running',%s,%s,%s,%s,%s::jsonb,NOW()) ON CONFLICT(singleton) DO UPDATE SET
          version=EXCLUDED.version,status=EXCLUDED.status,round_no=EXCLUDED.round_no,dataset_start=EXCLUDED.dataset_start,
          dataset_end=EXCLUDED.dataset_end,dataset_size=EXCLUDED.dataset_size,report=EXCLUDED.report,updated_at=NOW()""",
          (VERSION,report['round'],report['datasetStart'],report['datasetEnd'],report['datasetSize'],json.dumps(report)))
        q.execute("""INSERT INTO shen_practice_rounds(version,round_no,report) VALUES(%s,%s,%s::jsonb)
          ON CONFLICT(version,round_no) DO UPDATE SET report=EXCLUDED.report,created_at=NOW()""",(VERSION,report['round'],json.dumps(report)))
        best=max(float(report['schemeA'].get('holdoutAccuracy',0)),float(report['schemeB'].get('holdoutAccuracy',0)))
        q.execute("SELECT best_accuracy,adopted_round FROM shen_ai_control WHERE singleton=1 FOR UPDATE")
        ctl=q.fetchone() or (0,None); old=float(ctl[0] or 0); adopted=ctl[1]
        if best>old:
          q.execute("UPDATE shen_ai_control SET best_accuracy=%s,best_round=%s,best_report=%s::jsonb,updated_at=NOW() WHERE singleton=1",(best,report['round'],json.dumps(report)))
        # >=80% becomes usable automatically, but research keeps running toward 95%+. Never stops on threshold.
        if best>=USE_THRESHOLD and (adopted is None or best>=old):
          q.execute("UPDATE shen_ai_control SET adopted_round=%s,adopted_report=%s::jsonb,updated_at=NOW() WHERE singleton=1",(report['round'],json.dumps(report)))
    finally:core.db_release(c)

def _control():
    ensure_schema(); c=core.db_connect(retries=0)
    try:
      with c.cursor(cursor_factory=core.RealDictCursor) as q:
        q.execute("SELECT * FROM shen_ai_control WHERE singleton=1"); return dict(q.fetchone() or {})
    finally: core.db_release(c)

def set_training(enabled):
    ensure_schema(); c=core.db_connect(retries=0)
    try:
      with c:
       with c.cursor() as q:q.execute("UPDATE shen_ai_control SET training_enabled=%s,updated_at=NOW() WHERE singleton=1",(bool(enabled),))
    finally: core.db_release(c)

def adopt_current():
    ensure_schema(); c=core.db_connect(retries=0)
    try:
      with c:
       with c.cursor(cursor_factory=core.RealDictCursor) as q:
        q.execute("SELECT round_no,report FROM shen_practice_runtime WHERE singleton=1"); r=q.fetchone()
        if not r:return False
        rep=r['report']; rep=json.loads(rep) if isinstance(rep,str) else rep
        q.execute("UPDATE shen_ai_control SET adopted_round=%s,adopted_report=%s::jsonb,updated_at=NOW() WHERE singleton=1",(r['round_no'],json.dumps(rep)))
        return True
    finally: core.db_release(c)

def lab_status():
    ctl=_control(); br=bridge_report(); adopted=ctl.get('adopted_report') or {}; adopted=json.loads(adopted) if isinstance(adopted,str) else adopted
    return {'trainingEnabled':bool(ctl.get('training_enabled',True)),'bestAccuracy':round(float(ctl.get('best_accuracy') or 0),2),
      'bestRound':ctl.get('best_round'),'adoptedRound':ctl.get('adopted_round'),'useThreshold':float(ctl.get('use_threshold') or USE_THRESHOLD),
      'researchTarget':float(ctl.get('research_target') or RESEARCH_TARGET),'targetReached':float(ctl.get('best_accuracy') or 0)>=float(ctl.get('research_target') or RESEARCH_TARGET),
      'usable':float(ctl.get('best_accuracy') or 0)>=float(ctl.get('use_threshold') or USE_THRESHOLD),'handoff':br,'adoptedReport':adopted}

def worker():
    ensure_schema(); round_no=0
    c=core.db_connect(retries=0)
    try:
      with c.cursor() as q:q.execute("SELECT COALESCE(MAX(round_no),0) FROM shen_practice_rounds WHERE version=%s",(VERSION,)); round_no=int(q.fetchone()[0] or 0)
    finally: core.db_release(c)
    while True:
      try:
        if not _control().get('training_enabled',True): time.sleep(20); continue
        data=load_dataset(10000)
        if len(data)<10000: time.sleep(20); continue
        round_no+=1; report=run_round(data,round_no); save_report(report)
        best=max(report['schemeA']['holdoutAccuracy'],report['schemeB']['holdoutAccuracy'])
        print(f"[AI训练] 第{round_no}轮 | 本轮最佳={best}% | >=80可使用但继续研究 | 研究目标95%+",flush=True)
        time.sleep(300)
      except Exception as e:
        print('[练习错误]',type(e).__name__,str(e)[:240],flush=True); time.sleep(20)

def bridge_report():
    """Stable hand-off contract for v8god 0.0.6. Read-only summary of the locked 10k research result."""
    c=core.db_connect(retries=0)
    try:
      with c.cursor(cursor_factory=core.RealDictCursor) as q:
        q.execute("SELECT version,status,round_no,dataset_start,dataset_end,dataset_size,report,updated_at FROM shen_practice_runtime WHERE singleton=1")
        r=q.fetchone()
        if not r:return {'ready':False,'reason':'no_practice_report','schemaVersion':'1.0'}
        rep=r.get('report') or {}; rep=json.loads(rep) if isinstance(rep,str) else rep
        methods=rep.get('methods') or []
        top=sorted(methods,key=lambda x:(float(x.get('accuracy',0) or 0),int(x.get('total',0) or 0)),reverse=True)[:10]
        schemes=[]
        for key in ('schemeA','schemeB'):
          s=rep.get(key) or {}
          schemes.append({'id':key,'name':s.get('name'),'accuracy':s.get('accuracy',0),'holdoutAccuracy':s.get('holdoutAccuracy',0),'hits':s.get('hits',0),'total':s.get('total',0),'holdoutTotal':s.get('holdoutTotal',0)})
        best=max(schemes,key=lambda x:(float(x.get('holdoutAccuracy',0) or 0),int(x.get('holdoutTotal',0) or 0))) if schemes else None
        ready=int(r.get('dataset_size') or 0)>=10000
        qualified=bool(best and ready and float(best.get('holdoutAccuracy',0) or 0)>=80 and int(best.get('holdoutTotal',0) or 0)>0)
        return {'schemaVersion':'1.0','producer':'v8god-0.0.5-bridge','ready':ready,'qualified80':qualified,
          'dataset':{'start':r.get('dataset_start'),'end':r.get('dataset_end'),'size':int(r.get('dataset_size') or 0),'locked':ready},
          'training':{'status':r.get('status'),'round':int(r.get('round_no') or 0),'updatedAt':r.get('updated_at').isoformat() if r.get('updated_at') else None},
          'bestScheme':best,'schemes':schemes,'topMethods':top,'methodCount':int(rep.get('methodCount',len(methods)) or 0),
          'recentTests':rep.get('recentTests') or [],'ideas':rep.get('ideas') or [],
          'handoff':{'consumer':'v8god-0.0.6','rule':'0.0.6 must read server-side report; do not recompute or fabricate >=80% qualification.'}}
    finally: core.db_release(c)
