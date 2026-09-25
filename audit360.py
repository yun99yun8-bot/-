"""Independent 360-rule audit for the immutable 10k training set.
Read-only: never rewrites lottery/history rows.
"""
import json
from datetime import date
import collector_core as core

ANCHOR_DATE='2026-09-24'; ANCHOR_PERIOD=481; ANCHOR_BLOCK=86511906; SWITCH_EVERY=360

def _idx(ds,p):
    return date.fromisoformat(str(ds)).toordinal()*1440+(int(p)-1)
ANCHOR_IDX=_idx(ANCHOR_DATE,ANCHOR_PERIOD)

def reference_target(ds,p):
    """Independent reference implementation: +20/period; every 360 periods cumulative -2 correction."""
    delta=_idx(ds,p)-ANCHOR_IDX
    switches=delta//SWITCH_EVERY
    return ANCHOR_BLOCK + delta*20 - switches*2

def _norm_nums(v):
    if isinstance(v,str): v=json.loads(v)
    return [f'{int(x):02d}' for x in (v or [])]

def expected_unique_raw_count(start_idx,end_idx):
    sds,sp=_period_from_idx(start_idx); eds,ep=_period_from_idx(end_idx)
    first=reference_target(sds,sp)-19; last=reference_target(eds,ep)
    return last-first+1,first,last

def _period_from_idx(i):
    o,z=divmod(int(i),1440); return date.fromordinal(o).isoformat(),z+1

def run_audit(limit=10000, mismatch_limit=200):
    c=core.db_connect(retries=0)
    if c is None: raise RuntimeError('database unavailable')
    try:
      with c.cursor(cursor_factory=core.RealDictCursor) as q:
        q.execute("""SELECT period_key,period_date,period_no,COUNT(*) AS n
          FROM period_groups GROUP BY period_key,period_date,period_no HAVING COUNT(*)=20
          ORDER BY period_date DESC,period_no DESC LIMIT %s""",(int(limit),))
        ps=list(reversed(q.fetchall()))
        if not ps: return {'pass':False,'reason':'no_complete_periods','checkedPeriods':0}
        keys=[r['period_key'] for r in ps]
        q.execute("""SELECT period_key,period_date,period_no,group_no,target_block,block_number,block_hash,numbers,single_count
          FROM period_groups WHERE period_key = ANY(%s) ORDER BY period_date,period_no,group_no""",(keys,))
        rows=q.fetchall()
        by={}
        for r in rows: by.setdefault(r['period_key'],[]).append(r)
        minbn=min(int(r['block_number']) for r in rows); maxbn=max(int(r['block_number']) for r in rows)
        q.execute("SELECT block_number,block_hash FROM historical_raw_blocks WHERE block_number BETWEEN %s AND %s",(minbn,maxbn))
        raw={int(r['block_number']):str(r['block_hash']).upper() for r in q.fetchall()}
        q.execute("SELECT COUNT(*),COUNT(DISTINCT block_number) FROM historical_raw_blocks WHERE block_number BETWEEN %s AND %s",(minbn,maxbn))
        raw_count,raw_distinct=map(int,q.fetchone())

      errors=[]; counts={'mapping':0,'groupBlock':0,'rawHash':0,'hashRule':0,'numbers':0,'single':0}
      prev_idx=None
      for p in ps:
        ds=p['period_date'].isoformat() if hasattr(p['period_date'],'isoformat') else str(p['period_date']); pn=int(p['period_no']); key=p['period_key']
        idx=_idx(ds,pn); target=reference_target(ds,pn)
        if prev_idx is not None and idx!=prev_idx+1 and len(errors)<mismatch_limit: errors.append({'period':key,'type':'period_gap','expectedIndex':prev_idx+1,'actualIndex':idx})
        prev_idx=idx
        rr=by.get(key,[])
        if len(rr)!=20:
          if len(errors)<mismatch_limit: errors.append({'period':key,'type':'group_count','actual':len(rr)})
          continue
        for r in rr:
          g=int(r['group_no']); expected_bn=target-(20-g); h=str(r['block_hash']).upper()
          if int(r['target_block'])==target: counts['mapping']+=1
          elif len(errors)<mismatch_limit: errors.append({'period':key,'group':g,'type':'target_mapping','stored':int(r['target_block']),'expected':target})
          if int(r['block_number'])==expected_bn: counts['groupBlock']+=1
          elif len(errors)<mismatch_limit: errors.append({'period':key,'group':g,'type':'group_block','stored':int(r['block_number']),'expected':expected_bn})
          rh=raw.get(expected_bn)
          if rh and rh==h: counts['rawHash']+=1
          elif len(errors)<mismatch_limit: errors.append({'period':key,'group':g,'type':'raw_hash','block':expected_bn,'rawPresent':bool(rh)})
          try:
            calc=core.calc_numbers(h); counts['hashRule']+=1
            calc_nums=[f'{n:02d}' for n in calc]
            if _norm_nums(r['numbers'])==calc_nums: counts['numbers']+=1
            elif len(errors)<mismatch_limit: errors.append({'period':key,'group':g,'type':'numbers','stored':_norm_nums(r['numbers']),'expected':calc_nums})
            sc=core.calc_single_count(calc)
            if int(r['single_count'])==sc: counts['single']+=1
            elif len(errors)<mismatch_limit: errors.append({'period':key,'group':g,'type':'single','stored':int(r['single_count']),'expected':sc})
          except Exception as e:
            if len(errors)<mismatch_limit: errors.append({'period':key,'group':g,'type':'hash_rule_error','message':str(e)[:120]})
      total=len(ps)*20
      start_idx=_idx(ps[0]['period_date'],ps[0]['period_no']); end_idx=_idx(ps[-1]['period_date'],ps[-1]['period_no'])
      expected_raw,first,last=expected_unique_raw_count(start_idx,end_idx)
      anchors={
        '481': reference_target('2026-09-24',481),
        '482': reference_target('2026-09-24',482),
        '1201': reference_target('2026-09-24',1201),
      }
      passed=(len(ps)==limit and not errors and all(v==total for v in counts.values()))
      return {'pass':passed,'rule':'anchor 2026-09-24#0481=86511906; +20/period; cumulative -2 each 360 periods',
        'checkedPeriods':len(ps),'checkedGroups':total,'datasetStart':keys[0],'datasetEnd':keys[-1],
        'checks':counts,'expectedEach':total,'mismatchCount':len(errors),'mismatches':errors,
        'rawWindow':{'first':first,'last':last,'expectedUnique':expected_raw,'storedInWindow':raw_count,'distinct':raw_distinct,'note':'360切换会让相邻期复用2个区块，因此10000期不等于200000个唯一原始区块。'},
        'referenceAnchors':anchors,'aiTrainingAllowed':passed}
    finally: core.db_release(c)
