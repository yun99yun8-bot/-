"""v8god 0.0.4 strategy helpers: recommendation + user-defined backtest.
Research/simulation only. Never assumes a betting strategy guarantees profit.
"""
from collections import Counter
import math
import practice_engine as pe


def _gap_before(seq, i, d):
    g=0
    for x in reversed(seq[:i]):
        if x==d: break
        g+=1
    return g


def recommend(data, current_omission):
    """Rank singles by empirical omission condition + walk-forward expert consensus.
    Uses the fixed historical set only; no future live outcome is consumed.
    """
    if not data: return {'action':'WAIT','reason':'10000期数据尚未就绪','candidates':[]}
    op=pe.omission_probability_report(data)
    candidates=[]
    for d in range(8):
        gap=int((current_omission or {}).get(str(d),0) or 0)
        rows=op.get(str(d),{}).get('thresholds') or []
        eligible=[r for r in rows if int(r['omission'])<=gap and int(r['samples'])>=20]
        r=max(eligible,key=lambda z:(z['omission'],z['samples'])) if eligible else None
        prob=(float(r['nextProbability'])/100) if r else 0
        break_even=1/pe.ODDS[d]
        edge=prob-break_even if r else -1
        candidates.append({'single':d,'omission':gap,'samples':int(r['samples']) if r else 0,
          'empiricalNextProbability':round(prob*100,2),'breakEvenProbability':round(break_even*100,2),
          'edgePoints':round(edge*100,2),'odds':pe.ODDS[d]})
    candidates.sort(key=lambda x:(x['edgePoints'],x['samples']),reverse=True)
    best=candidates[0]
    # conservative gate: enough observations and positive empirical margin; otherwise WAIT.
    action='WATCH' if best['samples']>=30 and best['edgePoints']>0 else 'WAIT'
    reason=(f"单{best['single']}当前遗漏{best['omission']}期；历史相近阈值下一期出现率{best['empiricalNextProbability']}%，"
            f"赔率盈亏平衡概率{best['breakEvenProbability']}%，样本{best['samples']}次。") if best['samples'] else '当前遗漏条件缺少足够历史样本，继续观察。'
    return {'action':action,'reason':reason,'candidates':candidates[:4],
            'warning':'历史条件概率和赔率模拟不能保证未来盈利；样本不足或没有正的历史优势时建议保持WAIT。'}


def backtest(data, cfg):
    targets=sorted({int(x) for x in cfg.get('targets',[3,4]) if 0<=int(x)<=7})
    if not targets: targets=[3,4]
    min_gap=max(0,min(100,int(cfg.get('minOmission',0))))
    stake=max(1,min(10000,float(cfg.get('stake',1))))
    stop_loss=max(0,float(cfg.get('stopLoss',0)))
    take_profit=max(0,float(cfg.get('takeProfit',0)))
    seq=[p['groups'][20]['single'] for p in data]
    pnl=0.0; peak=0.0; max_dd=0.0; bets=hits=0; longest_loss=loss_run=0
    # Equal fixed stake per selected outcome. Trigger only if any selected target meets omission threshold.
    for i in range(max(120,1),len(seq)):
        active=[d for d in targets if _gap_before(seq,i,d)>=min_gap]
        if not active: continue
        cost=stake*len(active); pnl-=cost; bets+=1
        actual=seq[i]
        if actual in active:
            pnl+=stake*pe.ODDS[actual]; hits+=1; loss_run=0
        else:
            loss_run+=1; longest_loss=max(longest_loss,loss_run)
        peak=max(peak,pnl); max_dd=max(max_dd,peak-pnl)
        if stop_loss and pnl<=-stop_loss: break
        if take_profit and pnl>=take_profit: break
    return {'targets':targets,'minOmission':min_gap,'stakeEach':stake,'bets':bets,'hits':hits,
      'hitRate':round(100*hits/bets,2) if bets else 0,'net':round(pnl,3),'maxDrawdown':round(max_dd,3),
      'longestLossStreak':longest_loss,'note':'固定10000期历史回放；结果是历史模拟，不代表未来收益。'}
