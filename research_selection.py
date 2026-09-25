"""Choose research candidates from genuinely pre-result, newly verified rows.

Selection does not change old forecasts and does not publish a betting pick.
The baseline is always scored on the same periods as each candidate.
"""
from collections import defaultdict
from math import sqrt


def choose_candidate(rows, minimum=300):
    grouped=defaultdict(list)
    for row in rows:
        name=str(row['candidate'])
        if name.startswith(('fixed_','trial_')):continue
        actual=int(row['actual_single']);pick=int(row['prediction'])
        grouped[name].append((str(row['period_key']),actual,pick))
    report={}; contenders=[]
    for name,values in sorted(grouped.items()):
        # Input is latest -> oldest. Compare only predictions that were already
        # locked and verified; validation of that condition belongs to SQL.
        recent=values[:100];n=len(values)
        hits=sum(actual==pick for _,actual,pick in values)
        base=sum(actual==3 for _,actual,_ in values)
        wins=sum(pick==actual and actual!=3 for _,actual,pick in values)
        losses=sum(actual==3 and pick!=3 for _,actual,pick in values)
        recent_wins=sum(pick==actual and actual!=3 for _,actual,pick in recent)
        recent_losses=sum(actual==3 and pick!=3 for _,actual,pick in recent)
        margin=wins-losses-3*sqrt(wins+losses)
        report[name]={'valid':n,'hits':hits,'baseline3Hits':base,
                      'pairedWins':wins,'pairedLosses':losses,
                      'recentPairedWins':recent_wins,'recentPairedLosses':recent_losses,
                      'conservativeMargin':round(margin,2)}
        if n>=minimum and margin>0 and recent_wins>=recent_losses:
            contenders.append((margin/n,hits/n,name))
    winner=max(contenders)[2] if contenders else None
    return {'status':'candidate_found' if winner else 'insufficient_evidence',
            'candidate':winner,'minimumValid':minimum,'candidates':report}
