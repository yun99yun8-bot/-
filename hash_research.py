"""Train an auditable, bounded hash-feature model from completed periods.

Features use only group 1..17. Group 20 is a label, never an input. The
algorithm deliberately keeps low-cardinality aggregates instead of memorizing
individual hashes: a digest suffix is not a reliable future-hash formula.
"""
from collections import Counter, defaultdict
from math import exp, log

BASE = [1/128,7/128,21/128,35/128,35/128,21/128,7/128,1/128]
FEATURE_NAMES = ('last_single','penultimate_single','recent_odd','group_mode',
                 'suffix_digit','letter_band','digit_band')
MIN_PERIODS = 400


def features(groups):
    """A bounded signature of the seventeen pre-result block hashes."""
    if not all(str(i) in groups for i in range(1,18)):
        return None
    singles=[]; hashes=[]
    for i in range(1,18):
        row=groups[str(i)]
        h=str(row.get('block') or row.get('block_hash') or '').upper()
        try: single=int(row.get('singleCount',row.get('single_count')))
        except (TypeError,ValueError): return None
        if len(h)!=64 or any(c not in '0123456789ABCDEF' for c in h) or not 0<=single<=7:
            return None
        hashes.append(h); singles.append(single)
    tail=''.join(h[-16:] for h in hashes[-3:])
    count=Counter(singles)
    mode=min(range(8),key=lambda i:(-count[i],i))
    right=hashes[-1][-1]
    return {'last_single':str(singles[-1]),
            'penultimate_single':str(singles[-2]),
            'recent_odd':str(sum(s%2 for s in singles[-5:])),
            'group_mode':str(mode),
            'suffix_digit':right if right.isdigit() else 'letter',
            'letter_band':str(min(3,sum(c in 'ABCDE' for c in tail)//4)),
            'digit_band':str(min(3,sum(c.isdigit() for c in tail)//12))}


def build_examples(rows):
    """Rows must arrive in chronological period and group order."""
    examples=[]; key=None; groups={}; label=None
    def finish():
        x=features(groups)
        if x is not None and label is not None and 0<=label<=7:
            examples.append((key,x,label))
    for row in rows:
        incoming=str(row['period_key'])
        if key is not None and incoming!=key:
            finish();groups={};label=None
        key=incoming
        g=int(row['group_no'])
        if 1<=g<=17:groups[str(g)]=row
        elif g==20:
            try: label=int(row['single_count'])
            except (TypeError,ValueError): label=None
    if key is not None:finish()
    return examples


def fit(examples):
    labels=[0]*8; counts={n:[defaultdict(int) for _ in range(8)] for n in FEATURE_NAMES}
    vocabulary={n:set() for n in FEATURE_NAMES}
    for _,x,y in examples:
        labels[y]+=1
        for name in FEATURE_NAMES:
            value=x[name]; vocabulary[name].add(value); counts[name][y][value]+=1
    total=len(examples)
    return {'total':total,'labels':labels,
            'counts':{name:[dict(d) for d in counts[name]] for name in FEATURE_NAMES},
            'vocab':{name:sorted(v) for name,v in vocabulary.items()}}


def predict(model,x):
    if not model or not x or model.get('total',0)<1:return list(BASE)
    n=model['total']; vocab=model['vocab']; counts=model['counts']; labels=model['labels']
    logits=[]
    for y in range(8):
        prior=(labels[y]+32*BASE[y])/(n+32)
        value=log(prior)
        for name in FEATURE_NAMES:
            k=max(2,len(vocab[name])+1)
            freq=counts[name][y].get(x[name],0)
            value+=0.35*log((freq+4)/(labels[y]+4*k))
        logits.append(value)
    scale=max(logits); raw=[exp(v-scale) for v in logits]; z=sum(raw)
    return [v/z for v in raw]


def _logloss(pred,y):
    return -log(max(1e-12,pred[y]))


def train_snapshot(examples):
    """Chronological train/tune/test; test is never used to choose blend."""
    examples=sorted(examples,key=lambda item:item[0])
    n=len(examples)
    if n<MIN_PERIODS:
        return {'status':'insufficient','sample':n,'active':False,
                'trainedThrough':examples[-1][0] if examples else None}
    train_end=int(n*.70); tune_end=int(n*.85)
    training=examples[:train_end]; tuning=examples[train_end:tune_end]; test=examples[tune_end:]
    model=fit(training)
    choices=(0.0,0.15,0.30,0.50)
    def blended(x,a):
        learned=predict(model,x)
        return [(1-a)*BASE[i]+a*learned[i] for i in range(8)]
    alpha=min(choices,key=lambda a:sum(_logloss(blended(x,a),y) for _,x,y in tuning))
    baseline_loss=sum(_logloss(BASE,y) for _,x,y in test)/len(test)
    selected_loss=sum(_logloss(blended(x,alpha),y) for _,x,y in test)/len(test)
    baseline_hits=sum(BASE.index(max(BASE))==y for _,x,y in test)
    selected_hits=sum(max(range(8),key=lambda i:(blended(x,alpha)[i],-i))==y for _,x,y in test)
    # A single validation pass is evidence for a cautious blend, not proof of
    # predictability. Never activate a model that loses to the simple baseline.
    active=(len(test)>=60 and alpha>0 and baseline_loss-selected_loss>0.005
            and selected_hits>=baseline_hits)
    return {'status':'validated' if active else 'no_edge','sample':n,
            'trainSample':len(training),'tuneSample':len(tuning),'testSample':len(test),
            'trainedThrough':examples[-1][0],'alpha':alpha if active else 0.0,
            'baselineLoss':round(baseline_loss,5),'modelLoss':round(selected_loss,5),
            'baselineTop1':baseline_hits,'modelTop1':selected_hits,'active':active,
            'model':fit(examples) if active else None}


def snapshot_scores(snapshot,groups):
    x=features(groups)
    if x is None or not snapshot or not snapshot.get('active') or not snapshot.get('model'):
        return None
    alpha=float(snapshot['alpha']); learned=predict(snapshot['model'],x)
    return {i:(1-alpha)*BASE[i]+alpha*learned[i] for i in range(8)}
