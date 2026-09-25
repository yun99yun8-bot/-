"""Compare preregistered feature families on chronological future periods.

No target-period G20 fields are inputs. Candidate selection happens on tuning
periods; the final holdout is used only to accept or reject that selection.
"""
from collections import Counter, defaultdict
from math import exp, log

BASE = [1/128,7/128,21/128,35/128,35/128,21/128,7/128,1/128]
FEATURE_FAMILIES = {
    'history_only': ('history_last','history_previous','history_mode20','history_odd20'),
    'group_positions': ('last_single','penultimate_single','recent_odd','group_mode',
                        'position_1','position_5','position_9','position_13'),
    'hash_shape': ('suffix_digit','letter_band','digit_band','g17_tail2',
                   'g13_tail2','g17_hex_odd','g17_letters','g17_digits'),
}
FEATURE_FAMILIES['combined']=tuple(dict.fromkeys(
    name for names in FEATURE_FAMILIES.values() for name in names))
FEATURE_NAMES=FEATURE_FAMILIES['combined']
LEGACY_FEATURE_NAMES=('last_single','penultimate_single','recent_odd','group_mode',
                      'suffix_digit','letter_band','digit_band')
MIN_PERIODS = 400


def _history_features(recent_labels):
    """Newest-first labels strictly preceding the target period."""
    recent=[int(v) for v in (recent_labels or []) if 0<=int(v)<=7]
    window=recent[:20]; counts=Counter(window)
    return {'history_last':str(recent[0]) if recent else 'none',
            'history_previous':str(recent[1]) if len(recent)>1 else 'none',
            'history_mode20':str(min(range(8),key=lambda i:(-counts[i],i))) if window else 'none',
            'history_odd20':str(min(4,sum(v%2 for v in window)//5))}


def history_only_scores(snapshot,recent_labels):
    """Shadow forecast using no block or result from the target period."""
    if not snapshot or not snapshot.get('historyModel'):return None
    alpha=float(snapshot.get('historyAlpha') or 0)
    return _blended(snapshot['historyModel'],_history_features(recent_labels),alpha)


def features(groups, recent_labels=None):
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
    result={'last_single':str(singles[-1]),
            'penultimate_single':str(singles[-2]),
            'recent_odd':str(sum(s%2 for s in singles[-5:])),
            'group_mode':str(mode),
            'suffix_digit':right if right.isdigit() else 'letter',
            'letter_band':str(min(3,sum(c in 'ABCDE' for c in tail)//4)),
            'digit_band':str(min(3,sum(c.isdigit() for c in tail)//12)),
            'g17_tail2':hashes[-1][-2],
            'g13_tail2':hashes[12][-2],
            'g17_hex_odd':str(sum(int(c,16)%2 for c in hashes[-1][-16:])//4),
            'g17_letters':str(sum(c in 'ABCDE' for c in hashes[-1][-16:])//4),
            'g17_digits':str(sum(c.isdigit() for c in hashes[-1][-16:])//4)}
    for i in (1,5,9,13):result[f'position_{i}']=str(singles[i-1])
    result.update(_history_features(recent_labels))
    return result


def build_examples(rows):
    """Rows must arrive in chronological period and group order."""
    examples=[]; key=None; groups={}; label=None; prior=[]
    def finish():
        x=features(groups,prior)
        if x is not None and label is not None and 0<=label<=7:
            examples.append((key,x,label))
        if label is not None and 0<=label<=7:
            prior.insert(0,label)
            if len(prior)>30:prior.pop()
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


def fit(examples, feature_names=FEATURE_NAMES):
    labels=[0]*8; counts={n:[defaultdict(int) for _ in range(8)] for n in feature_names}
    vocabulary={n:set() for n in feature_names}
    for _,x,y in examples:
        labels[y]+=1
        for name in feature_names:
            value=x[name]; vocabulary[name].add(value); counts[name][y][value]+=1
    total=len(examples)
    return {'total':total,'labels':labels,'featureNames':list(feature_names),
            'counts':{name:[dict(d) for d in counts[name]] for name in feature_names},
            'vocab':{name:sorted(v) for name,v in vocabulary.items()}}


def predict(model,x):
    if not model or not x or model.get('total',0)<1:return list(BASE)
    n=model['total']; vocab=model['vocab']; counts=model['counts']; labels=model['labels']
    logits=[]
    for y in range(8):
        prior=(labels[y]+32*BASE[y])/(n+32)
        value=log(prior)
        for name in model.get('featureNames',LEGACY_FEATURE_NAMES):
            k=max(2,len(vocab[name])+1)
            freq=counts[name][y].get(x[name],0)
            value+=0.35*log((freq+4)/(labels[y]+4*k))
        logits.append(value)
    scale=max(logits); raw=[exp(v-scale) for v in logits]; z=sum(raw)
    return [v/z for v in raw]


def _logloss(pred,y):
    return -log(max(1e-12,pred[y]))


def _blended(model,x,alpha):
    learned=predict(model,x)
    return [(1-alpha)*BASE[i]+alpha*learned[i] for i in range(8)]


def _evaluation(model,alpha,examples):
    loss=0.0; hits=0; base_hits=0; baseline_loss=0.0
    for _,x,y in examples:
        scores=_blended(model,x,alpha)
        loss+=_logloss(scores,y);baseline_loss+=_logloss(BASE,y)
        hits+=int(max(range(8),key=lambda i:(scores[i],-i))==y)
        base_hits+=int(BASE.index(max(BASE))==y)
    size=len(examples)
    return {'n':size,'top1':hits,'baselineTop1':base_hits,
            'loss':round(loss/size,5) if size else None,
            'baselineLoss':round(baseline_loss/size,5) if size else None}


def train_snapshot(examples):
    """Rank feature families on tuning only, validate the winner on held-out periods."""
    examples=sorted(examples,key=lambda item:item[0])
    n=len(examples)
    if n<MIN_PERIODS:
        return {'status':'insufficient','sample':n,'active':False,
                'trainedThrough':examples[-1][0] if examples else None}
    train_end=int(n*.70); tune_end=int(n*.85)
    training=examples[:train_end]; tuning=examples[train_end:tune_end]; test=examples[tune_end:]
    choices=(0.15,0.30,0.50,0.75,1.0)
    candidates=[]; models={}
    for family,names in FEATURE_FAMILIES.items():
        model=fit(training,names);models[family]=model
        for alpha in choices:
            stats=_evaluation(model,alpha,tuning)
            candidates.append((stats['loss'],-stats['top1'],family,alpha))
    _,_,selected_family,alpha=min(candidates)
    selected=models[selected_family]
    test_stats=_evaluation(selected,alpha,test)
    mid=len(test)//2
    halves=[_evaluation(selected,alpha,part) for part in (test[:mid],test[mid:])]
    baseline_hits=test_stats['baselineTop1']; selected_hits=test_stats['top1']
    baseline_loss=test_stats['baselineLoss'];selected_loss=test_stats['loss']
    # Multiple families compete on tuning, so require independent gains in
    # both chronological holdout halves before a selected family is activated.
    active=(len(test)>=60 and baseline_loss-selected_loss>0.005
            and selected_hits>=baseline_hits+3
            and all(h['top1']>h['baselineTop1'] and
                    h['loss']<h['baselineLoss'] for h in halves))
    reports={}
    for family,model in models.items():
        tuned=min(choices,key=lambda a:(_evaluation(model,a,tuning)['loss'],a))
        holdout=_evaluation(model,tuned,test)
        reports[family]={'tunedAlpha':tuned,'tune':_evaluation(model,tuned,tuning),
                         'holdout':holdout,'selected':family==selected_family}
    return {'status':'validated' if active else 'no_edge','sample':n,
            'trainSample':len(training),'tuneSample':len(tuning),'testSample':len(test),
            'trainedThrough':examples[-1][0],'alpha':alpha if active else 0.0,
            'selectedFamily':selected_family,'featureFamilies':reports,
            'holdoutHalves':halves,'baselineLoss':baseline_loss,'modelLoss':selected_loss,
            'baselineTop1':baseline_hits,'modelTop1':selected_hits,'active':active,
            'model':fit(examples,FEATURE_FAMILIES[selected_family]) if active else None,
            'historyModel':fit(examples,FEATURE_FAMILIES['history_only']),
            'historyAlpha':reports['history_only']['tunedAlpha']}


def snapshot_scores(snapshot,groups,historical=None):
    x=features(groups,historical)
    if x is None or not snapshot or not snapshot.get('active') or not snapshot.get('model'):
        return None
    alpha=float(snapshot['alpha']); learned=predict(snapshot['model'],x)
    return {i:(1-alpha)*BASE[i]+alpha*learned[i] for i in range(8)}
