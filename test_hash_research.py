import random
import unittest

import hash_research as model


def example(period, signal, actual, missing=None):
    rows=[]
    for group in range(1,18):
        if group==missing:continue
        suffix=signal if group==17 else '0'
        rows.append({'period_key':f'2026-09-24:{period:04d}',
                     'group_no':group,'block_hash':'a'*63+suffix,
                     'single_count':3 if group<17 else 4})
    rows.append({'period_key':f'2026-09-24:{period:04d}',
                 'group_no':20,'block_hash':'f'*64,'single_count':actual})
    return rows


class HashResearchTests(unittest.TestCase):
    def test_historical_shadow_uses_only_prior_outcomes(self):
        rows=example(1,'1',2)+example(2,'9',5)+example(3,'1',2)
        earlier=model.build_examples(rows)
        self.assertEqual(earlier[1][1]['history_last'],'2')
        self.assertEqual(earlier[2][1]['history_last'],'5')
        changed=example(1,'1',2)+example(2,'9',5)+example(3,'1',7)
        self.assertEqual(model.build_examples(changed)[2][1],earlier[2][1])

    def test_complete_period_and_pre_result_only(self):
        rows=example(1,'1',2)+example(2,'9',5,missing=16)
        found=model.build_examples(rows)
        self.assertEqual(len(found),1)
        before=found[0][1]
        rows[17]['block_hash']='0'*64  # change group 20 label hash only
        self.assertEqual(model.build_examples(rows)[0][1],before)
        self.assertEqual(before['suffix_digit'],'1')

    def test_train_on_old_periods_validate_on_later_periods(self):
        rows=[]
        for period in range(1,801):
            suffix='1' if period%2 else '9'
            rows+=example(period,suffix,2 if suffix=='1' else 5)
        snap=model.train_snapshot(model.build_examples(rows))
        self.assertTrue(snap['active'])
        self.assertEqual(snap['sample'],800)
        self.assertLess(snap['modelLoss'],snap['baselineLoss'])
        self.assertEqual(set(snap['featureFamilies']),set(model.FEATURE_FAMILIES))
        self.assertIn(snap['selectedFamily'],model.FEATURE_FAMILIES)
        shadow=model.history_only_scores(snap,[5,2,5,2])
        self.assertEqual(len(shadow),8)
        self.assertAlmostEqual(sum(shadow),1.0)
        current={str(r['group_no']):{'block':r['block_hash'],'singleCount':r['single_count']}
                 for r in example(801,'1',2) if r['group_no']<=17}
        scores=model.snapshot_scores(snap,current)
        self.assertGreater(scores[2],scores[3])

    def test_no_historical_relation_does_not_activate(self):
        rng=random.Random(12); rows=[]
        for period in range(1,601):
            rows+=example(period,rng.choice(['1','9']),rng.choices(range(8),weights=[1,7,21,35,35,21,7,1])[0])
        snap=model.train_snapshot(model.build_examples(rows))
        self.assertFalse(snap['active'])
        self.assertIsNone(model.snapshot_scores(snap,{}))


if __name__=='__main__':unittest.main()
