"""Focused regression checks for the V9 research weights and pre-result evidence."""
import ast
import json
import math
import unittest
from pathlib import Path


# Load only the pure research functions so tests do not require a live Flask,
# TRON, or PostgreSQL service.
source = ast.parse(Path(__file__).with_name('app.py').read_text())
names = {'_norm_scores', '_candidate_rank', 'research_model_performance',
         '_hash_context_scores', '_relation_scores', 'v9_research_ensemble',
         'should_poll_target', 'outside_candidate'}
module = ast.Module(body=[n for n in source.body if isinstance(n, ast.FunctionDef)
                          and n.name in names], type_ignores=[])
scope = {'json': json, 'RealDictCursor': object()}
exec(compile(module, '<research>', 'exec'), scope)


class Cursor:
    def __init__(self, rows): self.rows = rows; self.query = ''
    def __enter__(self): return self
    def __exit__(self, *_): pass
    def execute(self, query, params): self.query = query
    def fetchall(self): return self.rows


class Connection:
    def __init__(self, rows): self.cur = Cursor(rows)
    def cursor(self, **_): return self.cur


class ResearchTests(unittest.TestCase):
    def test_outside_candidate_is_independent_of_formal_top3(self):
        scores={'0':1.0,'1':6.0,'2':17.0,'3':26.0,'4':28.0,
                '5':18.0,'6':3.0,'7':1.0}
        self.assertEqual(scope['outside_candidate'](scores),{'single':5,'score':18.0})
        self.assertIsNone(scope['outside_candidate']({}))

    def test_target_lookup_starts_at_g17_or_final_twelve_seconds(self):
        should_poll=scope['should_poll_target']
        self.assertFalse(should_poll(96,100,42))
        self.assertTrue(should_poll(97,100,42))
        self.assertTrue(should_poll(None,100,12))
        self.assertFalse(should_poll(None,100,13))

    def test_no_evidence_uses_labelled_exploratory_weights(self):
        scope['db_connect'] = lambda: None
        result = scope['v9_research_ensemble']({}, [2, 3, 4])
        self.assertEqual(result['weights']['binomial'], 0.25)
        self.assertGreater(result['weights']['structure17'], 0)
        self.assertEqual(result['research']['edgeStatus'], 'NO_EDGE')
        self.assertEqual(result['research']['weightMode'], 'exploratory_prior_plus_verified_bonus')

    def test_strong_pre_result_structure_can_move_top1_beyond_three_four(self):
        scope['db_connect'] = lambda: None
        groups = {str(i): {'singleCount': 2} for i in range(1, 18)}
        relation = {'ranking': [{'group': i, 'matchRate': 60} for i in range(1, 7)]}
        result = scope['v9_research_ensemble'](groups, [2, 3, 4] * 30, relation)
        self.assertEqual(result['single'], 2)
        self.assertEqual(result['top3'][0], 2)

    def test_candidate_is_scored_against_same_period_baseline(self):
        baseline = {str(i): (8 if i == 3 else 1) for i in range(8)}
        candidate = {str(i): (8 if i == 4 else 1) for i in range(8)}
        db = Connection([{'actual_single': 4, 'ensemble_detail':
                          {'components': {'binomial': baseline, 'short': candidate}}}])
        scope['db_connect'] = lambda: db
        scope['db_release'] = lambda _: None
        performance = scope['research_model_performance']()
        self.assertEqual(performance['short']['pairedWins'], 1)
        self.assertEqual(performance['short']['pairedLosses'], 0)
        self.assertIn('p.locked_at < b.block_time', db.cur.query)
        self.assertIn("p.model_version LIKE 'v9.%%'", db.cur.query)

    def test_verified_positive_lift_can_gain_weight(self):
        scope['research_model_performance'] = lambda _: {
            'short': {'n': 200, 'pairedLift': 15.0, 'pairedSE': 2.0,
                      'top1Rate': 45.0},
            'binomial': {'n': 200, 'top1Rate': 30.0}}
        result = scope['v9_research_ensemble']({}, [3, 4] * 60)
        self.assertGreater(result['weights']['short'], 0)
        self.assertEqual(result['research']['edgeStatus'], 'EVIDENCE')


if __name__ == '__main__': unittest.main()
