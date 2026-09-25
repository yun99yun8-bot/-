"""The score denominator must contain only genuine pre-result, aligned rows."""
import ast
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import unittest

tree=ast.parse(Path(__file__).with_name('app.py').read_text())
funcs={'period_index','period_target_block','ai_audit_summary'}
consts={'TAIL_ANCHOR_INDEX','TAIL_INTERVAL_PERIODS','TAIL_SEQUENCE'}
nodes=[n for n in tree.body if (isinstance(n,ast.FunctionDef) and n.name in funcs)
       or (isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id in consts for t in n.targets))]
scope={'datetime':datetime,'json':json}
exec(compile(ast.Module(body=nodes,type_ignores=[]),'<audit>','exec'),scope)


class AIAuditTests(unittest.TestCase):
    def test_invalid_rows_cannot_be_counted_as_model_misses(self):
        target=scope['period_target_block']('2026-09-25',275)
        publication=datetime(2026,9,24,20,36,tzinfo=timezone.utc)
        row={'period_date':date(2026,9,25),'period_no':275,'period_key':'2026-09-25:0275',
             'target_block':target,'block_number':target,'block_time':publication,
             'locked_at':publication-timedelta(seconds=8),'chain_single':3,
             'actual_single':3,'ai_analysis':3,'prediction_top3':[3,4,2],
             'model_version':'v9.4.8-audit-prediction-distribution-1'}
        hit=dict(row)
        second=dict(row,period_no=274,period_key='2026-09-25:0274',
                    target_block=target-20,block_number=target-20,ai_analysis=4)
        wrong_target=dict(row,target_block=target-2)
        missing=dict(row,period_no=273,period_key='2026-09-25:0273',
                     target_block=target-40,block_number=None)
        too_late=dict(row,period_no=272,period_key='2026-09-25:0272',
                      target_block=target-60,block_number=target-60,
                      locked_at=publication+timedelta(seconds=1))
        result=scope['ai_audit_summary']([hit,second,wrong_target,missing,too_late],
                                        {'sample':650,'testSample':98,'active':False})
        self.assertEqual(result['predictionRows'],5)
        self.assertEqual(result['validSamples'],2)
        self.assertEqual(result['windows']['1000']['top1Hits'],1)
        self.assertEqual(result['windows']['1000']['top3Hits'],2)
        self.assertEqual(result['invalidReasons']['targetMismatch'],1)
        self.assertEqual(result['invalidReasons']['blockMissing'],1)
        self.assertEqual(result['invalidReasons']['notLockedBeforeBlock'],1)
        self.assertEqual(result['hashModel']['sample'],650)
        self.assertEqual(result['actualCounts']['3'],2)
        self.assertEqual(result['predictedCounts']['3'],1)
        self.assertEqual(result['predictedCounts']['4'],1)
        self.assertEqual(result['modelVersions'][row['model_version']],{'verified':2,'top1Hits':1})
        self.assertEqual(result['latestSavedVersion'],row['model_version'])


if __name__=='__main__':unittest.main()
