"""Ground the target mapping in the platform screenshot's confirmed rows."""
import ast
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
import unittest

source=ast.parse(Path(__file__).with_name('app.py').read_text())
names={'period_index','period_target_block','tail_schedule','_tail_boundary',
       'calibrated_group20_row','historical_prediction_verified'}
constants={'TAIL_ANCHOR_INDEX','TAIL_INTERVAL_PERIODS','TAIL_SEQUENCE'}
scope={'datetime':datetime, 'date':date}
exec(compile(ast.Module(body=[n for n in source.body
                              if (isinstance(n,ast.FunctionDef) and n.name in names)
                              or (isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id in constants for t in n.targets))],type_ignores=[]),
             '<calibration>','exec'),scope)


class CalibrationTests(unittest.TestCase):
    def test_screenshot_2026_09_25_0266_to_0275(self):
        target=scope['period_target_block']
        for period in range(266,276):
            self.assertEqual(target('2026-09-25',period),86536400+20*(period-266))

    def test_prior_confirmed_anchors_stay_intact(self):
        target=scope['period_target_block']
        self.assertEqual(target('2026-09-24',481),86511906)
        self.assertEqual(target('2026-09-24',1001),86522304)
        self.assertEqual(target('2026-09-24',1201)-target('2026-09-24',1200),18)

    def test_next_period_keeps_zero_tail(self):
        target=scope['period_target_block']
        self.assertEqual(target('2026-09-25',276),86536600)
        self.assertEqual(target('2026-09-26',1)-target('2026-09-25',1440),20)

    def test_average_schedule_all_five_switches(self):
        target=scope['period_target_block']
        schedule=scope['tail_schedule']
        switches=[('2026-09-24',481,6),('2026-09-24',841,4),
                  ('2026-09-24',1201,2),('2026-09-25',121,0),
                  ('2026-09-25',481,8),('2026-09-25',841,6)]
        for ds,p,tail in switches:
            with self.subTest(date=ds,period=p):
                before_p=p-1
                self.assertEqual(target(ds,p)-target(ds,before_p),18)
                self.assertEqual(target(ds,p)%10,tail)
                self.assertEqual(schedule(ds,p)['tail'],tail)
                self.assertEqual(schedule(ds,p)['phaseStart']['period'],f'{p:04d}')
        self.assertEqual(schedule('2026-09-25',266)['tail'],0)
        self.assertTrue(schedule('2026-09-25',266)['estimated'])

    def test_old_period_group_is_not_an_official_result(self):
        valid=scope['calibrated_group20_row']
        expected=scope['period_target_block']('2026-09-25',121)
        row={'period_date':date(2026,9,25),'period_no':121,'target_block':expected-2}
        self.assertFalse(valid(row))
        row['target_block']=expected
        self.assertTrue(valid(row))

    def test_historical_predictions_need_matching_block_and_earlier_lock(self):
        check=scope['historical_prediction_verified']
        block_time=datetime(2026,9,25,0,1,tzinfo=timezone.utc)
        block={'block_time':block_time,'single_count':5}
        pred={'target_block':86536400,'actual_single':5,
              'locked_at':block_time-timedelta(seconds=10)}
        self.assertTrue(check(pred,block,86536400))
        self.assertFalse(check(pred,block,86536402))
        pred['locked_at']=block_time+timedelta(seconds=1)
        self.assertFalse(check(pred,block,86536400))
        pred['locked_at']=block_time-timedelta(seconds=10)
        pred['actual_single']=4
        self.assertFalse(check(pred,block,86536400))


if __name__=='__main__':unittest.main()
