import importlib
import sys
import types
import unittest

sys.modules.setdefault('collector_core',types.ModuleType('collector_core'))
sys.modules['collector_core'].RESEARCH_VERSION='research-family-v3'
import research_engine as engine


class PracticeTests(unittest.TestCase):
    @staticmethod
    def groups(signal):
        return {str(i):{'block':'A'*63+(signal if i==17 else '0'),
                        'singleCount':2 if i==17 else 3} for i in range(1,18)}

    def test_chronological_all_history_rolling_window_and_no_current_label_leak(self):
        p=engine.Practice()
        for i in range(1,2602):
            signal='1' if i%2 else '9'
            p.consume(f'2026-09-25:{i:04d}',self.groups(signal),2 if i%2 else 5)
        r=p.report()
        self.assertEqual((r['historicalPeriodsRead'],r['trainingPeriods'],r['replayPeriods']),
                         (2601,1000,1601))
        self.assertEqual(r['replayFrom'],'2026-09-25:1001')
        self.assertEqual(len(p.window),1000)
        self.assertEqual(p.window[0][0],'2026-09-25:1602')
        self.assertEqual(r['recentPractice'][0]['trainedThrough'],'2026-09-25:2600')
        self.assertEqual(len(r['recentPractice']),100)
        self.assertEqual(len(r['ranking']),6)
        self.assertEqual(len(r['topTwo']),2)
        self.assertEqual(r['ranking'][0]['hits'],r['replayPeriods'])
        snap=p.snapshot()
        self.assertEqual(snap['trainedThrough'],'2026-09-25:2601')
        self.assertEqual(snap['replayTopTwo'],r['topTwo'])

    def test_saves_forecast_before_future_result_is_seen(self):
        a,b=engine.Practice(),engine.Practice()
        for i in range(1,1001):
            signal='1' if i%2 else '9'
            for p in (a,b):p.consume(f'2026-09-25:{i:04d}',self.groups(signal),2 if i%2 else 5)
        a.consume('2026-09-25:1001',self.groups('1'),2)
        b.consume('2026-09-25:1001',self.groups('1'),7)
        first=a.report()['recentPractice'][0]
        second=b.report()['recentPractice'][0]
        self.assertEqual(first['predictions'],second['predictions'])
        self.assertEqual(first['trainedThrough'],'2026-09-25:1000')


if __name__=='__main__':unittest.main()
