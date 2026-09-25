import sys
import types
import unittest
from unittest.mock import patch

sys.modules.setdefault('collector_core',types.ModuleType('collector_core'))
import backfill


class BackfillTests(unittest.TestCase):
    def test_period_index_roundtrip(self):
        from datetime import date
        index=date(2026,9,26).toordinal()*1440+272
        self.assertEqual(backfill.period_from_index(index),('2026-09-26',273))

    def test_authentic_block_must_have_height_hash_and_timestamp(self):
        raw={'blockID':'A'*64,'block_header':{'raw_data':{
            'number':86510000,'timestamp':1790330000000}}}
        with patch.object(backfill.core,'calc_numbers',return_value=[1,2,3,4,5,6,7],create=True),\
             patch.object(backfill.core,'calc_single_count',return_value=4,create=True):
            self.assertEqual(backfill._normalize(raw)['number'],86510000)
            self.assertEqual(backfill._normalize(raw)['singleCount'],4)
            with self.assertRaises(ValueError):
                backfill._normalize({**raw,'blockID':'not a hash'})

    def test_missing_raw_height_is_not_filled_with_guessed_hash(self):
        target=1000
        warehouse={height:{'block_number':height,'block_hash':'A'*64,
                   'numbers':['01']*7,'single_count':7} for height in range(981,1000)}
        with patch.object(backfill.core,'period_target_block',return_value=target,create=True),\
             patch.object(backfill,'_existing',return_value={}),\
             patch.object(backfill,'_warehouse_period',return_value=warehouse),\
             patch.object(backfill,'_verified_row',return_value=True),\
             patch.object(backfill.core,'persist_period_groups',create=True) as write:
            with self.assertRaisesRegex(RuntimeError,'raw warehouse missing block 1000'):
                backfill.fetch_period(date_index())
            write.assert_not_called()



def date_index():
    from datetime import date
    return date(2026,9,26).toordinal()*1440


if __name__=='__main__':unittest.main()
