import global_test_setup

import os
import unittest

import numpy as np
from datetime import datetime

import util.common as common
from preload_database.database import initialize_connection, open_connection, PreloadDatabaseMode


TEST_DIR = os.path.dirname(__file__)
initialize_connection(PreloadDatabaseMode.POPULATED_MEMORY)
open_connection()


class CommonTest(unittest.TestCase):
    def setUp(self):
        self.epoch_dt = datetime(1900, 1, 1, 0, 0, 0)

    def test_isfillvalue(self):
        assert np.array_equal(common.isfillvalue([1, 2, 3, -999999999, 4]),
                              [False, False, False, True, False])
        assert np.array_equal(common.isfillvalue([1.2, np.NaN, np.NaN, 2.3, np.NaN]),
                              [False, True, True, False, True])
        assert np.array_equal(common.isfillvalue(['', 'abc']), [True, False])

    def test_ntp_to_datetime_valid(self):
        start_dt = datetime(2015, 1, 1, 12, 0, 0)
        ntp_time = (start_dt - self.epoch_dt).total_seconds()
        rval = common.ntp_to_datetime(ntp_time)
        self.assertEqual(start_dt, rval)

    def test_ntp_to_datetime_invalid(self):
        rval = common.ntp_to_datetime('invalid')
        self.assertIs(rval, None)

    def test_ntp_to_datestring(self):
        start_dt = datetime(2015, 1, 1, 12, 0, 0)
        ntp_time = (start_dt - self.epoch_dt).total_seconds()
        rval = common.ntp_to_datestring(ntp_time)
        self.assertEqual(start_dt.isoformat(), rval)

    def test_ntp_to_datestring_invalid(self):
        rval = common.ntp_to_datestring('invalid')
        self.assertEqual(rval, 'invalid')

    def test_time_range(self):
        start = 1
        stop = 10
        trange = stop - start
        tr = common.TimeRange(start, stop)

        self.assertEqual(tr.start, start)
        self.assertEqual(tr.stop, stop)
        self.assertEqual(tr.secs(), trange)

    def test_time_range_copy(self):
        start = 1
        stop = 10
        trange = stop - start
        tr = common.TimeRange(start, stop)
        tr2 = tr.copy()

        self.assertEqual(tr2.start, start)
        self.assertEqual(tr2.stop, stop)
        self.assertEqual(tr2.secs(), trange)
        self.assertIsNot(tr, tr2)
        self.assertEqual(tr, tr2)

    def test_time_range_collapse(self):
        tr = common.TimeRange(1, 10)
        tr2 = common.TimeRange(3, 11)
        tr3 = tr.collapse(tr2)

        self.assertEqual(tr3.start, 3)
        self.assertEqual(tr3.stop, 10)

    def test_stream_key_equality(self):
        subsite, node, sensor, method = 1, 2, 3, 4
        stream = 'nutnr_a_sample'
        sk1 = common.StreamKey(subsite, node, sensor, method, stream)
        sk2 = common.StreamKey(subsite, node, sensor, method, stream)
        self.assertEqual(sk1, sk2)

    def test_stream_key_hash(self):
        subsite, node, sensor, method = 1, 2, 3, 4
        stream = 'nutnr_a_sample'
        sk1 = common.StreamKey(subsite, node, sensor, method, stream)
        sk2 = common.StreamKey(subsite, node, sensor, method, stream)
        sk_set = {sk1, sk2}
        self.assertEqual(len(sk_set), 1)

    def test_stream_key_is_virtual(self):
        subsite, node, sensor, method = 1, 2, 3, 4
        sk1 = common.StreamKey(subsite, node, sensor, method, 'metbk_hourly')
        sk2 = common.StreamKey(subsite, node, sensor, method, 'botpt_nano_sample')
        self.assertTrue(sk1.is_virtual)
        self.assertFalse(sk2.is_virtual)

    def test_stream_key_is_glider(self):
        subsite1, node1, sensor1, method1 = 'CP05MOAS', 'GL388', '04-DOSTAM000', 'recovered_host'
        subsite2, node2, sensor2, method2 = 'CP05MOAS', 'XL388', '04-DOSTAM000', 'recovered_host'
        stream = 'dosta_abcdjm_glider_recovered'
        sk1 = common.StreamKey(subsite1, node1, sensor1, method1, stream)
        sk2 = common.StreamKey(subsite2, node2, sensor2, method2, stream)
        self.assertTrue(sk1.is_glider)
        self.assertFalse(sk2.is_glider)

    def test_stream_key_is_mobile(self):
        subsite1, node1, sensor1, method1 = 'XXX', 'WFP01', '04-DOSTAM000', 'recovered_host'
        subsite2, node2, sensor2, method2 = 'XXX', 'LJ01A', '04-DOSTAM000', 'recovered_host'
        stream = 'dosta_abcdjm_glider_recovered'
        sk1 = common.StreamKey(subsite1, node1, sensor1, method1, stream)
        sk2 = common.StreamKey(subsite2, node2, sensor2, method2, stream)
        self.assertTrue(sk1.is_mobile)
        self.assertFalse(sk2.is_mobile)
