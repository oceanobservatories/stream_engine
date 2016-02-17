import csv
import os
import unittest

import mock

import preload_database.database
from engine import app
from util.calc import find_stream
from util.common import StreamKey
from preload_database.model.preload import Stream

preload_database.database.initialize_connection(preload_database.database.PreloadDatabaseMode.POPULATED_FILE)
preload_database.database.open_connection()

TEST_DIR = os.path.dirname(__file__)


class StreamUnitTestMixin(object):
    subsite = 'CE05MOAS'
    node = 'GL311'
    sensor = '05-CTDGVM000'
    method = 'recovered_host'
    stream = 'ctdgv_m_glider_instrument_recovered'
    first = 3634041542.2546076775
    last = 3634041695.3398842812
    stream_key = StreamKey(subsite, node, sensor, method, stream)
    stream_db = Stream.query.filter(Stream.name == stream).first()


def get_test_metadata():
    mix = StreamUnitTestMixin
    return [(mix.subsite, mix.node, mix.sensor, mix.method, mix.stream)]


class StreamUnitTest(unittest.TestCase, StreamUnitTestMixin):
    TEST_KEYSPACE = 'stream_engine_test'

    @classmethod
    def setUpClass(cls):
        cls.metadata = list(csv.reader(open(os.path.join(TEST_DIR, 'metadata.csv'))))

    def setUp(self):
        app.config['TESTIN' \
                   'G'] = True
        self.app = app.test_client()

    def tearDown(self):
        preload_database.database.Session.remove()

    def test_find_stream_same_stream_same_desig(self):
        with mock.patch('util.cass._get_stream_metadata', return_value=self.metadata):
            stream = find_stream(self.stream_key, self.stream_db)
            self.assertEqual(stream, self.stream_key)

    def test_find_stream_same_node(self):
        with mock.patch('util.cass._get_stream_metadata', return_value=self.metadata):
            stream = Stream.query.filter(Stream.name == 'glider_eng_recovered').first()
            expected_sk = StreamKey(self.subsite, self.node, '00-ENG000000', 'recovered_host', stream.name)
            stream = find_stream(self.stream_key, stream)
            self.assertEqual(stream, expected_sk)

    def test_find_stream_same_subsite(self):
        with mock.patch('util.cass._get_stream_metadata', return_value=self.metadata):
            source_sk = StreamKey('CE02SHSM', 'RID26', '04-VELPTA000',
                                  'recovered_host', 'velpt_ab_dcl_instrument_recovered')
            expected_sk = StreamKey('CE02SHSM', 'RID27', '02-FLORTD000',
                                    'recovered_host', 'flort_dj_dcl_instrument_recovered')
            stream_db = Stream.query.filter(Stream.name == 'flort_dj_dcl_instrument_recovered').first()

            received_sk = find_stream(source_sk, stream_db)
            self.assertEqual(received_sk, expected_sk)
