import csv
import os
import unittest

import preload_database.database
from engine.routes import app
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


class StreamUnitTest(unittest.TestCase, StreamUnitTestMixin):
    TEST_KEYSPACE = 'stream_engine_test'

    @classmethod
    def setUpClass(cls):
        cls.metadata = list(csv.reader(open(os.path.join(TEST_DIR, 'metadata.csv'))))

    # @classmethod
    # def setUpClass(cls):
    #     os.chdir(TEST_DIR)
    #     if not os.path.exists(TEST_DIR + '/TEST_DATA_LOADED'):
    #         subprocess.call(['cqlsh', '-f', TEST_DIR + '/load.cql'])
    #         open(TEST_DIR + '/TEST_DATA_LOADED', 'wb').write('%s\n' % time.ctime())
    #
    #     app.config['CASSANDRA_KEYSPACE'] = StreamUnitTest.TEST_KEYSPACE
    #
    #     cluster = Cluster(app.config['CASSANDRA_CONTACT_POINTS'],
    #                       control_connection_timeout=app.config['CASSANDRA_CONNECT_TIMEOUT'])
    #     global_cassandra_state['cluster'] = cluster
    #     create_execution_pool()

    def setUp(self):
        app.config['TESTING'] = True
        self.app = app.test_client()

    def tearDown(self):
        preload_database.database.Session.remove()

