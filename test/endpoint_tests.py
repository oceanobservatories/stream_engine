import json
import os
import unittest
import subprocess

from cassandra.cluster import Cluster
import time

from engine.routes import app
import preload_database.database
from util.cass import create_execution_pool, global_cassandra_state
from util.common import StreamKey
import numpy as np


preload_database.database.initialize_connection(preload_database.database.PreloadDatabaseMode.POPULATED_FILE)
preload_database.database.open_connection()

TEST_DIR = os.path.dirname(__file__)


class StreamUnitTestMixin(object):
    subsite = 'RS00ENGC'
    node = 'XX00X'
    sensor = '00-CTDBPA002'
    method = 'streamed'
    stream = 'ctdbp_no_sample'
    first = 3634041542.2546076775
    last = 3634041695.3398842812
    stream_key = StreamKey(subsite, node, sensor, method, stream)


class StreamUnitTest(unittest.TestCase, StreamUnitTestMixin):
    TEST_KEYSPACE = 'stream_engine_test'

    @classmethod
    def setUpClass(cls):
        os.chdir(TEST_DIR)
        if not os.path.exists(TEST_DIR + '/TEST_DATA_LOADED'):
            subprocess.call(['cqlsh', '-f', TEST_DIR + '/load.cql'])
            open(TEST_DIR + '/TEST_DATA_LOADED', 'wb').write('%s\n' % time.ctime())

        app.config['CASSANDRA_KEYSPACE'] = StreamUnitTest.TEST_KEYSPACE

        cluster = Cluster(app.config['CASSANDRA_CONTACT_POINTS'],
                          control_connection_timeout=app.config['CASSANDRA_CONNECT_TIMEOUT'])
        global_cassandra_state['cluster'] = cluster
        create_execution_pool()

    def setUp(self):
        app.config['TESTING'] = True
        self.app = app.test_client()

    def tearDown(self):
        preload_database.database.Session.remove()

    def test_needs(self):
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        request = {
            'streams': [
                {
                    "node": "XX00X",
                    "stream": "ctdpf_ckl_wfp_instrument_recovered",
                    "subsite": "XX00XXXX",
                    "sensor": "00-CTDPFW100",
                    "method": "recovered",
                    "parameters": [1959]
                }
            ],
        }

        expected_response = {
            'streams': [
                {
                    "node": "XX00X",
                    "stream": "ctdpf_ckl_wfp_instrument_recovered",
                    "subsite": "XX00XXXX",
                    "sensor": "00-CTDPFW100",
                    "method": "recovered",
                    'coefficients': ['CC_longitude', 'CC_latitude']
                }
            ],
        }

        r = self.app.post('/needs', data=json.dumps(request), headers=headers)
        response = json.loads(r.data)
        self.assertDictEqual(response, expected_response)

    def test_particles(self):
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        request = {
            'streams': [
                {
                    "node": "XX00X",
                    "stream": "ctdpf_ckl_wfp_instrument_recovered",
                    "subsite": "XX00XXXX",
                    "sensor": "00-CTDPFW100",
                    "method": "recovered",
                    "parameters": [1959]
                }
            ],
            'coefficients': {
                'CC_latitude': [{'value': 1.0, 'deployment': 1}],
                'CC_longitude': [{'value': 1.0, 'deployment': 1}],
            },
            'include_provenance': False,
            'include_annotations': False,
            'qcParameters': {}
        }

        r = self.app.post('/particles', data=json.dumps(request), headers=headers)
        data = json.loads(r.data)
        with open(TEST_DIR + '/test_particles.json', mode='r') as f:
            testdata = json.loads(f.read())
            assert _almost_equal(data, testdata)

def _almost_equal(a, b):
    if isinstance(a, (list,tuple)) and isinstance(b, (list,tuple)):
        if len(a) != len(b):
            return False
        for i in xrange(0, len(a)):
            if not _almost_equal(a[i], b[i]):
                return False
    elif isinstance(a, (dict)) and isinstance(b, (dict)):
        if a.keys() != b.keys():
            return False
        for i in a:
            if not _almost_equal(a[i], b[i]):
                return False
    elif isinstance(a, float) or isinstance(b, float):
        ac = np.allclose(a,b)
        return ac
    else:
        return a == b
    return True
