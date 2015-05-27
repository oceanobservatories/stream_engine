import copy
import json
import os
import struct
import unittest
import subprocess

from cassandra.cluster import Cluster
import numpy
import time

from engine.routes import app
import preload_database.database
from preload_database.model.preload import Parameter, Stream
from util.cass import fetch_data, global_cassandra_state, get_distinct_sensors, get_streams, stream_exists, \
    fetch_nth_data, create_execution_pool
from util.common import StreamKey, TimeRange, CachedStream, CachedParameter, stretch, interpolate
from util.calc import StreamRequest, Chunk_Generator, Particle_Generator, find_stream, handle_byte_buffer, execute_dpa, build_func_map, in_range, build_CC_argument, \
    Interpolation_Generator
import parameter_util
import preload_database.database


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
        if not os.path.exists('TEST_DATA_LOADED'):
            subprocess.call(['cqlsh', '-f', 'load.cql'])
            open('TEST_DATA_LOADED', 'wb').write('%s\n' % time.ctime())

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

    def test_parameters(self):
        """
        Test whether we can retrieve a parameter by id and verify that
        it contains the correct data.
        :return:
        """
        pmap = {
            195: {
                'name': 'pressure',
                'ptype': 'quantity',
                'encoding': 'int32',
                'needs': [],
                'cc': [],
            },
            1963: {
                'name': 'ctdpf_ckl_seawater_density',
                'ptype': 'function',
                'encoding': 'float32',
                'needs': [193, 194, 195, 1959, 1960, 1961, 1962],
                'cc': ['CC_latitude', 'CC_longitude'],
            },
        }

        # by id
        for pdid in pmap:
            parameter = Parameter.query.get(pdid)
            self.assertIsNotNone(parameter)
            self.assertEqual(parameter.name, pmap[pdid]['name'])
            self.assertEqual(parameter.id, pdid)
            self.assertEqual(parameter.parameter_type.value, pmap[pdid]['ptype'])
            self.assertEqual(parameter.value_encoding.value, pmap[pdid]['encoding'])
            self.assertEqual(sorted([pdref.pdid for pdref in parameter_util.needs(parameter)]), pmap[pdid]['needs'])
            self.assertEqual(sorted(parameter_util.needs_cc(parameter)), pmap[pdid]['cc'])

            # by name (FAILS, parameter names are not unique!)
            # for pdid in pmap:
            # parameter = Parameter.query.filter(Parameter.name == pmap[pdid]['name']).first()
            # self.assertIsNotNone(parameter)
            # self.assertEqual(parameter.name, pmap[pdid]['name'])
            # self.assertEqual(parameter.id, pdid)
            # self.assertEqual(parameter.parameter_type.value, pmap[pdid]['ptype'])
            # self.assertEqual(parameter.value_encoding.value, pmap[pdid]['encoding'])
            # self.assertEqual(sorted([p.id for p in parameter.needs()]), pmap[pdid]['needs'])
            # self.assertEqual(sorted(parameter.needs_cc()), pmap[pdid]['cc'])

    def test_streams(self):
        """
        Test if we can retrieve a stream by name and verify that it contains
        the correct parameters.
        :return:
        """
        stream = Stream.query.filter(Stream.name == 'thsph_sample').first()
        self.assertEqual(stream.name, 'thsph_sample')
        self.assertEqual([p.id for p in stream.parameters],
                         [7, 10, 11, 12, 16, 863, 2260, 2261, 2262, 2263,
                          2264, 2265, 2266, 2267, 2624, 2625, 2626,
                          2627, 2628, 2629, 2630, 2631, 2632, 2633, 2634, 2635])

    def test_distinct_sensors(self):
        distinct = get_distinct_sensors()
        self.assertListEqual(distinct,
                             [(u'RS00ENGC', u'XX001', u'00-CTDPFA001'),
                              (u'XX00XXXX', u'XX00X', u'00-CTDPFW100'),
                              (u'RS00ENGC', u'XX00X', u'00-CTDBPA002')])

    def test_get_streams(self):
        streams = get_streams(self.subsite, self.node, self.sensor, self.method)
        streams = [s for s in streams]
        self.assertListEqual(streams,
                             [u'ctdbp_no_calibration_coefficients', u'ctdbp_no_sample'])

    def test_fetch_data(self):
        stream_key = StreamKey(self.subsite, self.node, self.sensor, self.method, 'ctdbp_no_sample')
        time_range = TimeRange(self.first, self.last)
        cols, future = fetch_data(stream_key, time_range)
        data = future.result()
        self.assertTrue(len(data) == 100)

    def test_fetch_nth_data(self):
        stream_key = StreamKey(self.subsite, self.node, self.sensor, self.method, 'ctdbp_no_sample')
        time_range = TimeRange(self.first, self.last)
        cols, future = fetch_nth_data(stream_key, time_range, num_points=20, chunk_size=5)
        data = future.result()
        self.assertTrue(len(data) == 20)

    def test_stream_exists(self):
        self.assertTrue(stream_exists(self.subsite, self.node, self.sensor, self.method, self.stream))
        self.assertFalse(stream_exists(self.subsite, self.node, self.sensor, self.method, 'cheese'))

    def test_find_stream(self):
        target_parameter = CachedParameter.from_id(163)  # coefficient C1 needed for PRESWAT
        distinct = get_distinct_sensors()
        stream_key = StreamKey(self.subsite, self.node, self.sensor, self.method, self.stream)
        streams = [CachedStream.from_id(i) for i in target_parameter.streams]
        sensor, stream = find_stream(stream_key, streams, distinct)
        self.assertEqual(sensor, self.sensor)
        self.assertEqual(stream.name, u'ctdbp_no_calibration_coefficients')

    def test_stretch(self):
        t1 = numpy.linspace(2000.0, 5000.0).tolist()
        t2 = numpy.linspace(1000.0, 6000.0).tolist()
        data = range(len(t1))
        t3, data2 = stretch(t1, data, t2)
        self.assertEqual(t3[0], t2[0])
        self.assertEqual(t3[-1], t2[-1])
        self.assertEqual(data2[0], data2[1])
        self.assertEqual(data2[-2], data2[-1])

    def test_interpolate(self):
        t1 = numpy.linspace(2000.0, 5000.0).tolist()
        t2 = numpy.linspace(1000.0, 6000.0).tolist()
        data = range(len(t1))
        t3, data2 = stretch(t1, data, t2)
        t4, data3 = interpolate(t3, data2, t2)
        self.assertListEqual(t2, t4)
        self.assertEqual(len(data3), len(t2))

        orig_times = [1, 2, 4, 5]
        new_times = [1, 2, 3, 4, 5]
        data = [1, 2, 4, 5]
        int_times, new_data = interpolate(orig_times, data, new_times)
        self.assertEqual(int_times, new_times)
        self.assertTrue(numpy.allclose(new_data, [1., 2., 3., 4., 5.]))

        data = ['a', 'b', 'd', 'e']
        int_times, new_data = interpolate(orig_times, data, new_times)
        self.assertEqual(int_times, new_times)
        self.assertListEqual(new_data.tolist(), ['a', 'b', 'b', 'd', 'e'])

        data = ['a']
        times = [1.0]
        times, data = stretch(times, data, new_times)
        int_times, new_data = interpolate(times, data, new_times)
        self.assertListEqual(new_data.tolist(), ['a', 'a', 'a', 'a', 'a'])

        data = [[1, 2, 3, 4, 5], [2, 3, 4, 5, 6]]
        times = [1.0, 3.0]
        new_times = [1.0, 2.0, 3.0]
        times, data = stretch(times, data, new_times)
        int_times, new_data = interpolate(times, data, new_times)
        self.assertTrue(numpy.allclose(new_data, [[1, 2, 3, 4, 5],
                                                  [1.5, 2.5, 3.5, 4.5, 5.5],
                                                  [2, 3, 4, 5, 6]]))

    def test_handle_byte_buffer(self):
        data = range(8)
        packed = struct.pack('>8i', *data)
        unpacked = handle_byte_buffer(packed, 'int32', [8])
        self.assertListEqual(data, unpacked.tolist())

        packed = struct.pack('>8d', *data)
        unpacked = handle_byte_buffer(packed, 'float32', [8])
        self.assertListEqual(data, unpacked.tolist())

        packed = struct.pack('>8q', *data)
        unpacked = handle_byte_buffer(packed, 'int64', [8])
        self.assertListEqual(data, unpacked.tolist())

    def test_execute_dpa(self):
        parameter = CachedParameter.from_id(3650)
        kwargs = {
            'SP': numpy.array([33.5, 33.5, 37, 34.9, 35, 35]),
            't': numpy.array([28., 28., 20., 6., 3., 2.]),
            'p': numpy.array([0., 10., 150., 800., 2500., 5000.]),
            'lat': numpy.tile(15.00, 6),
            'lon': numpy.tile(-55.00, 6)
        }
        result = execute_dpa(parameter, kwargs)

        check_values = numpy.array([1021.26851,
                                    1021.31148,
                                    1026.94422,
                                    1031.13498,
                                    1039.28768,
                                    1050.30616])
        numpy.testing.assert_allclose(result, check_values, rtol=1e-6, atol=0)

    def test_build_func_map(self):
        parameter = CachedParameter.from_id(3650)
        coefficients = {
            'CC_lat': [{'start': 0, 'stop': 6, 'value': 15.0}],
            'CC_lon': [{'start': 0, 'stop': 6, 'value': -55.0}]
        }
        chunk = {
            7: {'data': numpy.arange(6)},
            3649: {'data': numpy.array([33.5, 33.5, 37, 34.9, 35, 35])},
            908: {'data': numpy.array([28., 28., 20., 6., 3., 2.])},
            3647: {'data': numpy.array([0., 10., 150., 800., 2500., 5000.])},
        }

        expected_args = {
            'SP': numpy.array([33.5, 33.5, 37, 34.9, 35, 35]),
            't': numpy.array([28., 28., 20., 6., 3., 2.]),
            'p': numpy.array([0., 10., 150., 800., 2500., 5000.]),
            'lat': numpy.tile(15.00, 6),
            'lon': numpy.tile(-55.00, 6)
        }

        result = build_func_map(parameter, chunk, coefficients)
        self.assertListEqual(expected_args.keys(), result.keys())
        for key in result:
            numpy.testing.assert_array_equal(result[key], expected_args[key])

    def test_in_range(self):
        times = numpy.arange(1,6)
        numpy.testing.assert_array_equal(in_range((2,4), times), [False,True,True,False,False])
        numpy.testing.assert_array_equal(in_range((None,4), times), [True,True,True,False,False])
        numpy.testing.assert_array_equal(in_range((2,None), times), [False,True,True,True,True])
        numpy.testing.assert_array_equal(in_range((None,None), times), [True,True,True,True,True])
        numpy.testing.assert_array_equal(in_range((4,4,), times), [False,False,False,True,False])

    def test_build_CC_argument(self):
        times = numpy.arange(5)
        frames = [ {'start': 0, 'stop': 1, 'value': 1 },
                   {'start': 0, 'stop': 1, 'value': 1 },
                   {'start': 0, 'stop': 1, 'value': 1 } ]
        self.assertTrue(numpy.isnan(numpy.min(build_CC_argument(frames, times))))

        frames = [ {'start': 0, 'stop': 1, 'value': [1,2] },
                   {'start': 4, 'stop': 5, 'value': [3,4] },
                   {'start': 1, 'stop': 4, 'value': [2,3] } ]
        numpyresult = numpy.array([[1,2],[2,3],[2,3],[2,3],[3,4]])
        numpy.testing.assert_array_equal(numpyresult, build_CC_argument(frames, times))

    def test_data_stream(self):

        stream_key1 = StreamKey(self.subsite, self.node, self.sensor, self.method, 'ctdbp_no_sample')
        stream_key2 = StreamKey(self.subsite, self.node, self.sensor, self.method, 'ctdbp_no_calibration_coefficients')
        time_range = TimeRange(self.first, self.last)
        sr = StreamRequest([stream_key1, stream_key2], [],
                           {'CC_lat': [{'start': self.first, 'stop': self.last, 'value': 0.0}],
                            'CC_lon': [{'start': self.first, 'stop': self.last, 'value': 0.0}]}, time_range)
        particles = json.loads(''.join(list(Particle_Generator(Chunk_Generator()).chunks(sr))))

        self.assertEqual(len(particles), 100)

    def test_data_stream_with_limit(self):
        # number of points needs to be low enough that
        # we will exercise the fetch_nth code
        number_points = 20

        stream_key1 = StreamKey(self.subsite, self.node, self.sensor, self.method, 'ctdbp_no_sample')
        stream_key2 = StreamKey(self.subsite, self.node, self.sensor, self.method, 'ctdbp_no_calibration_coefficients')
        time_range = TimeRange(self.first, self.last)
        times = numpy.linspace(self.first, self.last, num=number_points)
        sr = StreamRequest([stream_key1, stream_key2], [],
                           {'CC_lat': [{'start': self.first, 'stop': self.last, 'value': 0.0}],
                            'CC_lon': [{'start': self.first, 'stop': self.last, 'value': 0.0}]},
                           time_range, limit=number_points, times=times)

        # first, verify we get n data points from chunk generator
        particles = json.loads(''.join(list(Particle_Generator(Chunk_Generator()).chunks(sr))))
        self.assertEqual(len(particles), number_points)

        # second, verify the times are all <= our input times
        retrieved_times = [p['time'] for p in particles]
        diffs = times - retrieved_times
        assert numpy.all(diffs >= 0)

    def test_invalid_request(self):
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        good_request = {
            'streams': [
                {
                    "node": "XX00X",
                    "stream": "ctdpf_ckl_wfp_instrument_recovered",
                    "subsite": "XX00XXXX",
                    "sensor": "00-CTDPFW100",
                    "method": "recovered"
                }
            ],
            'coefficients': {
                'CC_latitude': 1.0,
                'CC_longitude': 1.0,
            }
        }

        missing_coefficients = copy.deepcopy(good_request)
        del(missing_coefficients['coefficients'])

        missing_node = copy.deepcopy(good_request)
        del(missing_node['streams'][0]['node'])

        bad_refdes = copy.deepcopy(good_request)
        bad_refdes['streams'][0]['method'] = 'divined'

        bad_coefficients = copy.deepcopy(good_request)
        bad_coefficients['coefficients'] = ['bad']

        bad_parameters = copy.deepcopy(good_request)
        bad_parameters['streams'][0]['parameters'] = [-7]

        bad_parameters2 = copy.deepcopy(good_request)
        bad_parameters2['streams'][0]['parameters'] = [201]

        r = self.app.post('/particles', headers=headers)
        self.assertEqual(r.status_code, 400)

        # we just let this go through now...
        # r = self.app.post('/particles', data=json.dumps(missing_coefficients), headers=headers)
        # self.assertEqual(r.status_code, 400)

        r = self.app.post('/particles', data=json.dumps(missing_node), headers=headers)
        self.assertEqual(r.status_code, 400)

        r = self.app.post('/particles', data=json.dumps(bad_refdes), headers=headers)
        self.assertEqual(r.status_code, 404)

        r = self.app.post('/particles', data=json.dumps(bad_coefficients), headers=headers)
        self.assertEqual(r.status_code, 400)

        r = self.app.post('/particles', data=json.dumps(bad_parameters), headers=headers)
        self.assertEqual(r.status_code, 400)

        r = self.app.post('/particles', data=json.dumps(bad_parameters2), headers=headers)
        self.assertEqual(r.status_code, 400)

    def test_particles_request(self):
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
                'CC_latitude': [{'value': 1.0}],
                'CC_longitude': [{'value': 1.0}],
            }
        }

        r = self.app.post('/particles', data=json.dumps(request), headers=headers)
        data = json.loads(r.data)
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 3)
        self.assertTrue('ctdpf_ckl_seawater_pressure' in data[0])

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

    def test_incomplete_stream(self):
        # we have the ctdpf_sbe43_sample stream but not the corresponding coefficients stream
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        request = {
            'streams': [
                {
                    "node": "XX001",
                    "stream": "ctdpf_sbe43_sample",
                    "subsite": "RS00ENGC",
                    "sensor": "00-CTDPFA001",
                    "method": "streamed",
                }
            ],
        }

        r = self.app.post('/needs', data=json.dumps(request), headers=headers)
        response = json.loads(r.data)
        # print r, response

        r = self.app.post('/particles', data=json.dumps(request), headers=headers)
        response = json.loads(r.data)
        # print r, response
