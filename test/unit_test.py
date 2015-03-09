import base64
import os
import unittest
import msgpack
import numpy
from engine import app
from model.preload import Parameter, Stream
from util.cassandra_query import DataParameter, msgpack_one
from util.preload_insert import create_db


class StreamUnitTest(unittest.TestCase):
    def setUp(self):
        if not os.path.exists(app.config['DBFILE_LOCATION']):
            create_db()

    def tearDown(self):
        pass

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
                'needs': [195],
                'cc': [],
            },
            1963: {
                'name': 'ctdpf_ckl_seawater_density',
                'ptype': 'function',
                'encoding': 'float32',
                'needs': [193, 194, 195, 1959, 1960, 1961, 1962, 1963],
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
            self.assertEqual(sorted([p.id for p in parameter.needs()]), pmap[pdid]['needs'])
            self.assertEqual(sorted(parameter.needs_cc()), pmap[pdid]['cc'])

        # by name (FAILS, parameter names are not unique!)
        # for pdid in pmap:
        #     parameter = Parameter.query.filter(Parameter.name == pmap[pdid]['name']).first()
        #     self.assertIsNotNone(parameter)
        #     self.assertEqual(parameter.name, pmap[pdid]['name'])
        #     self.assertEqual(parameter.id, pdid)
        #     self.assertEqual(parameter.parameter_type.value, pmap[pdid]['ptype'])
        #     self.assertEqual(parameter.value_encoding.value, pmap[pdid]['encoding'])
        #     self.assertEqual(sorted([p.id for p in parameter.needs()]), pmap[pdid]['needs'])
        #     self.assertEqual(sorted(parameter.needs_cc()), pmap[pdid]['cc'])

    def test_streams(self):
        """
        Test if we can retrieve a stream by name and verify that it contains
        the correct parameters.
        :return:
        """
        stream = Stream.query.filter(Stream.name == 'thsph_sample').first()
        self.assertEqual(stream.name, 'thsph_sample')
        self.assertEqual([p.id for p in stream.parameters],
                         [7, 10, 11, 12, 863, 2260, 2261, 2262, 2263,
                          2264, 2265, 2266, 2267, 2624, 2625, 2626,
                          2627, 2628, 2629, 2630, 2631, 2632, 2633, 2634, 2635])

    def test_msgpack(self):
        """
        Create a DataParameter, msgpack it, then verify we can retrieve the
        original message contents.
        :return:
        """
        subsite = 'XXXX'
        node = 'XXX0X'
        sensor = 'FAKEAA001'
        stream = 'BOGUS'
        method = 'telepathy'
        parameter = Parameter.query.get(193)
        p = DataParameter(subsite, node, sensor, stream, method, parameter)
        p.data = numpy.array([[1, 2, 3], [4, 5, 6]])
        p.shape = p.data.shape

        packed = msgpack_one(p)
        unpacked = msgpack.unpackb(base64.b64decode(packed['data']))
        self.assertTrue(numpy.array_equal(p.data, numpy.array(unpacked).reshape(p.shape)))