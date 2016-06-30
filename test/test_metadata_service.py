import engine
import unittest
from util.metadata_service import *
from util.metadata_service.api import (_streamUrl, _partitionUrl, _get_json, _post_json, _put_json, _delete_json, _build_stream_metadata_record, _build_partition_metadata_record)
import util.metadata_service.api
import util.metadata_service.partition
import util.metadata_service.stream
from util.common import TimeRange, StreamKey

# Hide logging messages
logging.getLogger("requests").setLevel(logging.CRITICAL)
util.metadata_service.api._log.setLevel(logging.CRITICAL)
util.metadata_service.partition._log.setLevel(logging.CRITICAL)
util.metadata_service.stream._log.setLevel(logging.CRITICAL)

# Make the StreamKey class usable
from preload_database.database import initialize_connection, open_connection, PreloadDatabaseMode
initialize_connection(PreloadDatabaseMode.POPULATED_MEMORY)
open_connection()

class MetadataServiceTest(unittest.TestCase):

    def _stream_test_setup(self, subsite_suffix='', node_suffix='', sensor_suffix='', method_suffix='', stream_suffix=''):
        subsite = 'test_subsite' + str(subsite_suffix)
        node = 'test_node' + str(node_suffix)
        sensor = 'test_sensor' + str(sensor_suffix)
        method = 'test_method' + str(method_suffix)
        stream = 'test_stream' + str(stream_suffix)
        first = 1.1
        last = 2.2
        count = 3
        rec = _build_stream_metadata_record(subsite, node, sensor, method, stream, first, last, count)
        response = _post_json(_streamUrl, rec)
        sk = StreamKey(subsite, node, sensor, method, stream)
        return rec, response, sk


    def _stream_test_cleanup(self, response):
        url = '/'.join((_streamUrl, str(response['id'])))
        _delete_json(url)


    def _partition_test_setup(self, bin, store, first, last, count):
        subsite = 'test_subsite'
        node = 'test_node'
        sensor = 'test_sensor'
        method = 'test_method'
        stream = 'test_stream'
        rec = _build_partition_metadata_record(subsite, node, sensor, method, stream, bin, store, first, last, count)
        response = _post_json(_partitionUrl, rec)
        sk = StreamKey(subsite, node, sensor, method, stream)
        return rec, response, sk


    def _partition_test_cleanup(self, response):
        url = '/'.join((_partitionUrl, str(response['id'])))
        _delete_json(url)

    ###############
    # API Methods #
    ###############

    def test_get_stream_metadata_records(self):
        recList = []
        responseList = []
        for stream_suffix in range(0, 5):
            rec, response, _ = self._stream_test_setup(stream_suffix=stream_suffix)
            recList.append(rec)
            responseList.append(response)
        try:
            actual_result = get_stream_metadata_records()
            for rec in actual_result:
                self.assertIn('id', rec)
                del rec['id']
                self.assertIn(rec, recList)
        finally:
            for response in responseList:
                self._stream_test_cleanup(response)


    def test_get_stream_metadata_record(self):
        self.test_get_stream_metadata_record_not_found()
        rec, response, sk = self._stream_test_setup()
        try:
            actual_result = get_stream_metadata_record(sk)
            self.assertIn('id', actual_result)
            self.assertEqual(actual_result['id'], response['id'])
            del actual_result['id']
            self.assertEqual(actual_result, rec)
        finally:
            self._stream_test_cleanup(response)
            self.test_get_stream_metadata_record_not_found()


    def test_get_stream_metadata_record_not_found(self):
        subsite = 'test_subsite'
        node = 'test_node'
        sensor = 'test_sensor'
        method = 'test_method'
        stream = 'test_stream'
        sk = StreamKey(subsite, node, sensor, method, stream)
        actual_result = get_stream_metadata_record(sk)
        self.assertIsNone(actual_result)


    def test_get_partition_metadata_records(self):
        recList = []
        responseList = []
        for bin in range(0, 5):
            rec, response, sk = self._partition_test_setup(bin, 'test_store', 2.2, 3.3, 4)
            recList.append(rec)
            responseList.append(response)
        try:
            actual_result = get_partition_metadata_records(sk)
            for rec in actual_result:
                self.assertIn('id', rec)
                del rec['id']
                self.assertIn(rec, recList)
        finally:
            for response in responseList:
                self._partition_test_cleanup(response)


    def test_get_partition_metadata_record(self):
        self.test_get_partition_metadata_record_not_found()
        rec, response, sk = self._partition_test_setup(1, 'test_store', 2.2, 3.3, 4)
        try:
            actual_result = get_partition_metadata_record(sk, 1, 'test_store')
            self.assertIn('id', actual_result)
            self.assertEqual(actual_result['id'], response['id'])
            del actual_result['id']
            self.assertEqual(actual_result, rec)
        finally:
            self._partition_test_cleanup(response)
            self.test_get_partition_metadata_record_not_found()


    def test_get_partition_metadata_record_not_found(self):
        subsite = 'test_subsite'
        node = 'test_node'
        sensor = 'test_sensor'
        method = 'test_method'
        stream = 'test_stream'
        sk = StreamKey(subsite, node, sensor, method, stream)
        actual_result = get_partition_metadata_record(sk, 1, 'test_store')
        self.assertIsNone(actual_result)


    def test_create_partition_metadata_record(self):
        self.test_get_partition_metadata_record_not_found()
        try:
            sk = StreamKey('test_subsite', 'test_node', 'test_sensor', 'test_method', 'test_stream')
            actual_result = create_partition_metadata_record(sk, 1, 'test_store', 2.2, 3.3, 4)
            self.assertIn(('message', 'Element created successfully.'), actual_result.items())
            self.assertIn(('statusCode', 'CREATED'), actual_result.items())
            self.assertIn('id', actual_result)
            self.assertIsInstance(actual_result['id'], int)
        finally:
            self._partition_test_cleanup(actual_result)
            self.test_get_partition_metadata_record_not_found()


    def test_update_partition_metadata_record(self):
        self.test_get_partition_metadata_record_not_found()
        rec, response, sk = self._partition_test_setup(1, 'test_store', 2.2, 3.3, 4)
        try:
            rec['id'] = response['id']
            rec['count'] = 5
            actual_result = update_partition_metadata_record(rec)
            expected_result = {
                'message' : 'Element updated successfully.',
                'id' : response['id'],
                'statusCode' : 'OK'
            }
            self.assertEqual(actual_result, expected_result)
            # Check update worked
            rec_updated = get_partition_metadata_record(sk, 1, 'test_store')
            self.assertEqual(rec_updated, rec)
        finally:
            self._partition_test_cleanup(response)
            self.test_get_partition_metadata_record_not_found()

    ###################
    # Stream  Methods #
    ###################

    def test_build_stream_dictionary(self):
        expected_result = {
            'test_stream0' : {
                'test_method0' : {
                    'test_subsite0' : {
                        'test_node0' : [ 'test_sensor0', 'test_sensor1' ],
                        'test_node1' : [ 'test_sensor0', 'test_sensor1' ],
                    },
                    'test_subsite1' : {
                        'test_node0' : [ 'test_sensor0', 'test_sensor1' ],
                        'test_node1' : [ 'test_sensor0', 'test_sensor1' ],
                    }
                },
                'test_method1' : {
                    'test_subsite0' : {
                        'test_node0' : [ 'test_sensor0', 'test_sensor1' ],
                        'test_node1' : [ 'test_sensor0', 'test_sensor1' ],
                    },
                    'test_subsite1' : {
                        'test_node0' : [ 'test_sensor0', 'test_sensor1' ],
                        'test_node1' : [ 'test_sensor0', 'test_sensor1' ],
                    }
                }
            },
            'test_stream1' : {
                'test_method0' : {
                    'test_subsite0' : {
                        'test_node0' : [ 'test_sensor0', 'test_sensor1' ],
                        'test_node1' : [ 'test_sensor0', 'test_sensor1' ],
                    },
                    'test_subsite1' : {
                        'test_node0' : [ 'test_sensor0', 'test_sensor1' ],
                        'test_node1' : [ 'test_sensor0', 'test_sensor1' ],
                    }
                },
                'test_method1' : {
                    'test_subsite0' : {
                        'test_node0' : [ 'test_sensor0', 'test_sensor1' ],
                        'test_node1' : [ 'test_sensor0', 'test_sensor1' ],
                    },
                    'test_subsite1' : {
                        'test_node0' : [ 'test_sensor0', 'test_sensor1' ],
                        'test_node1' : [ 'test_sensor0', 'test_sensor1' ],
                    }
                }
            }
        }
        responseList = []
        for subsite_suffix in range(0, 2):
            for node_suffix in range(0, 2):
                for sensor_suffix in range(0, 2):
                    for method_suffix in range(0, 2):
                        for stream_suffix in range(0, 2):
                            _, response, _ = self._stream_test_setup(subsite_suffix, node_suffix, sensor_suffix, method_suffix, stream_suffix)
                            responseList.append(response)
        try:
            actual_result = build_stream_dictionary()
            self.assertEqual(actual_result, expected_result)
        finally:
            for response in responseList:
                self._stream_test_cleanup(response)


    def test_get_available_time_range(self):
        rec, response, sk = self._stream_test_setup()
        try:
            expected_result = TimeRange(rec['first'], rec['last'] + 1)
            actual_result = get_available_time_range(sk)
            self.assertEqual(actual_result, expected_result)
        finally:
            self._stream_test_cleanup(response)

    #####################
    # Partition Methods #
    #####################

    def test_get_location_metadata_by_store(self):
        bin_min = 10
        bin_max = bin_min + 10
        tr = TimeRange(bin_min + (engine.app.config['MAX_BIN_SIZE_MIN'] * 60), bin_max)

        # Filtered - bad bin (too small), right store
        _, response1, sk = self._partition_test_setup(bin_min - 1, CASS_LOCATION_NAME, 1.1, 6.6, 100)
        # Filtered - bad bin (too big), right store
        _, response2, _ = self._partition_test_setup(bin_max + 1, CASS_LOCATION_NAME, 2.2, 5.5, 200)
        # Filtered - good bin, wrong store
        _, response3, _ = self._partition_test_setup(bin_min + 1, SAN_LOCATION_NAME, 3.3, 4.4, 300)

        # 3 good ones
        _, response4, _ = self._partition_test_setup(bin_min + 2, CASS_LOCATION_NAME, 4.4, 3.3, 11)
        _, response5, _ = self._partition_test_setup(bin_min + 3, CASS_LOCATION_NAME, 5.5, 2.2, 12)
        _, response6, _ = self._partition_test_setup(bin_min + 4, CASS_LOCATION_NAME, 6.6, 1.1, 13)

        try:
            actual_result = get_location_metadata_by_store(sk, tr, CASS_LOCATION_NAME)
            self.assertEqual(actual_result.total, 36)
            self.assertEqual(actual_result.start_time, 4.4)
            self.assertEqual(actual_result.end_time, 3.3)
            self.assertItemsEqual(actual_result.bin_list, [bin_min + 2, bin_min + 3, bin_min + 4])
            bins = {
                bin_min + 2 : (11, 4.4, 3.3),
                bin_min + 3 : (12, 5.5, 2.2),
                bin_min + 4 : (13, 6.6, 1.1)
            }
            self.assertEqual(actual_result.bin_information, bins)
        finally:
            self._partition_test_cleanup(response1)
            self._partition_test_cleanup(response2)
            self._partition_test_cleanup(response3)
            self._partition_test_cleanup(response4)
            self._partition_test_cleanup(response5)
            self._partition_test_cleanup(response6)


    def test_get_first_before_metadata(self):
        bin_max = 10

        # Filtered - bad bin (too big), good first
        _, response1, sk = self._partition_test_setup(bin_max + 1, CASS_LOCATION_NAME, bin_max, 1.1, 100)
        # Filtered - bad bin (5th - sorted out), good first
        _, response2, _ = self._partition_test_setup(bin_max - 3, CASS_LOCATION_NAME, bin_max, 2.2, 101)
        # Filtered - good bin, bad first (too big)
        _, response3, _ = self._partition_test_setup(bin_max, CASS_LOCATION_NAME, bin_max + 1, 3.3, 102)
        # Filtered - bad bin (3rd - ignored), good first
        _, response4, _ = self._partition_test_setup(bin_max - 2, CASS_LOCATION_NAME, bin_max - 1, 4.4, 103)

        # Override config setting
        engine.app.config['PREFERRED_DATA_LOCATION'] = CASS_LOCATION_NAME

        # 1 good one
        _, response5, _ = self._partition_test_setup(bin_max - 1, CASS_LOCATION_NAME, bin_max - 2, 5.5, 104)

        # Filtered - good bin, good first, wrong store
        _, response6, _ = self._partition_test_setup(bin_max - 1, SAN_LOCATION_NAME, bin_max - 3, 6.6, 104)

        try:
            actual_result = get_first_before_metadata(sk, bin_max)
            self.assertIn(CASS_LOCATION_NAME, actual_result)
            self.assertEqual(actual_result[CASS_LOCATION_NAME].total, 104)
            self.assertEqual(actual_result[CASS_LOCATION_NAME].start_time, bin_max - 2)
            self.assertEqual(actual_result[CASS_LOCATION_NAME].end_time, 5.5)
            self.assertItemsEqual(actual_result[CASS_LOCATION_NAME].bin_list, [bin_max - 1])
            self.assertEqual(actual_result[CASS_LOCATION_NAME].bin_information, { bin_max - 1 : (104, bin_max - 2, 5.5) })
        finally:
            self._partition_test_cleanup(response1)
            self._partition_test_cleanup(response2)
            self._partition_test_cleanup(response3)
            self._partition_test_cleanup(response4)
            self._partition_test_cleanup(response5)
            self._partition_test_cleanup(response6)


    def test_get_location_metadata(self):
        bin_min = 10
        bin_max = bin_min + 10
        tr = TimeRange(bin_min + (engine.app.config['MAX_BIN_SIZE_MIN'] * 60), bin_max)

        # Filtered - bad bin (too small), good first, good last
        _, response1, sk = self._partition_test_setup(bin_min - 1, CASS_LOCATION_NAME, tr.stop, tr.start, 100)
        # Filtered - bad bin (too big), good first, good last
        _, response2, _ = self._partition_test_setup(bin_max + 1, CASS_LOCATION_NAME, tr.stop, tr.start, 200)
        # Filtered - good bin, bad first (too big), good last
        _, response3, _ = self._partition_test_setup(bin_min + 1, SAN_LOCATION_NAME, tr.stop + 1, tr.start, 300)
        # Filtered - good bin, good first, bad last (too small)
        _, response4, _ = self._partition_test_setup(bin_min + 2, SAN_LOCATION_NAME, tr.stop, tr.start - 1, 400)

        # 4 good ones
        _, response5, _ = self._partition_test_setup(bin_min + 3, CASS_LOCATION_NAME, tr.stop - 1, tr.start + 1, 11)
        _, response6, _ = self._partition_test_setup(bin_min + 4, CASS_LOCATION_NAME, tr.stop - 2, tr.start + 2, 12)
        _, response7, _ = self._partition_test_setup(bin_min + 5, SAN_LOCATION_NAME, tr.stop - 3, tr.start + 3, 13)
        _, response8, _ = self._partition_test_setup(bin_min + 6, SAN_LOCATION_NAME, tr.stop - 4, tr.start + 4, 14)

        # Override config setting
        engine.app.config['PREFERRED_DATA_LOCATION'] = CASS_LOCATION_NAME

        # Filtered - In both CASS and SAN (same bin, same count) - without warning
        _, response9, _ = self._partition_test_setup(bin_min + 3, SAN_LOCATION_NAME, tr.stop - 1, tr.start + 1, 11)
        # Filtered - In both CASS and SAN (same bin, different count) - with warning
        _, response0, _ = self._partition_test_setup(bin_min + 4, SAN_LOCATION_NAME, tr.stop - 2, tr.start + 2, 10)

        try:
            cass_loc_meta, san_loc_meta, messages = get_location_metadata(sk, tr)

            self.assertEqual(cass_loc_meta.total, 23)
            self.assertEqual(cass_loc_meta.start_time, tr.stop - 2)
            self.assertEqual(cass_loc_meta.end_time, tr.start + 2)
            self.assertItemsEqual(cass_loc_meta.bin_list, [bin_min + 3, bin_min + 4])
            cass_bins = {
                bin_min + 3 : (11, tr.stop - 1, tr.start + 1),
                bin_min + 4 : (12, tr.stop - 2, tr.start + 2)
            }
            self.assertEqual(cass_loc_meta.bin_information, cass_bins)

            self.assertEqual(san_loc_meta.total, 27)
            self.assertEqual(san_loc_meta.start_time, tr.stop - 4)
            self.assertEqual(san_loc_meta.end_time, tr.start + 4)
            self.assertItemsEqual(san_loc_meta.bin_list, [bin_min + 5, bin_min + 6])
            san_bins = {
                bin_min + 5 : (13, tr.stop - 3, tr.start + 3),
                bin_min + 6 : (14, tr.stop - 4, tr.start + 4)
            }
            self.assertEqual(san_loc_meta.bin_information, san_bins)

            expected_messages = [
                "Metadata count does not match for bin {0} - SAN: {1} CASS: {2}. Took location with highest data count.".format(bin_min + 4, 10, 12)
            ]
            self.assertItemsEqual(messages, expected_messages)
        finally:
            self._partition_test_cleanup(response1)
            self._partition_test_cleanup(response2)
            self._partition_test_cleanup(response3)
            self._partition_test_cleanup(response4)
            self._partition_test_cleanup(response5)
            self._partition_test_cleanup(response6)
            self._partition_test_cleanup(response7)
            self._partition_test_cleanup(response8)
            self._partition_test_cleanup(response9)
            self._partition_test_cleanup(response0)

