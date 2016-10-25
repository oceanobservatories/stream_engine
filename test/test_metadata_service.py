import global_test_setup

import copy
import engine
import mock
import unittest
import util.metadata_service
from preload_database.database import initialize_connection, open_connection, PreloadDatabaseMode
from util.common import TimeRange, StreamKey, MissingStreamMetadataException
from util.metadata_service import CASS_LOCATION_NAME, SAN_LOCATION_NAME


# Make the StreamKey class usable
initialize_connection(PreloadDatabaseMode.POPULATED_MEMORY)
open_connection()


class MockMetadataServiceAPI(object):
    '''Mock version of the util.metadata_service.metadata_service_api.api.MetadataServiceAPI class.'''

    def __init__(self):
        self.test_clean_up()

    def test_clean_up(self):
        self.stream_metadata_records = []
        self.partition_metadata_records = []

    def test_add_stream_metadata_record(self, subsite, node, sensor, method, stream, first, last, count):
        new_record = util.metadata_service.MetadataServiceAPI.build_stream_metadata_record(
            subsite, node, sensor, method, stream, first, last, count
        )
        self.stream_metadata_records.append(new_record)

    def test_add_partition_metadata_record(self, subsite, node, sensor, method, stream, bin, store, first, last, count):
        new_record = util.metadata_service.MetadataServiceAPI.build_partition_metadata_record(
            subsite, node, sensor, method, stream, bin, store, first, last, count
        )
        self.partition_metadata_records.append(new_record)

    def __check_record_equals(self, record, subsite, node, sensor, method=None, stream=None):
        if record['referenceDesignator']['subsite'] == subsite and \
           record['referenceDesignator']['node'] == node and \
           record['referenceDesignator']['sensor'] == sensor and \
           ((not method and not stream) or (record['method'] == method and record['stream'] == stream)):
            return True
        else:
            return False

    ##################
    # Mocked Methods #
    ##################

    def get_stream_metadata_records(self):
        return copy.deepcopy(self.stream_metadata_records)

    def get_stream_metadata_record(self, subsite, node, sensor, method, stream):
        result = None
        for rec in self.stream_metadata_records:
            if self.__check_record_equals(rec, subsite, node, sensor, method, stream):
                result = copy.deepcopy(rec)
                break
        return result

    def get_partition_metadata_records(self, subsite, node, sensor, method=None, stream=None):
        return [copy.deepcopy(rec) for rec in self.partition_metadata_records
                if self.__check_record_equals(rec, subsite, node, sensor, method, stream)]


# Mock api object patched into the modules under test and used below.
mock_metadata_service_api = MockMetadataServiceAPI()


@mock.patch('util.metadata_service.stream.metadata_service_api', new=mock_metadata_service_api)
@mock.patch('util.metadata_service.partition.metadata_service_api', new=mock_metadata_service_api)
class MetadataServiceTest(unittest.TestCase):
    '''Unit tests for the util.metadata_service.stream & util.metadata_service.partition modules.'''

    def setUp(self):
        mock_metadata_service_api.test_clean_up()

    def __build_knockoff_stream_key(self):
        subsite = 'test_subsite'
        node = 'test_node'
        sensor = 'test_sensor'
        method = 'test_method'
        stream = 'test_stream'
        return subsite, node, sensor, method, stream

    def __stream_test_setup(self, first, last, count, subsite_suffix='', node_suffix='', sensor_suffix='',
                            method_suffix='', stream_suffix=''):
        sk = self.__build_knockoff_stream_key()
        sk = list(sk)
        sk[0] += str(subsite_suffix)
        sk[1] += str(node_suffix)
        sk[2] += str(sensor_suffix)
        sk[3] += str(method_suffix)
        sk[4] += str(stream_suffix)
        sk = tuple(sk)
        mock_metadata_service_api.test_add_stream_metadata_record(*(sk + (first, last, count)))
        return StreamKey(*sk)

    def __partition_test_setup(self, bin, store, first, last, count, subsite_suffix='', node_suffix='',
                               sensor_suffix='', method_suffix='', stream_suffix=''):
        sk = self.__build_knockoff_stream_key()
        sk = list(sk)
        sk[0] += str(subsite_suffix)
        sk[1] += str(node_suffix)
        sk[2] += str(sensor_suffix)
        sk[3] += str(method_suffix)
        sk[4] += str(stream_suffix)
        sk = tuple(sk)
        mock_metadata_service_api.test_add_partition_metadata_record(*(sk + (bin, store, first, last, count)))
        return StreamKey(*sk)

    ###################
    # Stream  Methods #
    ###################

    def test_build_stream_dictionary(self):
        ##############
        # Test Setup #
        ##############
        expected_result = {
            'test_stream0': {
                'test_method0': {
                    'test_subsite0': {
                        'test_node0': ['test_sensor0', 'test_sensor1'],
                        'test_node1': ['test_sensor0', 'test_sensor1'],
                    },
                    'test_subsite1': {
                        'test_node0': ['test_sensor0', 'test_sensor1'],
                        'test_node1': ['test_sensor0', 'test_sensor1'],
                    }
                },
                'test_method1': {
                    'test_subsite0': {
                        'test_node0': ['test_sensor0', 'test_sensor1'],
                        'test_node1': ['test_sensor0', 'test_sensor1'],
                    },
                    'test_subsite1': {
                        'test_node0': ['test_sensor0', 'test_sensor1'],
                        'test_node1': ['test_sensor0', 'test_sensor1'],
                    }
                }
            },
            'test_stream1': {
                'test_method0': {
                    'test_subsite0': {
                        'test_node0': ['test_sensor0', 'test_sensor1'],
                        'test_node1': ['test_sensor0', 'test_sensor1'],
                    },
                    'test_subsite1': {
                        'test_node0': ['test_sensor0', 'test_sensor1'],
                        'test_node1': ['test_sensor0', 'test_sensor1'],
                    }
                },
                'test_method1': {
                    'test_subsite0': {
                        'test_node0': ['test_sensor0', 'test_sensor1'],
                        'test_node1': ['test_sensor0', 'test_sensor1'],
                    },
                    'test_subsite1': {
                        'test_node0': ['test_sensor0', 'test_sensor1'],
                        'test_node1': ['test_sensor0', 'test_sensor1'],
                    }
                }
            }
        }
        for subsite_suffix in range(2):
            for node_suffix in range(2):
                for sensor_suffix in range(2):
                    for method_suffix in range(2):
                        for stream_suffix in range(2):
                            self.__stream_test_setup(
                                1.1, 2.2, 3, subsite_suffix, node_suffix, sensor_suffix, method_suffix, stream_suffix
                            )
        ########
        # Test #
        ########
        actual_result = util.metadata_service.build_stream_dictionary()
        self.assertEqual(actual_result, expected_result)

    def test_get_available_time_range(self):
        ##############
        # Test Setup #
        ##############
        test_first = 1.1
        test_last = 2.2
        sk = self.__stream_test_setup(test_first, test_last, 3)
        expected_result = TimeRange(test_first, test_last + 1)
        ########
        # Test #
        ########
        actual_result = util.metadata_service.get_available_time_range(sk)
        self.assertEqual(actual_result, expected_result)

    #####################
    # Partition Methods #
    #####################

    def test_get_location_metadata_by_store(self):
        ##############
        # Test Setup #
        ##############
        bin_min = 10
        bin_max = bin_min + 10
        tr = TimeRange(bin_min + (engine.app.config['MAX_BIN_SIZE_MIN'] * 60), bin_max)

        # Filtered - bad bin (too small), right store
        sk = self.__partition_test_setup(bin_min - 1, CASS_LOCATION_NAME, 1.1, 6.6, 100)
        # Filtered - bad bin (too big), right store
        self.__partition_test_setup(bin_max + 1, CASS_LOCATION_NAME, 2.2, 5.5, 200)
        # Filtered - good bin, wrong store
        self.__partition_test_setup(bin_min + 1, SAN_LOCATION_NAME, 3.3, 4.4, 300)

        # 3 good ones
        self.__partition_test_setup(bin_min + 2, CASS_LOCATION_NAME, 4.4, 3.3, 11)
        self.__partition_test_setup(bin_min + 3, CASS_LOCATION_NAME, 5.5, 2.2, 12)
        self.__partition_test_setup(bin_min + 4, CASS_LOCATION_NAME, 6.6, 1.1, 13)
        ########
        # Test #
        ########
        actual_result = util.metadata_service.get_location_metadata_by_store(sk, tr, CASS_LOCATION_NAME)
        self.assertEqual(actual_result.total, 36)
        self.assertEqual(actual_result.start_time, 4.4)
        self.assertEqual(actual_result.end_time, 3.3)
        self.assertItemsEqual(actual_result.bin_list, [bin_min + 2, bin_min + 3, bin_min + 4])
        bins = {
            bin_min + 2: (11, 4.4, 3.3),
            bin_min + 3: (12, 5.5, 2.2),
            bin_min + 4: (13, 6.6, 1.1)
        }
        self.assertEqual(actual_result.bin_information, bins)

    def test_get_first_before_metadata(self):
        ##############
        # Test Setup #
        ##############
        bin_max = 10

        # Filtered - bad bin (too big), good first
        sk = self.__partition_test_setup(bin_max + 1, CASS_LOCATION_NAME, bin_max, 1.1, 100)
        # Filtered - bad bin (5th - sorted out), good first
        self.__partition_test_setup(bin_max - 3, CASS_LOCATION_NAME, bin_max, 2.2, 101)
        # Filtered - good bin, bad first (too big)
        self.__partition_test_setup(bin_max, CASS_LOCATION_NAME, bin_max + 1, 3.3, 102)
        # Filtered - bad bin (3rd - ignored), good first
        self.__partition_test_setup(bin_max - 2, CASS_LOCATION_NAME, bin_max - 1, 4.4, 103)

        # Override config setting
        engine.app.config['PREFERRED_DATA_LOCATION'] = CASS_LOCATION_NAME

        # 1 good one
        self.__partition_test_setup(bin_max - 1, CASS_LOCATION_NAME, bin_max - 2, 5.5, 104)

        # Filtered - good bin, good first, wrong store
        self.__partition_test_setup(bin_max - 1, SAN_LOCATION_NAME, bin_max - 3, 6.6, 104)
        ########
        # Test #
        ########
        actual_result = util.metadata_service.get_first_before_metadata(sk, bin_max)
        self.assertIn(CASS_LOCATION_NAME, actual_result)
        self.assertEqual(actual_result[CASS_LOCATION_NAME].total, 104)
        self.assertEqual(actual_result[CASS_LOCATION_NAME].start_time, bin_max - 2)
        self.assertEqual(actual_result[CASS_LOCATION_NAME].end_time, 5.5)
        self.assertItemsEqual(actual_result[CASS_LOCATION_NAME].bin_list, [bin_max - 1])
        self.assertEqual(actual_result[CASS_LOCATION_NAME].bin_information, {bin_max - 1: (104, bin_max - 2, 5.5)})

    def test_get_location_metadata(self):
        ##############
        # Test Setup #
        ##############
        bin_min = 10
        bin_max = bin_min + 10
        tr = TimeRange(bin_min + (engine.app.config['MAX_BIN_SIZE_MIN'] * 60), bin_max)

        # Filtered - bad bin (too small), good first, good last
        sk = self.__partition_test_setup(bin_min - 1, CASS_LOCATION_NAME, tr.stop, tr.start, 100)
        # Filtered - bad bin (too big), good first, good last
        self.__partition_test_setup(bin_max + 1, CASS_LOCATION_NAME, tr.stop, tr.start, 200)
        # Filtered - good bin, bad first (too big), good last
        self.__partition_test_setup(bin_min + 1, SAN_LOCATION_NAME, tr.stop + 1, tr.start, 300)
        # Filtered - good bin, good first, bad last (too small)
        self.__partition_test_setup(bin_min + 2, SAN_LOCATION_NAME, tr.stop, tr.start - 1, 400)

        # 4 good ones
        self.__partition_test_setup(bin_min + 3, CASS_LOCATION_NAME, tr.stop - 1, tr.start + 1, 11)
        self.__partition_test_setup(bin_min + 4, CASS_LOCATION_NAME, tr.stop - 2, tr.start + 2, 12)
        self.__partition_test_setup(bin_min + 5, SAN_LOCATION_NAME, tr.stop - 3, tr.start + 3, 13)
        self.__partition_test_setup(bin_min + 6, SAN_LOCATION_NAME, tr.stop - 4, tr.start + 4, 14)

        # Override config setting
        engine.app.config['PREFERRED_DATA_LOCATION'] = CASS_LOCATION_NAME

        # Filtered - In both CASS and SAN (same bin, same count) - without warning
        self.__partition_test_setup(bin_min + 3, SAN_LOCATION_NAME, tr.stop - 1, tr.start + 1, 11)
        # Filtered - In both CASS and SAN (same bin, different count) - with warning
        self.__partition_test_setup(bin_min + 4, SAN_LOCATION_NAME, tr.stop - 2, tr.start + 2, 10)
        ########
        # Test #
        ########
        cass_loc_meta, san_loc_meta, messages = util.metadata_service.get_location_metadata(sk, tr)

        self.assertEqual(cass_loc_meta.total, 23)
        self.assertEqual(cass_loc_meta.start_time, tr.stop - 2)
        self.assertEqual(cass_loc_meta.end_time, tr.start + 2)
        self.assertItemsEqual(cass_loc_meta.bin_list, [bin_min + 3, bin_min + 4])
        cass_bins = {
            bin_min + 3: (11, tr.stop - 1, tr.start + 1),
            bin_min + 4: (12, tr.stop - 2, tr.start + 2)
        }
        self.assertEqual(cass_loc_meta.bin_information, cass_bins)

        self.assertEqual(san_loc_meta.total, 27)
        self.assertEqual(san_loc_meta.start_time, tr.stop - 4)
        self.assertEqual(san_loc_meta.end_time, tr.start + 4)
        self.assertItemsEqual(san_loc_meta.bin_list, [bin_min + 5, bin_min + 6])
        san_bins = {
            bin_min + 5: (13, tr.stop - 3, tr.start + 3),
            bin_min + 6: (14, tr.stop - 4, tr.start + 4)
        }
        self.assertEqual(san_loc_meta.bin_information, san_bins)

        expected_messages = [
            "Metadata count does not match for bin {0} - SAN: {1} CASS: {2}. Took location with highest data count."
            .format(bin_min + 4, 10, 12)
        ]
        self.assertItemsEqual(messages, expected_messages)

    def test_missing_stream(self):
        with self.assertRaises(MissingStreamMetadataException):
            sk = StreamKey(*['fake'] * 5)
            util.metadata_service.get_available_time_range(sk)

    def test_get_particle_count(self):
        ##############
        # Test Setup #
        ##############
        bin_min = 10
        bin_max = bin_min + 10
        tr = TimeRange(bin_min, bin_max)

        sk = self.__partition_test_setup(bin_min + 3, CASS_LOCATION_NAME, tr.stop - 1, tr.start + 1, 11)
        self.__partition_test_setup(bin_min + 4, CASS_LOCATION_NAME, tr.stop - 2, tr.start + 2, 12)
        self.__partition_test_setup(bin_min + 5, SAN_LOCATION_NAME, tr.stop - 3, tr.start + 3, 13)
        self.__partition_test_setup(bin_min + 6, SAN_LOCATION_NAME, tr.stop - 4, tr.start + 4, 14)

        count = 11 + 12 + 13 + 14

        # Override config setting
        engine.app.config['PREFERRED_DATA_LOCATION'] = CASS_LOCATION_NAME

        ########
        # Test #
        ########
        print tr
        self.assertEqual(util.metadata_service.get_particle_count(sk, tr), count)

