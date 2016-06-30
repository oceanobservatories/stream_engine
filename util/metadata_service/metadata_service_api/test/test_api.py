##################################################################################
# Note: This is an integration test that needs uFrame running with empty         #
#       StreamMetadataRecord & PartitionMetadataRecord tables within PostgreSQL. #
##################################################################################

import requests
import unittest
from api import MetadataServiceAPI, MetadataServiceException

STREAM_METADATA_SERVICE_URL = 'http://127.0.0.1:12571/streamMetadata'
PARTITION_METADATA_SERVICE_URL = 'http://127.0.0.1:12571/partitionMetadata'


# Api object connected to live uFrame.
metadata_service_api = MetadataServiceAPI(STREAM_METADATA_SERVICE_URL, PARTITION_METADATA_SERVICE_URL)


class MetadataServiceTest(unittest.TestCase):
    '''Integration tests for the api.MetadataServiceAPI class.'''

    def setUp(self):
        self.stream_metadata_responses = []
        self.partition_metadata_responses = []

    def tearDown(self):
        for response in self.stream_metadata_responses:
            url = '/'.join((STREAM_METADATA_SERVICE_URL, str(response['id'])))
            metadata_service_api._MetadataServiceAPI__delete_json(url)
        for response in self.partition_metadata_responses:
            url = '/'.join((PARTITION_METADATA_SERVICE_URL, str(response['id'])))
            metadata_service_api._MetadataServiceAPI__delete_json(url)

    def __build_knockoff_stream_key(self):
        subsite = 'test_subsite'
        node = 'test_node'
        sensor = 'test_sensor'
        method = 'test_method'
        stream = 'test_stream'
        return subsite, node, sensor, method, stream

    def __stream_test_setup(self, first, last, count, subsite_suffix='', node_suffix='', sensor_suffix='', method_suffix='', stream_suffix=''):
        sk = self.__build_knockoff_stream_key()
        sk = list(sk)
        sk[0] += str(subsite_suffix)
        sk[1] += str(node_suffix)
        sk[2] += str(sensor_suffix)
        sk[3] += str(method_suffix)
        sk[4] += str(stream_suffix)
        sk = tuple(sk)
        rec = metadata_service_api.build_stream_metadata_record(*(sk + (first, last, count)))
        response = metadata_service_api._MetadataServiceAPI__post_json(STREAM_METADATA_SERVICE_URL, rec)
        self.stream_metadata_responses.append(response)
        return sk, rec, response

    def __partition_test_setup(self, bin, store, first, last, count, subsite_suffix='', node_suffix='', sensor_suffix='', method_suffix='', stream_suffix=''):
        sk = self.__build_knockoff_stream_key()
        sk = list(sk)
        sk[0] += str(subsite_suffix)
        sk[1] += str(node_suffix)
        sk[2] += str(sensor_suffix)
        sk[3] += str(method_suffix)
        sk[4] += str(stream_suffix)
        sk = tuple(sk)
        rec = metadata_service_api.build_partition_metadata_record(*(sk + (bin, store, first, last, count)))
        response = metadata_service_api._MetadataServiceAPI__post_json(PARTITION_METADATA_SERVICE_URL, rec)
        self.partition_metadata_responses.append(response)
        return sk, rec, response

    ##################################
    # Stream Metadata Record Methods #
    ##################################

    def test_get_stream_metadata_records(self):
        ##############
        # Test Setup #
        ##############
        recList = []
        for stream_suffix in range(5):
            _, rec, _ = self.__stream_test_setup(1.1, 2.2, 3, stream_suffix=stream_suffix)
            recList.append(rec)
        ########
        # Test #
        ########
        actual_result = metadata_service_api.get_stream_metadata_records()
        for rec in actual_result:
            self.assertIn('id', rec)
            self.assertIsInstance(rec['id'], int)
            del rec['id']
        self.assertItemsEqual(actual_result, recList)

    def test_get_stream_metadata_record(self):
        ##############
        # Test Setup #
        ##############
        self.test_get_stream_metadata_record_not_found() # Confirm record doesn't already exist
        sk, rec, response = self.__stream_test_setup(1.1, 2.2, 3)
        ########
        # Test #
        ########
        actual_result = metadata_service_api.get_stream_metadata_record(*sk)
        self.assertIn('id', actual_result)
        self.assertIsInstance(actual_result['id'], int)
        self.assertEqual(actual_result['id'], response['id'])
        del actual_result['id']
        self.assertEqual(actual_result, rec)

    def test_get_stream_metadata_record_not_found(self):
        ##############
        # Test Setup #
        ##############
        sk = self.__build_knockoff_stream_key()
        ########
        # Test #
        ########
        actual_result = metadata_service_api.get_stream_metadata_record(*sk)
        self.assertIsNone(actual_result)

    def test_delete_stream_metadata_records(self):
        ##############
        # Test Setup #
        ##############
        # Filtered - different subsite
        _, _, response1 = self.__stream_test_setup(1.1, 2.2, 3, subsite_suffix='0')
        # Filtered - different node
        _, _, response2 = self.__stream_test_setup(1.1, 2.2, 3, node_suffix='0')
        # Filtered - different sensor
        _, _, response3 = self.__stream_test_setup(1.1, 2.2, 3, sensor_suffix='0')

        # 3 good ones
        _, _, response4 = self.__stream_test_setup(1.1, 2.2, 3, method_suffix='0', stream_suffix='2')
        _, _, response5 = self.__stream_test_setup(2.2, 3.3, 4, method_suffix='1', stream_suffix='1')
        _, _, response6 = self.__stream_test_setup(3.3, 4.4, 5, method_suffix='2', stream_suffix='0')

        ref_des = self.__build_knockoff_stream_key()[0:3]
        expected_result = {
            'message' : 'Successfully deleted 3 records.',
            'id' : None,
            'statusCode' : 'OK'
        }
        ########
        # Test #
        ########
        actual_result = metadata_service_api.delete_stream_metadata_records(*ref_des)
        self.assertEqual(actual_result, expected_result)

        def check_was_not_deleted(id):
            # Verify this doesn't throw a MetadataServiceException
            url = '/'.join((STREAM_METADATA_SERVICE_URL, str(id)))
            response = metadata_service_api._MetadataServiceAPI__get_json(url)

        def check_was_deleted(id):
            with self.assertRaises(MetadataServiceException) as cm:
                url = '/'.join((STREAM_METADATA_SERVICE_URL, str(id)))
                response = metadata_service_api._MetadataServiceAPI__get_json(url)
            self.assertEqual(cm.exception.status_code, requests.codes.not_found)

        check_was_not_deleted(response1['id'])
        check_was_not_deleted(response2['id'])
        check_was_not_deleted(response3['id'])

        check_was_deleted(response4['id'])
        check_was_deleted(response5['id'])
        check_was_deleted(response6['id'])
        ###########
        # Cleanup #
        ###########
        self.stream_metadata_responses.remove(response4)
        self.stream_metadata_responses.remove(response5)
        self.stream_metadata_responses.remove(response6)

    #####################################
    # Partition Metadata Record Methods #
    #####################################

    def test_get_partition_metadata_records_3_param(self):
        ##############
        # Test Setup #
        ##############
        # Filtered - different subsite
        self.__partition_test_setup(1, 'test_store', 1.1, 2.2, 3, subsite_suffix='0')
        # Filtered - different node
        self.__partition_test_setup(1, 'test_store', 1.1, 2.2, 3, node_suffix='0')
        # Filtered - different sensor
        self.__partition_test_setup(1, 'test_store', 1.1, 2.2, 3, sensor_suffix='0')

        recList = []

        # Good - different method
        recList.append(self.__partition_test_setup(1, 'test_store', 1.1, 2.2, 3, method_suffix='0')[1])
        # Good - different stream
        recList.append(self.__partition_test_setup(1, 'test_store', 1.1, 2.2, 3, stream_suffix='0')[1])

        # 5 good ones
        for bin in range(5):
            _, rec, _ = self.__partition_test_setup(bin, 'test_store', 1.1, 2.2, 3)
            recList.append(rec)

        ref_des = self.__build_knockoff_stream_key()[0:3]
        ########
        # Test #
        ########
        actual_result = metadata_service_api.get_partition_metadata_records(*ref_des)
        for rec in actual_result:
            self.assertIn('id', rec)
            self.assertIsInstance(rec['id'], int)
            del rec['id']
        self.assertItemsEqual(actual_result, recList)

    def test_get_partition_metadata_records_5_param(self):
        ##############
        # Test Setup #
        ##############
        # Filtered - different subsite
        self.__partition_test_setup(1, 'test_store', 1.1, 2.2, 3, subsite_suffix='0')
        # Filtered - different node
        self.__partition_test_setup(1, 'test_store', 1.1, 2.2, 3, node_suffix='0')
        # Filtered - different sensor
        self.__partition_test_setup(1, 'test_store', 1.1, 2.2, 3, sensor_suffix='0')
        # Filtered - different method
        self.__partition_test_setup(1, 'test_store', 1.1, 2.2, 3, method_suffix='0')
        # Filtered - different stream
        self.__partition_test_setup(1, 'test_store', 1.1, 2.2, 3, stream_suffix='0')

        recList = []

        # 5 good ones
        for bin in range(5):
            _, rec, _ = self.__partition_test_setup(bin, 'test_store', 1.1, 2.2, 3)
            recList.append(rec)

        sk = self.__build_knockoff_stream_key()
        ########
        # Test #
        ########
        actual_result = metadata_service_api.get_partition_metadata_records(*sk)
        for rec in actual_result:
            self.assertIn('id', rec)
            self.assertIsInstance(rec['id'], int)
            del rec['id']
        self.assertItemsEqual(actual_result, recList)

    def test_get_partition_metadata_record(self):
        ##############
        # Test Setup #
        ##############
        self.test_get_partition_metadata_record_not_found() # Confirm record doesn't already exist
        sk, rec, response = self.__partition_test_setup(1, 'test_store', 1.1, 2.2, 3)
        ########
        # Test #
        ########
        actual_result = metadata_service_api.get_partition_metadata_record(*(sk + (1, 'test_store')))
        self.assertIn('id', actual_result)
        self.assertIsInstance(actual_result['id'], int)
        self.assertEqual(actual_result['id'], response['id'])
        del actual_result['id']
        self.assertEqual(actual_result, rec)

    def test_get_partition_metadata_record_not_found(self):
        ##############
        # Test Setup #
        ##############
        sk = self.__build_knockoff_stream_key()
        ########
        # Test #
        ########
        actual_result = metadata_service_api.get_partition_metadata_record(*(sk + (1, 'test_store')))
        self.assertIsNone(actual_result)

    def test_create_partition_metadata_record(self):
        ##############
        # Test Setup #
        ##############
        self.test_get_partition_metadata_record_not_found() # Confirm record doesn't already exist
        sk = self.__build_knockoff_stream_key()
        rec = metadata_service_api.build_partition_metadata_record(*(sk + (1, 'test_store', 1.1, 2.2, 3)))
        ########
        # Test #
        ########
        actual_result = metadata_service_api.create_partition_metadata_record(rec)
        self.assertIn(('message', 'Element created successfully.'), actual_result.items())
        self.assertIn(('statusCode', 'CREATED'), actual_result.items())
        self.assertIn('id', actual_result)
        self.assertIsInstance(actual_result['id'], int)
        ###########
        # Cleanup #
        ###########
        self.partition_metadata_responses.append(actual_result)

    def test_update_partition_metadata_record(self):
        ##############
        # Test Setup #
        ##############
        self.test_get_partition_metadata_record_not_found() # Confirm record doesn't already exist
        sk, rec, response = self.__partition_test_setup(1, 'test_store', 1.1, 2.2, 3)
        expected_result = {
            'message' : 'Element updated successfully.',
            'id' : response['id'],
            'statusCode' : 'OK'
        }
        ########
        # Test #
        ########
        rec['id'] = response['id']
        rec['count'] = 5
        actual_result = metadata_service_api.update_partition_metadata_record(rec['id'], rec)
        self.assertEqual(actual_result, expected_result)
        # Check update worked
        rec_updated = metadata_service_api.get_partition_metadata_record(*(sk + (1, 'test_store')))
        self.assertEqual(rec_updated, rec)

    def test_delete_partition_metadata_records(self):
        ##############
        # Test Setup #
        ##############
        # Filtered - different subsite
        _, _, response1 = self.__partition_test_setup(1, 'test_store', 1.1, 2.2, 3, subsite_suffix='0')
        # Filtered - different node
        _, _, response2 = self.__partition_test_setup(1, 'test_store', 1.1, 2.2, 3, node_suffix='0')
        # Filtered - different sensor
        _, _, response3 = self.__partition_test_setup(1, 'test_store', 1.1, 2.2, 3, sensor_suffix='0')

        # 3 good ones
        _, _, response4 = self.__partition_test_setup(1, 'test_store1', 1.1, 2.2, 3, method_suffix='0', stream_suffix='2')
        _, _, response5 = self.__partition_test_setup(2, 'test_store2', 2.2, 3.3, 4, method_suffix='1', stream_suffix='1')
        _, _, response6 = self.__partition_test_setup(3, 'test_store3', 3.3, 4.4, 5, method_suffix='2', stream_suffix='0')

        ref_des = self.__build_knockoff_stream_key()[0:3]
        expected_result = {
            'message' : 'Successfully deleted 3 records.',
            'id' : None,
            'statusCode' : 'OK'
        }
        ########
        # Test #
        ########
        actual_result = metadata_service_api.delete_partition_metadata_records(*ref_des)
        self.assertEqual(actual_result, expected_result)

        def check_was_not_deleted(id):
            # Verify this doesn't throw a MetadataServiceException
            url = '/'.join((PARTITION_METADATA_SERVICE_URL, str(id)))
            response = metadata_service_api._MetadataServiceAPI__get_json(url)

        def check_was_deleted(id):
            with self.assertRaises(MetadataServiceException) as cm:
                url = '/'.join((PARTITION_METADATA_SERVICE_URL, str(id)))
                response = metadata_service_api._MetadataServiceAPI__get_json(url)
            self.assertEqual(cm.exception.status_code, requests.codes.not_found)

        check_was_not_deleted(response1['id'])
        check_was_not_deleted(response2['id'])
        check_was_not_deleted(response3['id'])

        check_was_deleted(response4['id'])
        check_was_deleted(response5['id'])
        check_was_deleted(response6['id'])
        ###########
        # Cleanup #
        ###########
        self.partition_metadata_responses.remove(response4)
        self.partition_metadata_responses.remove(response5)
        self.partition_metadata_responses.remove(response6)

