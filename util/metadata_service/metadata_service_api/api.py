import requests

class MetadataServiceException(Exception):
    def __init__(self, status_code):
        super(MetadataServiceException, self).__init__('Metadata Service request failed with status: {0}'.format(status_code))
        self.status_code = status_code


class MetadataServiceAPI(object):
    '''Class used to communicate with uFrame's Metadata Service API.'''

    def __init__(self, stream_url, partition_url):
        self.__stream_url = stream_url
        self.__partition_url = partition_url

    ##################
    # Common Methods #
    ##################

    @staticmethod
    def __get_json(url):
        response = requests.get(url)
        if (response.status_code == requests.codes.ok):
            return response.json()
        else:
            raise MetadataServiceException(response.status_code)

    @staticmethod
    def __post_json(url, json):
        response = requests.post(url, json=json)
        if (response.status_code == requests.codes.created):
            return response.json()
        else:
            raise MetadataServiceException(response.status_code)

    @staticmethod
    def __put_json(url, json):
        response = requests.put(url, json=json)
        if (response.status_code == requests.codes.ok):
            return response.json()
        else:
            raise MetadataServiceException(response.status_code)

    @staticmethod
    def __delete_json(url):
        response = requests.delete(url)
        if (response.status_code == requests.codes.ok):
            return response.json()
        else:
            raise MetadataServiceException(response.status_code)

    @staticmethod
    def build_stream_metadata_record(subsite, node, sensor, method, stream, first, last, count):
        return {
            '@class' : '.StreamMetadataRecord',
            'referenceDesignator' : {
                'subsite' : subsite,
                'node' : node,
                'sensor' : sensor
            },
            'method' : method,
            'stream' : stream,
            'first' : first,
            'last' : last,
            'count' : count
        }

    @staticmethod
    def build_partition_metadata_record(subsite, node, sensor, method, stream, bin, store, first, last, count):
        return {
            '@class' : '.PartitionMetadataRecord',
            'referenceDesignator' : {
                'subsite' : subsite,
                'node' : node,
                'sensor' : sensor
            },
            'method' : method,
            'stream' : stream,
            'bin' : bin,
            'store' : store,
            'first' : first,
            'last' : last,
            'count' : count
        }

    ##################################
    # Stream Metadata Record Methods #
    ##################################

    def get_stream_metadata_records(self):
        return self.__get_json(self.__stream_url)

    def get_stream_metadata_record(self, subsite, node, sensor, method, stream):
        url = '/'.join((self.__stream_url, 'inv', subsite, node, sensor, method, stream))
        try:
            return self.__get_json(url)
        except MetadataServiceException, e:
            if (e.status_code == requests.codes.not_found):
                return None
            raise e

    def delete_stream_metadata_records(self, subsite, node, sensor):
        url = '/'.join((self.__stream_url, 'inv', subsite, node, sensor))
        return self.__delete_json(url)

    #####################################
    # Partition Metadata Record Methods #
    #####################################

    def get_partition_metadata_records(self, subsite, node, sensor, method=None, stream=None):
        if method and stream:
            url = '/'.join((self.__partition_url, 'inv', subsite, node, sensor, method, stream))
        else:
            url = '/'.join((self.__partition_url, 'inv', subsite, node, sensor))
        return self.__get_json(url)

    def get_partition_metadata_record(self, subsite, node, sensor, method, stream, bin, store):
        url = '/'.join((self.__partition_url, 'inv', subsite, node, sensor, method, stream, str(bin), store))
        try:
            return self.__get_json(url)
        except MetadataServiceException, e:
            if (e.status_code == requests.codes.not_found):
                return None
            raise e

    def create_partition_metadata_record(self, partition_metadata_record):
        return self.__post_json(self.__partition_url, partition_metadata_record)

    def update_partition_metadata_record(self, record_id, partition_metadata_record):
        url = '/'.join((self.__partition_url, str(record_id)))
        return self.__put_json(url, partition_metadata_record)

    def delete_partition_metadata_records(self, subsite, node, sensor):
        url = '/'.join((self.__partition_url, 'inv', subsite, node, sensor))
        return self.__delete_json(url)

