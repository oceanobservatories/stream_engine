import engine
import logging
import requests
from util.common import log_timing

_log = logging.getLogger(__name__)

_streamUrl = engine.app.config['STREAM_METADATA_SERVICE_URL']
_partitionUrl = engine.app.config['PARTITION_METADATA_SERVICE_URL']

class MetadataServiceException(Exception):
    def __init__(self, status_code):
        super(MetadataServiceException, self).__init__('Metadata Service request failed with status: {0}'.format(status_code))
        self.status_code = status_code

##################
# Common Methods #
##################

def _get_json(url):
    _log.info("Getting: {0}".format(url))
    response = requests.get(url)
    if (response.status_code == requests.codes.ok):
        return response.json()
    else:
        raise MetadataServiceException(response.status_code)


def _post_json(url, json):
    _log.info("Posting: {0}".format(url))
    response = requests.post(url, json=json)
    if (response.status_code == requests.codes.created):
        return response.json()
    else:
        raise MetadataServiceException(response.status_code)


def _put_json(url, json):
    _log.info("Putting: {0}".format(url))
    response = requests.put(url, json=json)
    if (response.status_code == requests.codes.ok):
        return response.json()
    else:
        raise MetadataServiceException(response.status_code)


def _delete_json(url):
    _log.info("Deleting: {0}".format(url))
    response = requests.delete(url)
    if (response.status_code == requests.codes.ok):
        return response.json()
    else:
        raise MetadataServiceException(response.status_code)


def _build_stream_metadata_record(subsite, node, sensor, method, stream, first, last, count):
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


def _build_partition_metadata_record(subsite, node, sensor, method, stream, bin, store, first, last, count):
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

@log_timing(_log)
def get_stream_metadata_records():
    return _get_json(_streamUrl)


@log_timing(_log)
def get_stream_metadata_record(stream_key):
    # Example: http://127.0.0.1:12571/streamMetadata/inv/{subsite}/{node}/{sensor}/{method}/{stream}
    url = '/'.join((_streamUrl, 'inv') + stream_key.as_tuple())
    try:
        return _get_json(url)
    except MetadataServiceException, e:
        if (e.status_code == requests.codes.not_found):
            return None
        raise e

#####################################
# Partition Metadata Record Methods #
#####################################

@log_timing(_log)
def get_partition_metadata_records(stream_key):
    # Example: http://127.0.0.1:12571/partitionMetadata/inv/{subsite}/{node}/{sensor}/{method}/{stream}
    url = '/'.join((_partitionUrl, 'inv') + stream_key.as_tuple())
    return _get_json(url)


@log_timing(_log)
def get_partition_metadata_record(stream_key, bin, store):
    # Example: http://127.0.0.1:12571/partitionMetadata/inv/{subsite}/{node}/{sensor}/{method}/{stream}/{bin}/{store}
    url = '/'.join((_partitionUrl, 'inv') + stream_key.as_tuple() + (str(bin), store))
    try:
        return _get_json(url)
    except MetadataServiceException, e:
        if (e.status_code == requests.codes.not_found):
            return None
        raise e


@log_timing(_log)
def create_partition_metadata_record(stream_key, bin, store, first, last, count):
    partitionMetadataRecord = _build_partition_metadata_record(*(stream_key.as_tuple() + (bin, store, first, last, count)))
    return _post_json(_partitionUrl, partitionMetadataRecord)


@log_timing(_log)
def update_partition_metadata_record(partitionMetadataRecord):
    # Example: http://127.0.0.1:12571/partitionMetadata/{id}
    url = '/'.join((_partitionUrl, str(partitionMetadataRecord['id'])))
    return _put_json(url, partitionMetadataRecord)

