import engine
import logging
from collections import namedtuple
from util.common import log_timing, TimeRange, timed_cache, MissingStreamMetadataException
from util.metadata_service import metadata_service_api

_log = logging.getLogger(__name__)

_RecordInfo = namedtuple('_RecordInfo', ['subsite', 'node', 'sensor', 'method', 'stream'])


@log_timing(_log)
def _get_stream_metadata_list():
    return metadata_service_api.get_stream_metadata_records()


def _get_stream_metadata():
    return [_RecordInfo(method=rec['method'], stream=rec['stream'], **rec['referenceDesignator'])
            for rec in _get_stream_metadata_list()]


def get_refdes_streams_for_subsite_node(subsite, node):
    return [(Dict['referenceDesignator']['subsite']+'-'+Dict['referenceDesignator']['node'] +
             '-'+Dict['referenceDesignator']['sensor'], Dict['stream']) for Dict in _get_stream_metadata_list()
            if Dict['referenceDesignator']['subsite'] == subsite and Dict['referenceDesignator']['node'] == node]


def get_streams_for_subsite_node_sensor(subsite, node, sensor):
    return [Dict['stream'] for Dict in _get_stream_metadata_list()
            if Dict['referenceDesignator']['subsite'] == subsite and Dict['referenceDesignator']['node'] == node
            and Dict['referenceDesignator']['sensor'] == sensor]


def get_streams_for_refdes(refdes):
    subsite, node, sensor = refdes.split('-', 2)
    return get_streams_for_subsite_node_sensor(subsite, node, sensor)


@timed_cache(engine.app.config['METADATA_CACHE_SECONDS'])
def build_stream_dictionary():
    d = {}
    for subsite, node, sensor, method, stream in _get_stream_metadata():
        d.setdefault(stream, {}).setdefault(method, {}).setdefault(subsite, {}).setdefault(node, []).append(sensor)
    return d


@log_timing(_log)
def get_available_time_range(stream_key):
    stream_metadata_record = metadata_service_api.get_stream_metadata_record(*stream_key.as_tuple())
    if stream_metadata_record is None:
        raise MissingStreamMetadataException('Query returned no results for primary stream')
    return TimeRange(stream_metadata_record['first'], stream_metadata_record['last'] + 1)
