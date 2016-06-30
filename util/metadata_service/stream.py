import engine
import logging
from collections import namedtuple
from util.common import log_timing, TimeRange, timed_cache
from util.metadata_service import metadata_service_api

_log = logging.getLogger(__name__)

_RecordInfo = namedtuple('_RecordInfo', ['subsite', 'node', 'sensor', 'method', 'stream'])


@log_timing(_log)
def _get_stream_metadata():
    stream_metadata_record_list = metadata_service_api.get_stream_metadata_records()
    return [_RecordInfo(method=rec['method'], stream=rec['stream'], **rec['referenceDesignator'])
            for rec in stream_metadata_record_list]


@timed_cache(engine.app.config['METADATA_CACHE_SECONDS'])
def build_stream_dictionary():
    d = {}
    for subsite, node, sensor, method, stream in _get_stream_metadata():
        d.setdefault(stream, {}).setdefault(method, {}).setdefault(subsite, {}).setdefault(node, []).append(sensor)
    return d


@log_timing(_log)
def get_available_time_range(stream_key):
    stream_metadata_record = metadata_service_api.get_stream_metadata_record(*stream_key.as_tuple())
    return TimeRange(stream_metadata_record['first'], stream_metadata_record['last'] + 1)
