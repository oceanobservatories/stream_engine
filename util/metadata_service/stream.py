import engine
import logging
from api import get_stream_metadata_records, get_stream_metadata_record
from collections import namedtuple
from util.common import log_timing, TimeRange, timed_cache

_log = logging.getLogger(__name__)

_Data = namedtuple('Data', ['subsite', 'node', 'sensor', 'method', 'stream'])


@log_timing(_log)
def _get_stream_metadata():
    streamMetadataRecordList = get_stream_metadata_records()
    return [_Data(method=rec['method'], stream=rec['stream'], **rec['referenceDesignator']) for rec in streamMetadataRecordList]


@timed_cache(engine.app.config['METADATA_CACHE_SECONDS'])
def build_stream_dictionary():
    d = {}
    for subsite, node, sensor, method, stream in _get_stream_metadata():
        d.setdefault(stream, {}).setdefault(method, {}).setdefault(subsite, {}).setdefault(node, []).append(sensor)
    return d


@log_timing(_log)
def get_available_time_range(stream_key):
    streamMetadataRecord = get_stream_metadata_record(stream_key)
    return TimeRange(streamMetadataRecord['first'], streamMetadataRecord['last'] + 1)

