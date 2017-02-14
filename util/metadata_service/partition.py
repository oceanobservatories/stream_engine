import engine
import logging
from collections import namedtuple
from operator import itemgetter
from util.common import log_timing
from util.location_metadata import LocationMetadata
from util.metadata_service import metadata_service_api

SAN_LOCATION_NAME = 'san'

CASS_LOCATION_NAME = 'cass'

_log = logging.getLogger(__name__)

_RecordInfo = namedtuple('_RecordInfo', ['bin', 'store', 'count', 'first', 'last'])


def _get_first_possible_bin(t, stream):
    # TODO: Do something with 'stream' parameter.
    return long(t - (engine.app.config['MAX_BIN_SIZE_MIN'] * 60))


def _time_in_bin_units(t, stream):
    # TODO: Do something with 'stream' parameter.
    return long(t)


@log_timing(_log)
def _query_partition_metadata_before(stream_key, time_start):
    """
    Return the last 4 bins before the the given start time.  Need to return 4 to account for some possibilities:
        Data is present in both SAN and CASS. Need 2 bins to choose location
        Data in current bin is all after the start of the time range:
            Need to check to make sure start time is before current time so need bins for fallback.
    :param stream_key:
    :param time_start:
    :return: Return a list of named tuples which contain the following information about metadata bins
    [
        [
            bin : The bin number
            store : cass or san for location
            count : Number of data points
            first : first ntp time of data in bin
            last : last ntp time of data in the bin
        ],
    ]
    """
    start_bin = _time_in_bin_units(time_start, stream_key.stream_name)
    partition_metadata_record_list = metadata_service_api.get_partition_metadata_records(*stream_key.as_tuple())
    partition_metadata_record_list.sort(key=itemgetter('bin'), reverse=True)
    result = []
    for rec in partition_metadata_record_list:
        if rec['bin'] <= start_bin:
            result.append(_RecordInfo(rec['bin'], rec['store'], rec['count'], rec['first'], rec['last']))
        if len(result) == 4:
            break
    return result


@log_timing(_log)
def _query_partition_metadata(stream_key, time_range):
    """
    Get the metadata entries for partitions that contain data within the time range.
    :param stream_key:
    :param time_range:
    :return: Return a list of named tuples which contain the following information about metadata bins
    [
        [
            bin : The bin number
            store : cass or san for location
            count : Number of data points
            first : first ntp time of data in bin
            last : last ntp time of data in the bin
        ],
    ]
    """
    start_bin = _get_first_possible_bin(time_range.start, stream_key.stream_name)
    end_bin = _time_in_bin_units(time_range.stop, stream_key.stream_name)
    partition_metadata_record_list = metadata_service_api.get_partition_metadata_records(*stream_key.as_tuple())
    result = []
    for rec in partition_metadata_record_list:
        if rec['bin'] >= start_bin and rec['bin'] <= end_bin:
            result.append(_RecordInfo(rec['bin'], rec['store'], rec['count'], rec['first'], rec['last']))
    return result


@log_timing(_log)
def get_location_metadata_by_store(stream_key, time_range, filter_store):
    """
    Get the bins, counts, times, and total data contained in PostgreSQL for a streamkey in the given time range
    :param stream_key: stream-key
    :param time_range: time range to search
    :return: Returns a LocationMetadata object.
    """
    results = _query_partition_metadata(stream_key, time_range)
    bins = {}
    for bin, store, count, first, last in results:
        if store == filter_store:
            bins[bin] = (count, first, last)
    return LocationMetadata(bins)


@log_timing(_log)
def get_first_before_metadata(stream_key, start_time):
    """
    Return metadata information for the first bin before the time range
        Cass metadata, San metadata, messages
    :param stream_key:
    :param start_time:
    :return:
    """
    res = _query_partition_metadata_before(stream_key, start_time)
    # filter to ensure start time < time_range_start
    res = filter(lambda x: x.first <= start_time, res)
    if not res:
        return {}
    first_bin = res[0].bin
    res = filter(lambda x: x.bin == first_bin, res)
    if len(res) == 1:
        to_use = res[0]
    else:
        # Check same size
        if res[0].count == res[1].count:
            # take the choosen one
            res = filter(lambda x: x.store == engine.app.config['PREFERRED_DATA_LOCATION'], res)
            to_use = res[0]
        # other otherwise take the larger of the two
        else:
            if res[0].count < res[1].count:
                to_use = res[1]
            else:
                to_use = res[0]
    return {to_use.store: LocationMetadata({to_use.bin: (to_use.count, to_use.first, to_use.last)})}


@log_timing(_log)
def get_location_metadata(stream_key, time_range):
    results = _query_partition_metadata(stream_key, time_range)
    cass_bins = {}
    san_bins = {}
    messages = []
    for bin, store, count, first, last in results:
        # Check to see if the data in the bin is within the time range.
        if time_range.start <= last and time_range.stop >= first:
            if store == CASS_LOCATION_NAME:
                cass_bins[bin] = (count, first, last)
            if store == SAN_LOCATION_NAME:
                san_bins[bin] = (count, first, last)
    # Define some aliases
    if engine.app.config['PREFERRED_DATA_LOCATION'] == SAN_LOCATION_NAME:
        preferred_bins = san_bins
        other_bins = cass_bins
    else:
        preferred_bins = cass_bins
        other_bins = san_bins
    # Handle data in both stores
    for key in preferred_bins.keys():
        if key in other_bins:
            cass_count, _, _ = cass_bins[key]
            san_count, _, _ = san_bins[key]
            if cass_count != san_count:
                message = "Metadata count does not match for bin {0} - SAN: {1} CASS: {2}.".format(
                    key, san_count, cass_count
                )
                _log.warn(message)
                messages.append(message + " Took location with highest data count.")
                if cass_count > san_count:
                    san_bins.pop(key)
                else:
                    cass_bins.pop(key)
            else:
                other_bins.pop(key)
    return LocationMetadata(cass_bins), LocationMetadata(san_bins), messages


def get_particle_count(stream_key, time_range):
    """
    :param stream_key:
    :param time_range:
    :return:  number of particles in time_range for stream_key
    """
    results = _query_partition_metadata(stream_key, time_range)
    particles = 0
    for row in results:
        if row.first < time_range.stop and row.last > time_range.start:
            particles += row.count

    return particles
