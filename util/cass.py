import uuid
import numpy
import engine
import time
import hashlib

from collections import deque, namedtuple
import logging

from functools import wraps
from multiprocessing.pool import Pool
from multiprocessing import BoundedSemaphore
from threading import Lock, Thread

from cassandra.cluster import Cluster, QueryExhausted, ResponseFuture, PagedResult, _NOT_SET
from cassandra.query import _clean_column_name, tuple_factory, SimpleStatement, BatchStatement
from cassandra.concurrent import execute_concurrent_with_args, execute_concurrent
from cassandra import ConsistencyLevel
from itertools import izip
import msgpack

from util.common import log_timing, TimeRange, FUNCTION, to_xray_dataset

logging.getLogger('cassandra').setLevel(logging.WARNING)
log = logging.getLogger(__name__)

PARTITION_COLUMNS = ['bin', 'store', 'count', 'first', 'last']
Row = namedtuple('Row', PARTITION_COLUMNS)
l0_stream_columns = ['time', 'id', 'driver_class', 'driver_host', 'driver_module', 'driver_version', 'event_json']
ProvTuple = namedtuple('provenance_tuple', ['subsite', 'sensor', 'node', 'method', 'deployment', 'id', 'file_name', 'parser_name', 'parser_version'])
StreamProvTuple = namedtuple('stream_provenance_tuple', l0_stream_columns)

BinInfo = namedtuple('BinInfo', 'bin count first last')

# cassandra database handle
global_cassandra_state = {}
multiprocess_lock = BoundedSemaphore(2)
execution_pool = None

STREAM_EXISTS_PS = 'stream_exists'
METADATA_FOR_REFDES_PS = 'metadata_for_refdes'
DISTINCT_PS = 'distinct'
L0_PROV_POS = 'l0_provenance'
L0_STREAM_ONE_POS = 'l0_stream_1'
L0_STREAM_RANGE_POS = 'l0_stream_range'
PARTITION_INSERT_QUERY = 'partition_insert'

SAN_LOCATION_NAME = 'san'
CASS_LOCATION_NAME = 'cass'

STREAM_EXISTS_RAW = \
    '''
select stream, count, first, last from STREAM_METADATA
where SUBSITE=? and NODE=? and SENSOR=? and METHOD=? and STREAM=?
'''

METADATA_FOR_REFDES_RAW = \
    '''
SELECT stream, count FROM STREAM_METADATA
where SUBSITE=? and NODE=? and SENSOR=? and METHOD=?
'''

DISTINCT_RAW = \
    '''
SELECT DISTINCT subsite, node, sensor FROM stream_metadata
'''

L0_RAW = \
"""select * from dataset_l0_provenance where subsite=? and node=? and sensor=? and method=? and deployment=? and id=?"""

L0_STREAM_ONE_RAW = """SELECT {:s} FROM streaming_l0_provenance WHERE refdes = ? AND method = ? and time <= ? ORDER BY time DESC LIMIT 1""""".format(', '.join(l0_stream_columns))

L0_STREAM_RANGE_RAW  = """SELECT {:s} FROM streaming_l0_provenance WHERE refdes = ? AND method = ? and time >= ? and time <= ?""".format(', '.join(l0_stream_columns))

PARITION_INSERT_RAW = """INSERT INTO partition_metadata (stream, refdes, method, bin, store, count, first, last) values (?, ?, ?, ?, ?, ?, ?, ?)"""

COUNT_QUERY_RAW = """SELECT COUNT(*) from {:s} WHERE subsite = ? and node = ? and sensor = ? and bin = ? and method = ?"""

def get_session():
    """
    Connect to the cassandra cluster and prepare all statements if not already connected.
    Otherwise, return the cached session and statements
    This is necessary to avoid connecting to cassandra prior to forking!
    :return: session and dictionary of prepared statements
    """
    if global_cassandra_state.get('query_consistency') is None:
        with multiprocess_lock:
            consistency =  engine.app.config['CASSANDRA_QUERY_CONSISTENCY']
            log.info("Setting query consistency level to " + str(consistency))
            const_level = ConsistencyLevel.name_to_value.get(consistency)
            if const_level is None:
                log.warn("Unknown Consistency Level " + str(consistency)  + " setting to QUORUM")
                const_level = ConsistencyLevel.QUORUM
            global_cassandra_state['query_consistency'] = const_level

    if global_cassandra_state.get('cluster') is None:
        with multiprocess_lock:
            log.debug('Creating cassandra session')
            global_cassandra_state['cluster'] = Cluster(
                engine.app.config['CASSANDRA_CONTACT_POINTS'],
                control_connection_timeout=engine.app.config['CASSANDRA_CONNECT_TIMEOUT'],
                compression=True,
                protocol_version=3)

    if global_cassandra_state.get('session') is None:
        with multiprocess_lock:
            session = global_cassandra_state['cluster'].connect(engine.app.config['CASSANDRA_KEYSPACE'])
            session.row_factory = tuple_factory
            session.default_timeout = engine.app.config['CASSANDRA_DEFAULT_TIMEOUT']
            global_cassandra_state['session'] = session
            query_consistency = global_cassandra_state.get('query_consistency')
            prep = global_cassandra_state['prepared_statements'] = {}
            prep[STREAM_EXISTS_PS] = session.prepare(STREAM_EXISTS_RAW)
            prep[STREAM_EXISTS_PS].consistency_level = query_consistency
            prep[METADATA_FOR_REFDES_PS] = session.prepare(METADATA_FOR_REFDES_RAW)
            prep[METADATA_FOR_REFDES_PS].consistency_level = query_consistency
            prep[DISTINCT_PS] = session.prepare(DISTINCT_RAW)
            prep[DISTINCT_PS].consistency_level = query_consistency
            prep[L0_PROV_POS] = session.prepare(L0_RAW)
            prep[L0_PROV_POS].consistency_level = query_consistency
            prep[L0_STREAM_ONE_POS] = session.prepare(L0_STREAM_ONE_RAW)
            prep[L0_STREAM_ONE_POS].consistency_level = query_consistency
            prep[L0_STREAM_RANGE_POS] = session.prepare(L0_STREAM_RANGE_RAW)
            prep[L0_STREAM_RANGE_POS].consistency_level = query_consistency

    return global_cassandra_state['session'], global_cassandra_state['prepared_statements'], global_cassandra_state['query_consistency']


def cassandra_session(func):
    """
    Wrap a function to automatically add the session and prepared arguments retrieved from get_session
    :param func:
    :return:
    """
    @wraps(func)
    def inner(*args, **kwargs):
        session, preps, query_consistency = get_session()
        kwargs['session'] = session
        kwargs['prepared'] = preps
        kwargs['query_consistency'] = query_consistency
        return func(*args, **kwargs)

    return inner


class FakeFuture(ResponseFuture):
    """
    Class to utilize the ResponseFuture interface for a query already completed
    """
    def __init__(self, rows):
        self._callback_lock = Lock()
        self._errors = {}
        self._callbacks = []
        self._errbacks = []
        self._final_result = rows


class ConcurrentBatchFuture(ResponseFuture):
    """
    Combines the apply_async(execute_concurrent) pattern with paging

    There are a number of parameters that can be tuned:

        q_per_page -- number of queries to run per page
                      if < 1, then everything will be done
                      in a single page

        q_per_proc -- number of queries to run per process

        c_per_proc -- number of concurrent queries to run per process

        blocking_init -- whether or not to block and fetch results immediately
                         on construction of an instance of this class.
                         q_per_page must also be < 1 for this to happen.

    There are also a number of global parameters that are also related:

        engine.app.config['POOL_SIZE'] -- number of processes in the pool

        engine.app.config['CASSANDRA_FETCH_SIZE'] -- number of rows to fetch
                                                     at a time from the underlying
                                                     connection
    """

    def __init__(self, stream_key, cols, times):
        self.stream_key = stream_key
        self.cols = cols
        self.times = times
        self.q_per_page = 0
        self.q_per_proc = 10
        self.c_per_proc = 50
        self.blocking_init = True
        self._has_more_pages = False
        self._final_result = None
        if self.q_per_page < 1 and self.blocking_init:
            self.result()

    def add_callback(self, fn, *args, **kwargs):
        if self._final_result is not None:
            fn(self._final_result, *args, **kwargs)
        else:
            def thread_internals():
                for i in range(0, len(self.times), self.q_per_page):
                    rows = self._fetch_data_concurrent(self.times[i:i + self.q_per_page])
                    self._has_more_pages = i*self.q_per_page < len(self.times)
                    fn(rows, *args, **kwargs)
            Thread(target=thread_internals).start()

    def add_errback(self, fn, *args, **kwargs):
        pass

    @property
    def has_more_pages(self):
        return self._has_more_pages

    def start_fetching_next_page(self):
        if self._has_more_pages:
            return
        else:
            raise QueryExhausted

    def result(self):
        if self._final_result is None:
            self._final_result = self._fetch_data_concurrent(self.times)
        return self._final_result

    @log_timing(log)
    def _fetch_data_concurrent(self, times):
        futures = []
        for i in xrange(0, len(times), self.q_per_proc):
            args = (self.stream_key, self.cols, times[i:i + self.q_per_proc])
            kwargs = {'concurrency': self.c_per_proc}
            future = execution_pool.apply_async(fetch_concurrent, args, kwargs)
            futures.append(future)

        rows = []
        for future in futures:
            for data in future.get():
                rows.extend(data)

        return rows


@log_timing(log)
@cassandra_session
def get_distinct_sensors(session=None, prepared=None, query_consistency=None):
    rows = session.execute(prepared.get(DISTINCT_PS))
    return rows


@log_timing(log)
@cassandra_session
def get_streams(subsite, node, sensor, method, session=None, prepared=None, query_consistency=None):
    rows = session.execute(prepared[METADATA_FOR_REFDES_PS], (subsite, node, sensor, method))
    return [row[0] for row in rows]  # return streams with count>0


@log_timing(log)
@cassandra_session
def get_query_columns(stream_key, session=None, prepared=None, query_consistency=None):
    # grab the column names from our metadata
    cols = global_cassandra_state['cluster'].metadata.keyspaces[engine.app.config['CASSANDRA_KEYSPACE']]. \
        tables[stream_key.stream.name].columns.keys()
    cols = map(_clean_column_name, cols)
    unneeded = ['subsite', 'node', 'sensor', 'method']
    cols = [c for c in cols if c not in unneeded]
    return cols


@log_timing(log)
@cassandra_session
def query_partition_metadata_before(stream_key, time_start, session=None, prepared=None, query_consistency=None):
    '''
    Return the last 4 bins before the the given time range.  Need to return 4 to account for some possibilites:
        Data is present in both SAN and CASS. Need 2 bins to choose location
        Data in current bin is all after the start of the time range:
            Need to check to make sure start time is before current time so need bins for fallback.
    :param stream_key:
    :param time_range:
    :return: Return a list of named tuples which contain the following information about metadata bins
    [
        [
            bin, The bin number
            store : cass or san for location,
            count : Number of data points
            first : first ntp time of data in bin
            last : last ntp time of data in the bin
        ],
    ]
    '''
    query_name = "bin_meta_first_{:s}_{:s}_{:s}_{:s}_{:s}".format(stream_key.subsite, stream_key.node, stream_key.sensor,
                                                            stream_key.method, stream_key.stream.name)
    start_bin=  time_in_bin_units(time_start, stream_key.stream.name)
    if query_name not in prepared:
        base = (
            "SELECT {:s} FROM partition_metadata WHERE stream = '{:s}' AND  refdes = '{:s}' AND method = '{:s}' AND bin <= ? ORDER BY method DESC, bin DESC LIMIT 4").format(
            ','.join(PARTITION_COLUMNS), stream_key.stream.name, '{:s}-{:s}-{:s}'.format(stream_key.subsite, stream_key.node, stream_key.sensor),
            stream_key.method)
        query = session.prepare(base)
        query.consistency_level = query_consistency
        prepared[query_name] = query
    query = prepared[query_name]
    res = session.execute(query, [start_bin])
    return [Row(*row) for row in res]

@log_timing(log)
@cassandra_session
def query_partition_metadata(stream_key, time_range, session=None, prepared=None, query_consistency=None):
    '''
    Get the metadata entries for partitions that contain data within the time range.
    :param stream_key:
    :param time_range:
    :return: Return a list of lists which contain the following information about metadata bins
    [
        [
            bin #,
            store : cass or san for location,
            count : Number of data points
            first : first ntp time of data in bin
            last : last ntp time of data in the bin
        ],
    ]
    '''
    query_name = "bin_meta_{:s}_{:s}_{:s}_{:s}_{:s}".format(stream_key.subsite, stream_key.node, stream_key.sensor,
                                                            stream_key.method, stream_key.stream.name)
    start_bin=  get_first_possible_bin(time_range.start, stream_key.stream.name)
    end_bin = time_in_bin_units(time_range.stop, stream_key.stream.name)
    if query_name not in prepared:
        base = (
            "SELECT bin, store, count, first, last FROM partition_metadata WHERE stream = '{:s}' AND  refdes = '{:s}' AND method = '{:s}' AND bin >= ? and bin <= ?").format(
            stream_key.stream.name, stream_key.as_three_part_refdes(),
            stream_key.method)
        query = session.prepare(base)
        query.consistency_level = query_consistency
        prepared[query_name] = query
    query = prepared[query_name]
    return session.execute(query, [start_bin, end_bin])

@log_timing(log)
def get_cass_location_metadata(stream_key, time_range):
    """
    Get the bins, counts, times, and total data contained in cassandra for a streamkey in the given time range
    :param stream_key: stream-key
    :param time_range: time range to search
    :return: Returns a 3-tuple.
            First entry is the sorted list of bins that contain data in cassandra.
            Second entry is a dictonary of each bin pointing to the count, start, and stop times  in a tuple.
            Third entry is the total data contained in cassandra for the time range.
    """
    results = query_partition_metadata(stream_key, time_range)
    bins = {}
    total = 0
    for data_bin, location, count, first, last in results:
        if location == 'cass':
            total = total + count
            bins[data_bin] =  (count, first, last)
    return LocationMetadata(bins)

@log_timing(log)
def get_san_location_metadata(stream_key, time_range):
    """
    Get the bins, counts, times, and total data contained in cassandra for a streamkey in the given time range
    :param stream_key: stream-key
    :param time_range: time range to search
    :return: Returns a 3-tuple.
            First entry is the sorted list of bins that contain data in cassandra.
            Second entry is a dictonary of each bin pointing to the count, start, and stop times  in a tuple.
            Third entry is the total data contained in cassandra for the time range.
    """
    results = query_partition_metadata(stream_key, time_range)
    bins = {}
    total = 0
    for data_bin, location, count, first, last in results:
        if location == SAN_LOCATION_NAME:
            total = total + count
            bins[data_bin] =  (count, first, last)
    return LocationMetadata(bins)

@log_timing(log)
def get_cass_lookback_dataset(stream_key, start_time, data_bin, deployments):
    # try to fetch the first n times to ensure we get a deployment value in there.
    cols, rows = fetch_with_func(query_n_before, stream_key, [(data_bin, start_time,engine.app.config['LOOKBACK_QUERY_LIMIT'] )])
    needed = set(deployments)
    dep_idx = cols.index('deployment')
    ret_rows = []
    for r in rows:
        if r[dep_idx ] in needed:
            ret_rows.append(r)
            needed.remove(r[dep_idx])
    return to_xray_dataset(cols, ret_rows, stream_key)

@log_timing(log)
def get_first_before_metadata(stream_key, start_time):
    """
    Return metadata information for the first bin before the time range
     Cass metadata, San metadata, messages

    :param stream_key:
    :param time_range:
    :return:
    """
    res = query_partition_metadata_before(stream_key, start_time)
    #filter to ensure start time < time_range_start
    res = filter(lambda x: x.first <= start_time, res)
    if len(res) == 0:
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
        #other otherwise take the larger of the two
        else:
            if res[0].count < res[1].count:
                to_use = res[1]
            else:
                to_use =res[0]
    return {to_use.store : LocationMetadata({to_use.bin: (to_use.count, to_use.first, to_use.last)})}

@log_timing(log)
def get_location_metadata(stream_key, time_range):
    results = query_partition_metadata(stream_key, time_range)
    cass_bins = {}
    san_bins = {}
    cass_total = 0
    san_total = 0
    messages = []
    for data_bin, location, count, first, last in results:
        # Check to see if the data in the bin is within the time range.
        if time_range.start <= last and time_range.stop >= first:
            if location == CASS_LOCATION_NAME:
                cass_total = cass_total + count
                cass_bins[data_bin] =  (count, first, last)
            if location == SAN_LOCATION_NAME:
                san_total = san_total + count
                san_bins[data_bin] =  (count, first, last)
    if engine.app.config['PREFERRED_DATA_LOCATION'] == SAN_LOCATION_NAME:
        for key in san_bins.keys():
            if key in cass_bins:
                cass_count, _, _ = cass_bins[key]
                san_count, _, _ = san_bins[key]
                if cass_count != san_count:
                    log.warn("Metadata count does not match for bin %d - SAN: %d  CASS: %d", key, san_count, cass_count)
                    messages.append(
                        "Metadata count does not match for bin {:d} - SAN: {:d}  CASS: {:d} Took location with highest data count.".format(
                            key, san_count, cass_count))
                    if cass_count > san_count:
                        san_bins.pop(key)
                        san_total -= san_count
                    else:
                        cass_bins.pop(key)
                        cass_total -= cass_count
                else:
                    cass_bins.pop(key)
                    cass_total -= cass_count
    else:
        for key in cass_bins.keys():
            if key in san_bins:
                cass_count, _, _ = cass_bins[key]
                san_count, _, _ = san_bins[key]
                if cass_count != san_count:
                    log.warn("Metadata count does not match for bin %d - SAN: %d  CASS: %d", key, san_count, cass_count)
                    messages.append(
                        "Metadata count does not match for bin {:d} - SAN: {:d}  CASS: {:d} Took location with highest data count.".format(
                            key, san_count, cass_count))
                    if cass_count > san_count:
                        san_bins.pop(key)
                        san_total -= san_count
                    else:
                        cass_bins.pop(key)
                        cass_total -= cass_count
                else:
                    san_bins.pop(key)
                    san_total -= san_count
    cass_metadata = LocationMetadata(cass_bins)
    san_metadata = LocationMetadata(san_bins)
    return cass_metadata, san_metadata, messages

class LocationMetadata(object):

    def __init__(self,bin_dict):
        # bin, count, first, last
        values = [BinInfo(*((i,) + bin_dict[i])) for i in bin_dict]
        # sort by start time
        values = sorted(values, key=lambda x: x.first)
        firsts = [b.first for b in values]
        lasts = [b.last for b in values]
        bin_list = [b.bin for b in values]
        counts = [b.count for b in values]
        if len(bin_list) > 0:
            self.total = sum(counts)
            self.start_time = min(firsts)
            self.end_time = max(lasts)
        else:
            self.total = 0
            self.start_time = 0
            self.end_time = 0
        self.bin_list = bin_list
        self.bin_information = bin_dict

    def __repr__(self):
        val = 'total: {:d} Bins -> '.format(self.total) + str(self.bin_list)
        return val

    def __str__(self):
        return self.__repr__()

    def secs(self):
        return self.end_time - self.start_time

    def particle_rate(self):
        return float(self.total) / self.secs()

@log_timing(log)
@cassandra_session
def query_bin_first(stream_key, bins, cols=None,  session=None, prepared=None, query_consistency=None):
    query_name = 'bin_first_%s_%s_%s_%s_%s' % (stream_key.stream.name, stream_key.subsite, stream_key.node,
                                               stream_key.sensor, stream_key.method)
    # attempt to find one data point beyond the requested start/stop times
    if query_name not in prepared:
        base = "select %s  from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s'" % \
                   (', '.join(cols), stream_key.stream.name, stream_key.subsite, stream_key.node,
                    stream_key.sensor, stream_key.method)
        query = session.prepare(base + 'ORDER BY method, time LIMIT 1')
        query.consistency_level = query_consistency
        prepared[query_name] = query
    query = prepared[query_name]
    result = []
    # prepare the arugments for cassandra. Each need to be in their own list
    bins = [[x] for x in bins]
    for success, rows in execute_concurrent_with_args(session, query, bins,concurrency=50):
        if success:
            result.extend(list(rows))
    return result

@log_timing(log)
@cassandra_session
def query_first_after(stream_key, times_and_bins, cols, session=None, prepared=None, query_consistency=None):
    query_name = 'first_after_%s_%s_%s_%s_%s' % (stream_key.stream.name, stream_key.subsite, stream_key.node,
                                               stream_key.sensor, stream_key.method)
    if query_name not in prepared:
        base = "select %s  from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s' and time >= ?" % \
               (', '.join(cols), stream_key.stream.name, stream_key.subsite, stream_key.node,
                stream_key.sensor, stream_key.method)
        query = session.prepare(base + 'ORDER BY method ASC, time ASC LIMIT 1')
        query.consistency_level = query_consistency
        prepared[query_name] = query
    result = []
    query = prepared[query_name]
    for success, rows in execute_concurrent_with_args(session, query, times_and_bins, concurrency=50):
        if success:
            result.extend(list(rows))
    return result

@log_timing(log)
@cassandra_session
def query_n_before(stream_key, query_arguments, cols, session=None, prepared=None, query_consistency=None):
    query_name = 'first_before_%s_%s_%s_%s_%s' % (stream_key.stream.name, stream_key.subsite, stream_key.node,
                                               stream_key.sensor, stream_key.method)
    if query_name not in prepared:
        base = "select %s  from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s' and time <= ?" % \
               (', '.join(cols), stream_key.stream.name, stream_key.subsite, stream_key.node,
                stream_key.sensor, stream_key.method)
        query = session.prepare(base + 'ORDER BY method DESC, time DESC LIMIT ?')
        query.consistency_level = query_consistency
        prepared[query_name] = query
    result = []
    query = prepared[query_name]
    for success, rows in execute_concurrent_with_args(session, query, query_arguments, concurrency=50):
        if success:
            result.extend(list(rows))
    return result

@log_timing(log)
@cassandra_session
def query_full_bin(stream_key, bins_and_limit, cols, session=None, prepared=None, query_consistency=None):
    query_name = 'full_bin_%s_%s_%s_%s_%s' % (stream_key.stream.name, stream_key.subsite, stream_key.node,
                                                  stream_key.sensor, stream_key.method)
    if query_name not in prepared:
        base = "select %s  from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s' and time >= ? and time <= ?" % \
               (', '.join(cols), stream_key.stream.name, stream_key.subsite, stream_key.node,
                stream_key.sensor, stream_key.method)
        query = session.prepare(base)
        query.consistency_level = query_consistency
        prepared[query_name] = query
    result = []
    query = prepared[query_name]
    for success, rows in execute_concurrent_with_args(session, query, bins_and_limit, concurrency=50):
        if success:
            result.extend(list(rows))
    return result


@log_timing(log)
@cassandra_session
def fetch_data_sync(stream_key, time_range, strict_range=False, session=None, prepared=None, query_consistency=None):
    cols = get_query_columns(stream_key)

    # attempt to find one data point beyond the requested start/stop times
    start = time_range.start
    stop = time_range.stop
    base = "select %%s from %s where subsite='%s' and node='%s' and sensor='%s' and bin=%%s and method='%s'" % \
           (stream_key.stream.name, stream_key.subsite, stream_key.node, stream_key.sensor, stream_key.method)

    # attempt to find one data point beyond the requested start/stop times
    if not strict_range:
        first_statment = SimpleStatement(
            base % ('time', time_to_bin(start, stream_key.stream.name)) + ' and time<%s order by method desc limit 1', (start,),
            consistency_level=query_consistency)
        first = session.execute(first_statment)
        second_statement = SimpleStatement(
            base % ('time', time_to_bin(stop, stream_key.stream.name)) + ' and time>%s limit 1', (stop,),
            consistency_level=query_consistency
        )
        last = session.execute(second_statement)
        if first:
            start = first[0][0]
        if last:
            stop = last[0][0]
        # search "close enough" because floats are not exact
        start -= 0.005
        stop += 0.005

    times = [(b, start, stop) for b in xrange(time_to_bin(start, stream_key.stream.name), time_to_bin(stop, stream_key.stream.name) + 1)]
    return cols, ConcurrentBatchFuture(stream_key, cols, times)


@log_timing(log)
@cassandra_session
def fetch_concurrent(stream_key, cols, times, concurrency=50, session=None, prepared=None, query_consistency=None):
    query_name = 'fetch_data_%s_%s_%s_%s_%s' % (stream_key.stream.name, stream_key.subsite,
                                                stream_key.node, stream_key.sensor, stream_key.method)
    if query_name not in prepared:
        base = "select %%s from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s'" % \
               (stream_key.stream.name, stream_key.subsite, stream_key.node, stream_key.sensor, stream_key.method)
        query = session.prepare(base % ','.join(cols) + ' and time>=? and time<=?')
        query.consistency_level = query_consistency
        query.fetch_size = engine.app.config['CASSANDRA_FETCH_SIZE']
        prepared[query_name] = query
    query = prepared[query_name]
    results = execute_concurrent_with_args(session, query, times, concurrency=concurrency)
    results = [list(r[1]) if type(r[1]) == PagedResult else r[1] for r in results if r[0]]
    return results

@log_timing(log)
@cassandra_session
def fetch_l0_provenance(stream_key, provenance_values, deployment, session=None, prepared=None,
                        query_consistency=None):
    """
    Fetch the l0_provenance entry for the passed information.
    All of the neccessary infromation should be stored as a tuple in the
    provenance metadata store.
    """
    # UUIDs are cast to strings so remove all 'None' values
    prov_ids = list({x for x in provenance_values if x != 'None'})
    provenance_arguments = [(stream_key.subsite, stream_key.node, stream_key.sensor, stream_key.method, deployment, uuid.UUID(prov_id)) for prov_id in prov_ids]
    query = prepared[L0_PROV_POS]
    results = execute_concurrent_with_args(session, query, provenance_arguments)
    records = [ProvTuple(*rows[0]) for success, rows in results if success and len(rows) > 0]
    if len(provenance_arguments) != len(records):
        log.warn("Could not find %d provenance entries", len(provenance_arguments) - len(records))
    prov_dict = {str(row.id) : {'file_name' : row.file_name, 'parser_name' : row.parser_name, 'parser_version' : row.parser_version} for row in records}
    return prov_dict

@log_timing(log)
@cassandra_session
def get_streaming_provenance(stream_key, times, session=None, prepared=None, query_consistency=None):
    # Get the first entry before the current
    q1 = prepared[L0_STREAM_ONE_POS]
    q2 = prepared[L0_STREAM_RANGE_POS]
    args = [stream_key.as_three_part_refdes(), stream_key.method, times[0]]
    res = session.execute(q1, args)
    # get the provenance results within the time range
    args.append(times[-1])
    res.extend(session.execute(q2, args))
    prov_results = []
    prov_dict = {}
    # create tuples for all of the objects and instert time values into the provenance
    for r in res:
        r = StreamProvTuple(*r)
        if r.id not in prov_dict:
            prov_results.append(r)
            prov_dict[str(r.id)] = {name : getattr(r, name) for name in l0_stream_columns if name != 'id'}
            # change the UUID to a string to prevent error on output
    prov = numpy.array(['None'] * len(times), dtype=object)
    for sp, ep in zip(prov_results[:-1], prov_results[1:]):
        prov[numpy.logical_and(times >= sp.time, times  <= ep.time)] = str(sp.id)
    if len(prov_results) > 0:
        last_prov = prov_results[-1]
        prov[times >= last_prov.time] = str(last_prov.id)
    return prov, prov_dict

@log_timing(log)
def fetch_nth_data(stream_key, time_range, num_points=1000, location_metadata=None):
    """
    Given a time range, generate evenly spaced times over the specified interval. Fetch a single
    result from either side of each point in time.
    :param stream_key:
    :param time_range:
    :param num_points:
    :return:
    """
    cols = get_query_columns(stream_key)

    if location_metadata is None:
        location_metadata, _, _ = get_location_metadata(stream_key, time_range)

    estimated_rate = location_metadata.particle_rate()
    estimated_particles = int(estimated_rate * time_range.secs())
    data_ratio = estimated_particles / num_points
    log.info("CASS: Estimated total number of points to be %d based on calculated mean rate of %f particles/s",
             estimated_particles, estimated_rate)
    # Fetch it all if it's gonna be close to the same size
    if data_ratio < engine.app.config['UI_FULL_RETURN_RATIO']:
        log.info("CASS: Estimated points (%d) / the requested  number (%d) is less than ratio %f.  Returning all points.",
                 estimated_particles, num_points, engine.app.config['UI_FULL_RETURN_RATIO'])
        _, results = fetch_all_data(stream_key, time_range, location_metadata)
    # We have a small amount of bins with data so we can read them all
    elif estimated_particles < engine.app.config['UI_FULL_SAMPLE_LIMIT'] \
            and data_ratio < engine.app.config['UI_FULL_SAMPLE_RATIO']:
        log.info("CASS: Reading all (%d) bins and then sampling.", len(location_metadata.bin_list))
        _, results = sample_full_bins(stream_key, time_range, num_points, location_metadata.bin_list, cols)
    # We have a lot of bins so just grab the first from each of the bins
    elif len(location_metadata.bin_list) > num_points:
        log.info("CASS: More bins (%d) than requested points (%d). Selecting first particle from %d bins.",
                 len(location_metadata.bin_list), num_points, num_points)
        _, results = sample_n_bins(stream_key, time_range, num_points, location_metadata.bin_list, cols)
    else:
        log.info("CASS: Sampling %d points across %d bins.", num_points, len(location_metadata.bin_list))
        _, results = sample_n_points(stream_key, time_range, num_points, location_metadata.bin_list,
                                     location_metadata.bin_information, cols)

    # dedup data before return values
    size = len(results)
    to_return = []
    uuids = set()
    uuid_index = cols.index('id')
    for row in results:
        my_uuid = row[uuid_index]
        if my_uuid in uuids:
            continue
        uuids.add(my_uuid)
        to_return.append(row)
    log.info("Removed %d duplicates from data", size - len(to_return))
    log.info("Returning %s rows from %s fetch", len(to_return), stream_key.as_refdes())
    return to_xray_dataset(cols, to_return, stream_key)

@log_timing(log)
def sample_full_bins(stream_key, time_range, num_points, metadata_bins, cols=None):
    # Read all the data and do sampling from there.
    # There may need to be de-duplicating done on this method
    _, all_data = fetch_with_func(query_full_bin, stream_key, [(x,time_range.start, time_range.stop) for x in metadata_bins], cols)
    if len(all_data) < num_points * 4:
        results = all_data
    else:
        indexes = numpy.floor(numpy.linspace(0, len(all_data)-1, num_points)).astype(int)
        selected_data = numpy.array(all_data)[indexes].tolist()
        results = selected_data
    return cols, results

@log_timing(log)
def sample_n_bins(stream_key, time_range, num_points, metadata_bins, cols=None):
    results = []
    sb = metadata_bins[0]
    lb = metadata_bins[-1]
    # Get the first data point for all of the bins in the middle
    metadata_bins = metadata_bins[1:-1]
    indexes = numpy.floor(numpy.linspace(0, len(metadata_bins)-1, num_points-2)).astype(int)
    bins_to_use = numpy.array(metadata_bins)[indexes].tolist()
    cols, rows = fetch_with_func(query_first_after, stream_key, [(sb, time_range.start)], cols=cols )
    results.extend(rows)

    # Get the first data point
    _, rows = fetch_with_func(query_bin_first, stream_key, bins_to_use, cols)
    results.extend(rows)

    # Get the last data point
    _, rows = fetch_with_func(query_n_before, stream_key, [(lb, time_range.stop, 1)], cols=cols )
    results.extend(rows)
    return cols, results

@log_timing(log)
def sample_n_points(stream_key, time_range, num_points, metadata_bins, bin_information, cols=None):
    results = []

    # get the first point
    cols, rows = fetch_with_func(query_first_after, stream_key, [(metadata_bins[0], time_range.start)], cols=cols )
    results.extend(rows)

    # create a set to keep track of queries and avoid duplicates
    queries = set()
    # We want to know start and stop time of each bin that has data
    metadata_bins = [(x, bin_information[x][1], bin_information[x][2]) for x in metadata_bins]
    metadata_bins.sort()
    bin_queue = deque(metadata_bins)
    # Create our time array of sampling times
    times = numpy.linspace(time_range.start, time_range.stop, num_points)
    # Get the first bin that has data within the range
    current = bin_queue.popleft()
    for t in times:
        # Are we currently within the range of data in the bin if so add a sampling point
        if current[1] <= t < current[2]:
            queries.add((current[0], t, 1))
        else:
            # can we roll over to the next one?
            if len(bin_queue) > 0 and bin_queue[0][1] <= t:
                current = bin_queue.popleft()
                queries.add((current[0], t, 1))
            # otherwise get the last sample from the last bin we had
            else:
                queries.add((current[0], current[2], 1))
    times = list(queries)
    times.append((metadata_bins[-1][0], time_range.stop, 1))
    _, lin_sampled= fetch_with_func(query_n_before, stream_key, times, cols)
    results.extend(lin_sampled)
    # Sort the data
    results = sorted(results, key=lambda dat: dat[1])
    return cols, results


def fetch_with_func(f, stream_key, args, cols=None):
    if cols is None:
        cols = get_query_columns(stream_key)
    return cols, f(stream_key, args, cols)


@log_timing(log)
@cassandra_session
def fetch_bin(stream_key, time_bin, session=None, prepared=None, query_consistency=None):
    """
    Fetch an entire bin
    """
    cols = get_query_columns(stream_key)

    base = ("select %s from %s where subsite=%%s and node=%%s and sensor=%%s and bin=%%s " + \
            "and method=%%s") % (','.join(cols), stream_key.stream.name)
    query = SimpleStatement(base)
    query.consistency_level = query_consistency
    return cols, list(session.execute(query, (stream_key.subsite,
                                              stream_key.node,
                                              stream_key.sensor,
                                              time_bin,
                                              stream_key.method)))


# Fetch all records in the time_range by querying for every time bin in the time_range
@log_timing(log)
def fetch_all_data(stream_key, time_range, location_metadata=None):

    """
    Given a time range, Fetch all records from the starting hour to ending hour
    :param stream_key:
    :param time_range:
    :param session:
    :param prepared:
    :return:
    """
    if location_metadata is None:
        location_metadata = get_cass_location_metadata(stream_key, time_range)
    cols = get_query_columns(stream_key)

    futures = []
    for bin_num in location_metadata.bin_list:
        futures.append(execution_pool.apply_async(execute_unlimited_query, (stream_key, cols, bin_num, time_range)))

    rows = []
    for future in futures:
        rows.extend(future.get())

    return cols, rows

@log_timing(log)
def get_full_cass_dataset(stream_key, time_range, location_metadata=None):
    cols, rows = fetch_all_data(stream_key, time_range, location_metadata)
    return to_xray_dataset(cols, rows, stream_key)


@cassandra_session
@log_timing(log)
def execute_unlimited_query(stream_key, cols, time_bin, time_range, session=None, prepared=None,
                            query_consistency=None):

    base = ("select %s from %s where subsite=%%s and node=%%s and sensor=%%s and bin=%%s " + \
            "and method=%%s and time>=%%s and time<=%%s") % (','.join(cols), stream_key.stream.name)
    query = SimpleStatement(base)
    query.consistency_level = query_consistency
    return list(session.execute(query, (stream_key.subsite,
                                        stream_key.node,
                                        stream_key.sensor,
                                        time_bin,
                                        stream_key.method,
                                        time_range.start,
                                        time_range.stop)))

@cassandra_session
@log_timing(log)
def insert_dataset(stream_key, dataset, session=None, prepared=None, query_consistency=None ):
    """
    Insert an xray dataset back into CASSANDRA.
    First we check to see if there is data in the bin, if there is we either overwrite and update
    the values or fail and let the user known why
    :param stream_key: Stream that we are updating
    :param dataset: xray dataset we are updating
    :return:
    """
    # All of the bins on SAN data will be the same in the netcdf file take the first
    data_bin = dataset['bin'].values[0]
    data_lists = {}
    size = dataset['index'].size
    #get the metadata partion
    meta = query_partition_metadata(stream_key, TimeRange(bin_to_time(data_bin),
                                                          bin_to_time(data_bin + 1)))
    bin_meta = None
    for i in meta:
        if i[0] == data_bin and i[1] == CASS_LOCATION_NAME:
            bin_meta = i
            break
    if bin_meta is not None and not engine.app.config['SAN_CASS_OVERWRITE']:
        # If there is already data and we do not want overwriting return an error
        error_message = "Data present in Cassandra bin {:d} for {:s}. Aborting operation!".format(data_bin, stream_key.as_refdes())
        log.error(error_message)
        return error_message

    # get the data in the correct format
    cols = get_query_columns(stream_key)
    dynamic_cols = cols[1:]
    key_cols = ['subsite', 'node', 'sensor', 'bin', 'method']
    cols = key_cols + dynamic_cols
    arrays = set([p.name for p in stream_key.stream.parameters if p.parameter_type != FUNCTION and p.is_array])
    data_lists['bin'] = [data_bin] * size
    # id and provenance are expected to be UUIDs so convert them to uuids
    data_lists['id'] = [uuid.UUID(x) for x in  dataset['id'].values]
    data_lists['provenance'] = [uuid.UUID(x) for x in dataset['provenance'].values]
    for i in arrays:
        data_lists[i] =  [msgpack.packb(x) for x in dataset[i].values.tolist()]
    for dc in dynamic_cols:
        # if it is in the dataset and not already in the datalist we need to put it in the list
        if dc in dataset and dc not in data_lists:
            if '_FillValue' in dataset[dc].attrs:
                temp_val = dataset[dc].values.astype(object)
                temp_val[temp_val == dataset[dc].attrs['_FillValue']] = None
                data_lists[dc] = temp_val
            else:
                data_lists[dc] = dataset[dc].values

    # if we don't have metadata for the bin or we want to overwrite the values from cassandra continue
    count = 0
    if bin_meta is not None:
        log.warn("Data present in Cassandra bin %s for %s.  Overwriting old and adding new data.", data_bin, stream_key.as_refdes())

    # get the query to insert information
    query_name = 'load_{:s}_{:s}_{:s}_{:s}_{:s}'.format(stream_key.stream_name, stream_key.subsite, stream_key.node, stream_key.sensor, stream_key.method)
    if query_name not in prepared:
        col_names = ', '.join(cols)
        # Take subsite, node, sensor, ?, and method
        key_str= "'{:s}', '{:s}', '{:s}', ?, '{:s}'".format(stream_key.subsite, stream_key.sensor, stream_key.node, stream_key.method)
        #and the rest as ? for thingskey_cols[:3] +['?'] + key_cols[4:] + ['?' for _ in cols]
        data_str = ', '.join(['?' for _ in dynamic_cols])
        full_str = '{:s}, {:s}'.format(key_str, data_str)
        query = 'INSERT INTO {:s} ({:s}) values ({:s})'.format(stream_key.stream.name, col_names, full_str)
        query = session.prepare(query)
        query.consistency_level = query_consistency
        prepared[query_name] = query
    query = prepared[query_name]
    # make the data list
    to_insert = []
    data_names = ['bin'] + dynamic_cols
    for i in range(size):
        row = [data_lists[col][i] for col in data_names]
        to_insert.append(row)

    # insert data
    fails = 0
    for good, x in execute_concurrent_with_args(session, query, to_insert,  concurrency=50):
        if not good:
            fails += 1
        else:
            count += 1
    if fails > 0:
        log.warn("Failed to insert %s particles into Cassandra bin %d for %s!", fails, data_bin, stream_key.as_refdes())

    if PARTITION_INSERT_QUERY not in prepared:
        partion_insert_query = session.prepare(PARITION_INSERT_RAW)
        partion_insert_query.consistency_level = query_consistency
        prepared[PARTITION_INSERT_QUERY] = partion_insert_query
    partion_insert_query = prepared.get(PARTITION_INSERT_QUERY)
    # Update the metadata
    if bin_meta is None:
        # Create metadata entry for the new bin
        ref_des = stream_key.as_three_part_refdes()
        st = dataset['time'].min()
        et = dataset['time'].max()
        session.execute(partion_insert_query, (stream_key.stream.name, ref_des, stream_key.method, data_bin, CASS_LOCATION_NAME, count, st, et))
        ret_val = 'Inserted {:d} particles into Cassandra bin {:d} for {:s}.'.format(count, dataset['bin'].values[0], stream_key.as_refdes())
        return ret_val
    else:
        query_key = "{:s}_count_bin_particles".format(stream_key.stream.name)
        if query_key not in prepared:
            count_query = session.prepare(COUNT_QUERY_RAW.format(stream_key.stream.name))
            count_query.consistency_level = query_consistency
            prepared[query_key] = count_query
        count_query = prepared.get(query_key)
        # get the new count, check times, and update metadata
        new_count = session.execute(count_query, (stream_key.subsite, stream_key.node, stream_key.sensor, data_bin, stream_key.method))[0][0]
        ref_des = stream_key.as_three_part_refdes()
        st = min(dataset['time'].values.min(), bin_meta[3])
        et = max(dataset['time'].values.max(), bin_meta[4])
        meta_query = "INSERT INTO partition_metadata (stream, refdes, method, bin, store, count, first, last) values ('{:s}', '{:s}', '{:s}', {:d}, '{:s}', {:d}, {:f}, {:f})" \
            .format(stream_key.stream.name, ref_des, stream_key.method, data_bin, CASS_LOCATION_NAME, new_count, st, et)
        meta_query = SimpleStatement(meta_query)
        meta_query.consistency_level = query_consistency
        session.execute(meta_query)
        ret_val = 'After updating Cassandra bin {:d} for {:s} there are {:d} particles.'.format(data_bin, stream_key.as_refdes(), new_count)
        log.info(ret_val)
        return ret_val

@cassandra_session
@log_timing(log)
def fetch_annotations(stream_key, time_range, session=None, prepared=None, query_consistency=None, with_mooring=True):
    #------- Query 1 -------
    # query where annotation effectivity is whithin the query time range
    # or stadles the end points
    select_columns = "subsite, node, sensor, time, time2, parameters, provenance, annotation, method, deployment, id "
    select_clause = "select " + select_columns + "from annotations "
    where_clause = "where subsite=? and node=? and sensor=?"
    time_constraint = " and time>=%s and time<=%s"
    query_base = select_clause + where_clause  + time_constraint
    query_string = query_base % (time_range.start, time_range.stop)

    query1 = session.prepare(query_string)
    query1.consistency_level = query_consistency

    #------- Query 2 --------
    # Where annoation effectivity straddles the entire query time range
    # -- This is necessary because of the way the Cassandra uses the
    #    start-time in the primary key
    time_constraint_wide = " and time<=%s"
    query_base_wide = select_clause + where_clause + time_constraint_wide
    query_string_wide = query_base_wide % (time_range.start)

    query2 = session.prepare(query_string_wide)
    query2.consistency_level = query_consistency

    #----------------------------------------------------------------------
    # Prepare arguments for both query1 and query2
    #----------------------------------------------------------------------
    # [(subsite,node,sensor),(subsite,node,''),(subsite,'','')
    tup1 = (stream_key.subsite,stream_key.node,stream_key.sensor)
    tup2 = (stream_key.subsite,stream_key.node, '')
    tup3 = (stream_key.subsite,'','')
    args = []
    args.append(tup1)
    if with_mooring:
        args.append(tup2)
        args.append(tup3)

    result = []
    # query where annotation effectivity is whithin the query time range
    # or stadles the end points
    for success, rows in execute_concurrent_with_args(session, query1, args, concurrency=3):
        if success:
            result.extend(list(rows))

    temp = []
    for success, rows in execute_concurrent_with_args(session, query2, args, concurrency=3):
        if success:
            temp.extend(list(rows))

    for row in temp:
        time2 = row[4]
        if time_range.stop < time2:
            result.append(row)

    return result


@cassandra_session
@log_timing(log)
def store_qc_results(qc_results_values, pk, particle_ids, particle_bins, particle_deploys, param_name, session=None, strict_range=False, prepared=None, query_consistency=None):
    start_time = time.clock()
    if engine.app.config['QC_RESULTS_STORAGE_SYSTEM'] == 'cass':
        log.info('Storing QC results in Cassandra.')
        insert_results = session.prepare("insert into ooi.qc_results " \
                       "(subsite, node, sensor, bin, deployment, stream, id, parameter, results) " \
                       "values (?, ?, ?, ?, ?, ?, ?, ?, ?)")

        batch = BatchStatement(consistency_level=ConsistencyLevel.name_to_value.get(engine.app.config['CASSANDRA_QUERY_CONSISTENCY']))
        for (qc_results, particle_id, particle_bin, particle_deploy) in izip(qc_results_values, particle_ids, particle_bins, particle_deploys):
            batch.add(insert_results, (pk.get('subsite'), pk.get('node'), pk.get('sensor'),
                                       particle_bin, particle_deploy, pk.get('stream'),
                                       uuid.UUID(particle_id), param_name, str(qc_results)))
        session.execute_async(batch)
        log.info("QC results stored in {} seconds.".format(time.clock() - start_time))
    elif engine.app.config['QC_RESULTS_STORAGE_SYSTEM'] == 'log':
        log.info('Writing QC results to log file.')
        qc_log = logging.getLogger('qc.results')
        qc_log_string = ""
        for (qc_results, particle_id, particle_bin, particle_deploy) in izip(qc_results_values, particle_ids, particle_bins, particle_deploys):
            qc_log_string += "refdes:{0}-{1}-{2}, bin:{3}, stream:{4}, deployment:{5}, id:{6}, parameter:{7}, qc results:{8}\n"\
                .format(pk.get('subsite'), pk.get('node'), pk.get('sensor'), particle_bin,
                        pk.get('stream'), particle_deploy, particle_id, param_name, qc_results)
        qc_log.info(qc_log_string[:-1])
        log.info("QC results stored in {} seconds.".format(time.clock() - start_time))
    else:
        log.info("Configured storage system '{}' not recognized, qc results not stored.".format(engine.app.config['QC_RESULTS_STORAGE_SYSTEM']))

@log_timing(log)
@cassandra_session
def stream_exists(subsite, node, sensor, method, stream, session=None, prepared=None, query_consistency=None):
    ps = prepared.get(STREAM_EXISTS_PS)
    rows = session.execute(ps, (subsite, node, sensor, method, stream))
    return len(rows) == 1


def initialize_worker():
    global global_cassandra_state
    global_cassandra_state = {}


def connect_worker():
    get_session()

@log_timing(log)
def create_execution_pool():
    global execution_pool
    pool_size = engine.app.config['POOL_SIZE']
    execution_pool = Pool(pool_size, initializer=initialize_worker)

    futures = []
    for i in xrange(pool_size * 2):
        futures.append(execution_pool.apply_async(connect_worker))

    [f.get() for f in futures]

def get_pool():
    return execution_pool

def get_first_possible_bin(t, stream):
    t =  t - (engine.app.config['MAX_BIN_SIZE_MIN'] * 60)
    return long(t)

def time_in_bin_units(t, stream):
    return long(t)

def time_to_bin(t, stream):
    bin_size_seconds = 24 * 60 * 60
    return long(t / bin_size_seconds) * bin_size_seconds

def bin_to_time(b):
    return float(b)

@cassandra_session
@log_timing(log)
def get_available_time_range(stream_key, session=None, prepared=None, query_consistency=None):
    rows = session.execute(prepared[STREAM_EXISTS_PS], (stream_key.subsite, stream_key.node,
                                                        stream_key.sensor, stream_key.method,
                                                        stream_key.stream.name))
    stream, count, first, last = rows[0]
    return TimeRange(first, last + 1)
