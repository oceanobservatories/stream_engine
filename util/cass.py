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

log = logging.getLogger(__name__)

l0_stream_columns = ['time', 'id', 'driver_class', 'driver_host', 'driver_module', 'driver_version', 'event_json']
ProvTuple = namedtuple('provenance_tuple', ['subsite', 'sensor', 'node', 'method', 'deployment', 'id', 'file_name', 'parser_name', 'parser_version'])
StreamProvTuple = namedtuple('stream_provenance_tuple', l0_stream_columns)

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
                compression=True)

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

    @log_timing
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


@log_timing
@cassandra_session
def get_distinct_sensors(session=None, prepared=None, query_consistency=None):
    rows = session.execute(prepared.get(DISTINCT_PS))
    return rows


@log_timing
@cassandra_session
def get_streams(subsite, node, sensor, method, session=None, prepared=None, query_consistency=None):
    rows = session.execute(prepared[METADATA_FOR_REFDES_PS], (subsite, node, sensor, method))
    return [row[0] for row in rows]  # return streams with count>0


@log_timing
@cassandra_session
def get_query_columns(stream_key, session=None, prepared=None, query_consistency=None):
    # grab the column names from our metadata
    cols = global_cassandra_state['cluster'].metadata.keyspaces[engine.app.config['CASSANDRA_KEYSPACE']]. \
        tables[stream_key.stream.name].columns.keys()
    cols = map(_clean_column_name, cols)
    unneeded = ['subsite', 'node', 'sensor', 'method']
    cols = [c for c in cols if c not in unneeded]
    return cols


@log_timing
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
    start_bin=  time_to_bin(time_range.start)
    end_bin = time_to_bin(time_range.stop)
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
    return LocationMetadata(sorted(bins.keys()), bins, total)

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
    return LocationMetadata(sorted(bins.keys()), bins, total)

def get_location_metadata(stream_key, time_range):
    results = query_partition_metadata(stream_key, time_range)
    cass_bins = {}
    san_bins = {}
    cass_total = 0
    san_total = 0
    messages = []
    for data_bin, location, count, first, last in results:
        # Check to see if the data in the bin is within the time range.
        if time_range.start < last and time_range.stop > first:
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
    cass_metadata = LocationMetadata(sorted(cass_bins.keys()), cass_bins, cass_total)
    san_metadata = LocationMetadata(sorted(san_bins.keys()), san_bins, san_total)
    return cass_metadata, san_metadata, messages

class LocationMetadata(object):

    def __init__(self, bin_list, bin_info, total):
        self.bin_list = bin_list
        self.bin_information = bin_info
        self.total = total

    def __repr__(self):
        val = 'total: {:d} Bins -> '.format(self.total) + str(self.bin_list)
        return val

    def __str__(self):
        return self.__repr__()


@log_timing
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

@log_timing
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

@log_timing
@cassandra_session
def query_first_before(stream_key, times_and_bins, cols, session=None, prepared=None, query_consistency=None):
    query_name = 'first_before_%s_%s_%s_%s_%s' % (stream_key.stream.name, stream_key.subsite, stream_key.node,
                                               stream_key.sensor, stream_key.method)
    if query_name not in prepared:
        base = "select %s  from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s' and time <= ?" % \
               (', '.join(cols), stream_key.stream.name, stream_key.subsite, stream_key.node,
                stream_key.sensor, stream_key.method)
        query = session.prepare(base + 'ORDER BY method DESC, time DESC LIMIT 1')
        query.consistency_level = query_consistency
        prepared[query_name] = query
    result = []
    query = prepared[query_name]
    for success, rows in execute_concurrent_with_args(session, query, times_and_bins, concurrency=50):
        if success:
            result.extend(list(rows))
    return result

@log_timing
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


@log_timing
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
            base % ('time', time_to_bin(start)) + ' and time<%s order by method desc limit 1', (start,),
            consistency_level=query_consistency)
        first = session.execute(first_statment)
        second_statement = SimpleStatement(
            base % ('time', time_to_bin(stop)) + ' and time>%s limit 1', (stop,),
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

    times = [(b, start, stop) for b in xrange(time_to_bin(start), time_to_bin(stop) + 1)]
    return cols, ConcurrentBatchFuture(stream_key, cols, times)


@log_timing
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
    last_prov = prov_results[-1]
    prov[times >= last_prov.time] = str(last_prov.id)
    return prov, prov_dict

@log_timing
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
        location_metadata = get_cass_location_metadata(stream_key, time_range)

    # Fetch it all if it's gonna be close to the same size
    if location_metadata.total < num_points * 2:
        log.info("CASS: Total points (%d) returned is less than twice the requested (%d).  Returning all points.", location_metadata.total, num_points)
        _, results = fetch_all_data(stream_key, time_range, location_metadata)
    # We have a small amount of bins with data so we can read them all
    elif len(location_metadata.bin_list) < engine.app.config['UI_FULL_BIN_LIMIT']:
        log.info("CASS: Reading all (%d) bins and then sampling.", len(location_metadata.bin_list))
        _, results = sample_full_bins(stream_key, time_range, num_points, location_metadata.bin_list, cols)
    # We have a lot of bins so just grab the first from each of the bins
    elif len(location_metadata.bin_list) > num_points:
        log.info("CASS: More bins (%d) than requested points (%d). Selecting first particle from %d bins.", len(location_metadata.bin_list), num_points, num_points)
        _, results = sample_n_bins(stream_key, time_range, num_points, location_metadata.bin_list, cols)
    else:
        log.info("CASS: Sampling %d points across %d bins.", num_points, len(location_metadata.bin_list))
        _, results = sample_n_points(stream_key, time_range, num_points, location_metadata.bin_list, location_metadata.bin_information, cols)

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
    return to_xray_dataset(cols, to_return, stream_key)

def sample_full_bins(stream_key, time_range, num_points, metadata_bins, cols=None):
    # Read all the data and do sampling from there.
    # There may need to be de-duplicating done on this method
    _, all_data = fetch_full_bins(stream_key, [(x,time_range.start, time_range.stop) for x in metadata_bins], cols)
    if len(all_data) < num_points * 4:
        results = all_data
    else:
        indexes = numpy.floor(numpy.linspace(0, len(all_data)-1, num_points)).astype(int)
        selected_data = numpy.array(all_data)[indexes].tolist()
        results = selected_data
    return cols, results


def sample_n_bins(stream_key, time_range, num_points, metadata_bins, cols=None):
    results = []
    sb = metadata_bins[0]
    lb = metadata_bins[-1]
    # Get the first data point for all of the bins in the middle
    metadata_bins = metadata_bins[1:-1]
    indexes = numpy.floor(numpy.linspace(0, len(metadata_bins)-1, num_points-2)).astype(int)
    bins_to_use = numpy.array(metadata_bins)[indexes].tolist()
    cols, rows = fetch_first_after_times(stream_key, [(sb, time_range.start)], cols=cols )
    results.extend(rows)

    # Get the first data point
    _, rows = fetch_bin_firsts(stream_key, bins_to_use, cols)
    results.extend(rows)

    # Get the last data point
    _, rows = fetch_first_before_times(stream_key, [(lb, time_range.stop)], cols=cols )
    results.extend(rows)
    return cols, results


def sample_n_points(stream_key, time_range, num_points, metadata_bins, bin_information, cols=None):
    results = []
    # get the first point
    cols, rows = fetch_first_after_times(stream_key, [(metadata_bins[0], time_range.start)], cols=cols )
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
            queries.add((current[0], t))
        else:
            # can we roll over to the next one?
            if len(bin_queue) > 0 and bin_queue[0][1] <= t:
                current = bin_queue.popleft()
                queries.add((current[0], t))
            # otherwise get the last sample from the last bin we had
            else:
                queries.add((current[0], current[2]))
    times = list(queries)
    _, lin_sampled= fetch_first_before_times(stream_key, times, cols)
    results.extend(lin_sampled)
    # get the last point
    _, rows = fetch_first_before_times(stream_key, [(metadata_bins[-1][0], time_range.stop)], cols=cols )
    results.extend(rows)
    # Sort the data
    results = sorted(results, key=lambda dat: dat[1])
    return cols, results

def fetch_bin_firsts(stream_key, bins, cols=None):
    if cols is None:
        cols = get_query_columns(stream_key)
    future = execution_pool.apply_async(query_bin_first, (stream_key, bins, cols))
    rows = future.get()
    return cols, rows

def fetch_first_after_times(stream_key, times_and_bins, cols=None):
    if cols is None:
        cols = get_query_columns(stream_key)
    future = execution_pool.apply_async(query_first_after, (stream_key, times_and_bins, cols))
    rows = future.get()
    return cols, rows

def fetch_first_before_times(stream_key, times_and_bins, cols=None):
    if cols is None:
        cols = get_query_columns(stream_key)
    future = execution_pool.apply_async(query_first_before, (stream_key, times_and_bins, cols))
    rows = future.get()
    return cols, rows

def fetch_full_bins(stream_key, bins_and_limit, cols=None):
    if cols is None:
        cols = get_query_columns(stream_key)
    future = execution_pool.apply_async(query_full_bin, (stream_key, bins_and_limit, cols))
    rows = future.get()
    return cols, rows

# Fetch all records in the time_range by querying for every time bin in the time_range
@log_timing
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

def get_full_cass_dataset(stream_key, time_range, location_metadata=None):
    cols, rows = fetch_all_data(stream_key, time_range, location_metadata)
    return to_xray_dataset(cols, rows, stream_key)


@cassandra_session
@log_timing
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
@log_timing
def insert_dataset(stream_key, dataset, session=None, prepared=None, query_consistency=None ):
    """
    Insert an xray dataset back into CASSANDRA.
    First we check to see if there is data in the bin, if there is we either overwrite and update
    the values or fail and let the user known why
    :param stream_key: Stream that we are updating
    :param dataset: xray dataset we are updating
    :return:
    """
    # It's easier to use pandas. So covert to dataframe
    dataframe = dataset.to_dataframe()
    # All of the bins on SAN data will be the same in the netcdf file take the first
    data_bin = dataframe['bin'].values[0]
    #get the metadata partion
    meta = query_partition_metadata(stream_key, TimeRange(bin_to_time(data_bin),
                                                          bin_to_time(data_bin + 1)))
    bin_meta = None
    for i in meta:
        if i[0] == data_bin and i[1] == CASS_LOCATION_NAME:
            bin_meta = i
            break

    # get the data in the correct format
    cols = get_query_columns(stream_key)
    cols = ['subsite', 'node', 'sensor', 'bin', 'method'] + cols[1:]
    arrays = set([p.name for p in stream_key.stream.parameters if p.parameter_type != FUNCTION and p.is_array])
    dataframe['subsite'] = stream_key.subsite
    dataframe['node'] =stream_key.node
    dataframe['sensor'] = stream_key.sensor
    dataframe['method'] = stream_key.method
    # id and provenance are expected to be UUIDs so convert them to uuids
    dataframe['id'] = dataframe['id'].apply(lambda x: uuid.UUID(x))
    dataframe['provenance'] = dataframe['provenance'].apply(lambda x: uuid.UUID(x))
    for i in arrays:
        dataframe[i] =  dataframe[i].apply(lambda x: msgpack.packb(x))
    dataframe = dataframe[cols]

    # if we don't have metadata for the bin or we want to overwrite the values from cassandra continue
    count = 0
    if bin_meta is None or engine.app.config['SAN_CASS_OVERWRITE']:
        if bin_meta is not None:
            log.warn("Data present in Cassandra bin %s for %s.  Overwriting old and adding new data.", data_bin, stream_key.as_refdes())

        # get the query to insert information
        query_name = 'load_{:s}_{:s}_{:s}_{:s}_{:s}'.format(stream_key.stream_name, stream_key.subsite, stream_key.node, stream_key.sensor, stream_key.method)
        if query_name not in prepared:
            query = 'INSERT INTO {:s} ({:s}) values ({:s})'.format(stream_key.stream.name,', '.join(cols), ', '.join(['?' for _ in cols]) )
            query = session.prepare(query)
            query.consistency_level = query_consistency
            prepared[query_name] = query
        query = prepared[query_name]

        # insert data
        for good, x in execute_concurrent_with_args(session, query, dataframe.values.tolist(), concurrency=50):
            if not good:
                log.warn("Failed to insert a particle into Cassandra bin %d for %s!", data_bin, stream_key.as_refdes())
            else:
                count += 1
    else:
        # If there is already data and we do not want overwriting return an error
        error_message = "Data present in Cassandra bin {:d} for {:s}. Aborting operation!".format(data_bin, stream_key.as_refdes())
        log.error(error_message)
        return error_message

    # Update the metadata
    if bin_meta is None:
        # Create metadata entry for the new bin
        ref_des = stream_key.as_three_part_refdes()
        st = dataframe['time'].min()
        et = dataframe['time'].max()
        meta_query = "INSERT INTO partition_metadata (stream, refdes, method, bin, store, count, first, last) values ('{:s}', '{:s}', '{:s}', {:d}, '{:s}', {:d}, {:f}, {:f})"\
            .format(stream_key.stream.name, ref_des, stream_key.method, data_bin, CASS_LOCATION_NAME, count, st, et)
        meta_query = SimpleStatement(meta_query)
        meta_query.consistency_level = query_consistency
        session.execute(meta_query)
        ret_val = 'Inserted {:d} particles into Cassandra bin {:d} for {:s}.'.format(count, dataframe['bin'].values[0], stream_key.as_refdes())
        log.info(ret_val)
        return ret_val
    else:
        # get the new count, check times, and update metadata
        q = "SELECT COUNT(*) from {:s} WHERE subsite = '{:s}' and node = '{:s}' and sensor = '{:s}' and bin = {:d} and method = '{:s}'".format(
            stream_key.stream.name, stream_key.subsite, stream_key.node, stream_key.sensor, data_bin, stream_key.method)
        q = SimpleStatement(q)
        q.consistency_level = query_consistency
        new_count = session.execute(q)[0][0]
        ref_des = stream_key.as_three_part_refdes()
        st = min(dataframe['time'].min(), bin_meta[3])
        et = max(dataframe['time'].max(), bin_meta[4])
        meta_query = "INSERT INTO partition_metadata (stream, refdes, method, bin, store, count, first, last) values ('{:s}', '{:s}', '{:s}', {:d}, '{:s}', {:d}, {:f}, {:f})" \
            .format(stream_key.stream.name, ref_des, stream_key.method, data_bin, CASS_LOCATION_NAME, new_count, st, et)
        meta_query = SimpleStatement(meta_query)
        meta_query.consistency_level = query_consistency
        session.execute(meta_query)
        ret_val = 'After updating Cassandra bin {:d} for {:s} there are {:d} particles.'.format(data_bin, stream_key.as_refdes(), new_count)
        log.info(ret_val)
        return ret_val

@cassandra_session
@log_timing
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
@log_timing
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


def create_execution_pool():
    global execution_pool
    pool_size = engine.app.config['POOL_SIZE']
    execution_pool = Pool(pool_size, initializer=initialize_worker)

    futures = []
    for i in xrange(pool_size * 2):
        futures.append(execution_pool.apply_async(connect_worker))

    [f.get() for f in futures]


def time_to_bin(t):
    return int(t / (24 * 60 * 60))

def bin_to_time(b):
    return b * 24 * 60 * 60

@cassandra_session
def get_available_time_range(stream_key, session=None, prepared=None, query_consistency=None):
    rows = session.execute(prepared[STREAM_EXISTS_PS], (stream_key.subsite, stream_key.node,
                                                        stream_key.sensor, stream_key.method,
                                                        stream_key.stream.name))
    stream, count, first, last = rows[0]
    return TimeRange(first, last + 1)
