import numpy
import engine
import hashlib

from collections import deque
import logging

from functools import wraps
from multiprocessing.pool import Pool
from multiprocessing import BoundedSemaphore
from threading import Lock, Thread

from cassandra.cluster import Cluster, QueryExhausted, ResponseFuture, PagedResult, _NOT_SET
from cassandra.query import _clean_column_name, tuple_factory, SimpleStatement
from cassandra.concurrent import execute_concurrent_with_args
from cassandra import ConsistencyLevel

from util.common import log_timing, TimeRange

log = logging.getLogger(__name__)


# cassandra database handle
global_cassandra_state = {}
multiprocess_lock = BoundedSemaphore(2)
execution_pool = None

STREAM_EXISTS_PS = 'stream_exists'
METADATA_FOR_REFDES_PS = 'metadata_for_refdes'
DISTINCT_PS = 'distinct'

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
            count : Number of data points
            first : first ntp time of data in bin
            last : last ntp time of data in the bin
        ],
    ]
    '''
    query_name = "hour_meta_{:s}_{:s}_{:s}_{:s}_{:s}".format(stream_key.subsite, stream_key.node, stream_key.sensor,
                                                            stream_key.method, stream_key.stream.name)
    start_hour =  long(time_range.start) / 3600L
    end_hour =  long(time_range.stop) / 3600L
    if query_name not in prepared:
        base = (
        "SELECT count, first, last FROM stream_metadata_hourly WHERE subsite = '{:s}' And node ='{:s}' and sensor = '{:s}' AND method = '{:s}' and stream = '{:s}' AND hour >= ? and hour <= ?").format(
            stream_key.subsite,
            stream_key.node,
            stream_key.sensor,
            stream_key.method,
            stream_key.stream.name)
        query = session.prepare(base)
        query.consistency_level = query_consistency
        prepared[query_name] = query
    query = prepared[query_name]
    return session.execute(query, [start_hour, end_hour])

def get_cass_bin_information(stream_key, time_range, session=None, prepared=None, query_consistency=None):
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
    for count, first, last in results:
        fb = time_to_bin(first)
        lb = time_to_bin(last)
        total = total + count
        if fb == lb:
            bins = update_bin_counts(bins, fb, count, first, last)
        else:
            for i in range(fb, lb+1):
                bins = update_bin_counts(bins, i, count, first, last)
    return sorted(bins.keys()), bins, total

def update_bin_counts(bins, key, count, first, last):
    if key in bins:
        c, f, l = bins[key]
        c = c + count
        if first < f:
            f = first
        if last > l:
            l = last
        bins[key] = (c, f, l)
    else:
        bins[key] = (count, first, last)
    return bins



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
        base = "select %s  from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s' and time > ? and time < ?" % \
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
def fetch_l0_provenance(subsite, node, sensor, method, deployment, prov_uuid,  session=None, prepared=None,
                        query_consistency=None):
    """
    Fetch the l0_provenance entry for the passed information.
    All of the neccessary infromation should be stored as a tuple in the
    provenance metadata store.
    """
    base = "select * from dataset_l0_provenance where subsite='%s' and node='%s' and sensor='%s' and method='%s' and deployment=%s and id=%s" % \
           (subsite, node, sensor, method, deployment, prov_uuid)
    statement = SimpleStatement(base, consistency_level=query_consistency)
    rows = session.execute(statement)
    return rows


@log_timing
@cassandra_session
def fetch_nth_data(stream_key, time_range, strict_range=False, num_points=1000, chunk_size=100, session=None,
                   prepared=None, query_consistency=None):
    """
    Given a time range, generate evenly spaced times over the specified interval. Fetch a single
    result from either side of each point in time.
    :param stream_key:
    :param time_range:
    :param num_points:
    :param chunk_size:
    :param session:
    :param prepared:
    :return:
    """
    cols = get_query_columns(stream_key)

    metadata_bins, bin_information, total_data = get_cass_bin_information(stream_key, time_range)
    # Fetch it all if it's gonna be close to the same size
    if total_data < num_points * 2:
        log.info("Total points (%d) returned is less than twice the requested (%d).  Returning all points.", total_data, num_points)
        _, results = fetch_all_data(stream_key, time_range)
    # We have a small amount of bins with data so we can read them all
    elif len(metadata_bins) < engine.app.config['UI_FULL_BIN_LIMIT']:
        log.info("Reading all (%d) bins and then sampling.", len(metadata_bins))
        _, results = sample_full_bins(stream_key, time_range, num_points, metadata_bins, cols)
    # We have a lot of bins so just grab the first from each of the bins
    elif len(metadata_bins) > num_points:
        log.info("More bins (%d) than requested points (%d). Selecting first particle from %d bins.", len(metadata_bins), num_points, num_points)
        _, results = sample_n_bins(stream_key, time_range, num_points, metadata_bins, cols)
    else:
        log.info("Sampling %d points across %d bins.", num_points, len(metadata_bins))
        _, results = sample_n_points(stream_key, time_range, num_points, metadata_bins, bin_information, cols)

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
    return cols, to_return

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
@cassandra_session
def fetch_all_data(stream_key, time_range, session=None, prepared=None, query_consistency=None):

    """
    Given a time range, Fetch all records from the starting hour to ending hour
    :param stream_key:
    :param time_range:
    :param session:
    :param prepared:
    :return:
    """
    cols = get_query_columns(stream_key)

    start = time_range.start
    stop = time_range.stop

    # list all the hour bins from start to stop
    bins = xrange(time_to_bin(start), time_to_bin(stop)+1, 1)
    futures = []
    for bin_num in bins:
        futures.append(execution_pool.apply_async(execute_unlimited_query, (stream_key, cols, bin_num, time_range)))

    rows = []
    for future in futures:
        rows.extend(future.get())

    return cols, rows



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
def store_qc_results(qc_results, particle_pk, particle_id, particle_bin, parameter, session=None, strict_range=False, prepared=None, query_consistency=None):
    query_string = "insert into ooi.qc_results " \
                   "(subsite, node, sensor, bin, deployment, stream, id, parameter, results) " \
                   "values ('{}', '{}', '{}', {}, {}, '{}', {}, '{}', '{}')".format(
        particle_pk.get('subsite'), particle_pk.get('node'), particle_pk.get('sensor'),
        particle_bin, particle_pk.get('deployment'), particle_pk.get('stream'),
        particle_id, parameter, qc_results)
    query = session.execute(query_string)

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


@cassandra_session
def get_available_time_range(stream_key, session=None, prepared=None, query_consistency=None):
    rows = session.execute(prepared[STREAM_EXISTS_PS], (stream_key.subsite, stream_key.node,
                                                        stream_key.sensor, stream_key.method,
                                                        stream_key.stream.name))
    stream, count, first, last = rows[0]
    return TimeRange(first, last + 1)
