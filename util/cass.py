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
    # we don't need any parts of the key(0-7) except the time column(5) and deployment column(6)
    cols = cols[5:7] + cols[8:]
    return cols


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

    start = time_range.start
    stop = time_range.stop
    times = [(time_to_bin(t), t) for t in numpy.linspace(start, stop, num_points)]
    futures = []
    for i in xrange(0, num_points, chunk_size):
        futures.append(execution_pool.apply_async(execute_query, (stream_key, cols, times[i:i + chunk_size], time_range, strict_range)))

    rows = []
    for future in futures:
        rows.extend(future.get())

    uniq = {}
    for row in rows:
        key = "{}{}".format(row[0], row[-1])  # this should include more than the time and the last value
        uniq[key] = row

    rows = sorted(uniq.values())

    rows = [r[1][0] for r in rows if r[0] and len(r[1]) > 0]
    return cols, rows

#---------------------------------------------------------------------------------------
# Fetch all records in the time_range by qurying for every time bin in the time_range
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
    bins = [(x,) for x in xrange(time_to_bin(start), time_to_bin(stop)+1, 1)]

    futures = []
    futures.append(execution_pool.apply_async(execute_unlimited_query, (stream_key, cols, bins, time_range)))

    rows = []
    for future in futures:
        rows.extend(future.get())

    uniq = {}
    # Remove dups:
    for row in rows:
        # The second element of 'rows' is a tuple of record fields
        tup = row[1]
        no_cols = len(tup)
        if no_cols > 0:
            # The first tuple element consists of a colon separated list of
            # all column values for each record (i.e, telemetered, retrieved
            # values), and is used to uniquely identify a record from among 
            # any duplicates that may have been ingested. 
            key = str(tup[0])
            m = hashlib.md5()
            m.update(str(key))
            uniq[m.hexdigest()] = row

    rows = sorted(uniq.values())

    rows = [r[1][0] for r in rows if r[0] and len(r[1]) > 0]
    return cols, rows


@cassandra_session
@log_timing
def execute_query(stream_key, cols, times, time_range, strict_range=False, session=None, prepared=None,
                  query_consistency=None):
    if strict_range:
        query = session.prepare("select %s from %s where subsite='%s' and node='%s'"
                                " and sensor='%s' and bin=? and method='%s' and time>=%s and time<=?"
                                " order by method desc limit 1" % (','.join(cols),
                                                                   stream_key.stream.name, stream_key.subsite, stream_key.node,
                                                                   stream_key.sensor, stream_key.method, time_range.start))
        query.consistency_level = query_consistency
    else:
        query_name = '%s_%s_%s_%s_%s' % (stream_key.stream.name, stream_key.subsite,
                                         stream_key.node, stream_key.sensor, stream_key.method)
        if query_name not in prepared:
            base = "select %s from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s'" % \
                   (','.join(cols), stream_key.stream.name, stream_key.subsite,
                    stream_key.node, stream_key.sensor, stream_key.method)
            query = session.prepare(base + ' and time<=? order by method desc limit 1')
            query.consistency_level = query_consistency
            prepared[query_name] = query
        query = prepared[query_name]

    result = list(execute_concurrent_with_args(session, query, times, concurrency=50))
    return result

@cassandra_session
@log_timing
def execute_unlimited_query(stream_key, cols, time_bins, time_range, session=None, prepared=None,
                  query_consistency=None):

    base = "select %s from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s' and time>=%s and time<=%s " % \
           (','.join(cols), stream_key.stream.name, stream_key.subsite,
            stream_key.node, stream_key.sensor, stream_key.method, time_range.start, time_range.stop)
    query = session.prepare(base + ' order by method')

    result = list(execute_concurrent_with_args(session, query, time_bins, concurrency=50))
    return result

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
