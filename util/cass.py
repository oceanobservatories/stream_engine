import numpy
import engine

from collections import deque
from functools import wraps
from multiprocessing.pool import Pool
from multiprocessing import BoundedSemaphore
from threading import Lock

from cassandra.cluster import Cluster, QueryExhausted, ResponseFuture, _NOT_SET
from cassandra.query import _clean_column_name, tuple_factory
from cassandra.concurrent import execute_concurrent_with_args

from util.common import log_timing, TimeRange


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
SELECT stream FROM STREAM_METADATA
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
    if global_cassandra_state.get('cluster') is None:
        with multiprocess_lock:
            engine.app.logger.info('Creating cassandra session')
            global_cassandra_state['cluster'] = Cluster(
                engine.app.config['CASSANDRA_CONTACT_POINTS'],
                control_connection_timeout=engine.app.config['CASSANDRA_CONNECT_TIMEOUT'],
                compression=True)

    if global_cassandra_state.get('session') is None:
        with multiprocess_lock:
            session = global_cassandra_state['cluster'].connect(engine.app.config['CASSANDRA_KEYSPACE'])
            session.row_factory = tuple_factory
            global_cassandra_state['session'] = session
            prep = global_cassandra_state['prepared_statements'] = {}
            prep[STREAM_EXISTS_PS] = session.prepare(STREAM_EXISTS_RAW)
            prep[METADATA_FOR_REFDES_PS] = session.prepare(METADATA_FOR_REFDES_RAW)
            prep[DISTINCT_PS] = session.prepare(DISTINCT_RAW)

    return global_cassandra_state['session'], global_cassandra_state['prepared_statements']


def cassandra_session(func):
    """
    Wrap a function to automatically add the session and prepared arguments retrieved from get_session
    :param func:
    :return:
    """
    @wraps(func)
    def inner(*args, **kwargs):
        session, preps = get_session()
        kwargs['session'] = session
        kwargs['prepared'] = preps
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


class JoinedFuture(ResponseFuture):
    """
    This class provides a single ResponseFuture interface for a
    collection of queries that need to be run in sequence.  This
    supports paging within and across individual queries.  If you
    do not need paging, cassandra.concurrent.execute_concurrent is
    likely to be much more efficient.

    Note about recursion.  The ResponseFuture add_callback and add_errback
    function will call the callback argument immediately if a result is available.
    Normally, the next page is not fetched from cassandra immediately so
    start_fetching_next_page will return and the callback will return.  However,
    in the case of this class, more than one future is active at one time, so it
    is more likely that the next page will be available immediately.  It is
    important to realize that ResponseFutures run on a single event loop.  If
    the futures parameter in this constructor is passed in as a generator, then
    the depth of recursion will be limited to buffer_size.
    """
    def __init__(self, futures, buffer_size=100):
        """
        If futures is a generator expression, then only buffer_size
        futures will be running at the same time.
        """
        self._callbacks = []
        self._errbacks = []
        self._final_result = None
        self._futures = iter(futures)
        self._buffer = deque(maxlen=buffer_size)
        try:
            for i in range(buffer_size):
                self._buffer.append(next(self._futures))
        except StopIteration:
            pass

    def _shift_buffer(self):
        """
        Pop the first future off of the buffer and add
        the next one from the list of futures waiting
        be run.  Then add any callbacks to the new
        first (current) future.
        """
        try:
            self._buffer.append(next(self._futures))
        except StopIteration:
            self._buffer.popleft()

        """
        If ResponseFuture has data, it won't actually add the
        callback to it's list resulting in future calls to
        start_fetching_next_page not having a callback.  This will
        force the callbacks to be added, then if data is ready, start
        the chain of calls to invoke them.  ResponseFuture will only
        call the first of callback or errback if data is ready.
        """
        run = False
        this_future = self._buffer[0]
        with this_future._callback_lock:
            this_future._callbacks.extend(self._callbacks)
            this_future._errbacks.extend(self._errbacks)
            if this_future._final_result is not _NOT_SET or this_future._final_exception:
                run = True

        if run:
            for (fn, args, kwargs) in self._callbacks[0:1]:
                this_future.add_callback(fn, *args, **kwargs)
            for (fn, args, kwargs) in self._errbacks[0:1]:
                this_future.add_errback(fn, *args, **kwargs)

    def add_callback(self, fn, *args, **kwargs):
        self._callbacks.append((fn, args, kwargs))
        self._buffer[0].add_callback(fn, *args, **kwargs)

    def add_errback(self, fn, *args, **kwargs):
        self._errbacks.append((fn, args, kwargs))
        self._buffer[0].add_errback(fn, *args, **kwargs)

    @property
    def has_more_pages(self):
        if len(self._buffer) == 1:
            return self._buffer[0].has_more_pages
        else:
            return len(self._buffer) > 0

    def start_fetching_next_page(self):
        try:
            if self._buffer[0].has_more_pages:
                self._buffer[0].start_fetching_next_page()
            else:
                self._shift_buffer()
        except IndexError:
            raise QueryExhausted

    def result(self):
        if self._final_result is None:
            self._final_result = []
            try:
                while True:
                    self._final_result.extend(self._buffer[0].result())
                    self._shift_buffer()
            except IndexError:
                pass
        return self._final_result


@log_timing
@cassandra_session
def get_distinct_sensors(session=None, prepared=None):
    rows = session.execute(prepared.get(DISTINCT_PS))
    return rows


@log_timing
@cassandra_session
def get_streams(subsite, node, sensor, method, session=None, prepared=None):
    rows = session.execute(prepared[METADATA_FOR_REFDES_PS], (subsite, node, sensor, method))
    return [row[0] for row in rows]


@log_timing
@cassandra_session
def get_query_columns(stream_key, session=None, prepared=None):
    # grab the column names from our metadata
    cols = global_cassandra_state['cluster'].metadata.keyspaces[engine.app.config['CASSANDRA_KEYSPACE']]. \
        tables[stream_key.stream.name].columns.keys()
    cols = map(_clean_column_name, cols)
    # we don't need any parts of the key(0-7) except the time column(5) and deployment column(6)
    cols = cols[5:7] + cols[8:]
    return cols


@log_timing
@cassandra_session
def fetch_data(stream_key, time_range, strict_range=False, session=None, prepared=None):
    cols = get_query_columns(stream_key)

    start = time_range.start
    stop = time_range.stop
    base = "select %%s from %s where subsite='%s' and node='%s' and sensor='%s' and bin=%%s and method='%s'" % \
           (stream_key.stream.name, stream_key.subsite, stream_key.node, stream_key.sensor, stream_key.method)

    # attempt to find one data point beyond the requested start/stop times
    if not strict_range:
        first = session.execute(base % ('time', time_to_bin(start)) + ' and time<%s order by method desc limit 1', (start,))
        last = session.execute(base % ('time', time_to_bin(stop)) + ' and time>%s limit 1', (stop,))
        if first:
            start = first[0][0]
        if last:
            stop = last[0][0]
        # search "close enough" because floats are not exact
        start -= 0.005
        stop += 0.005

    query_name = 'fetch_data_%s_%s_%s_%s_%s' % (stream_key.stream.name, stream_key.subsite,
                                 stream_key.node, stream_key.sensor, stream_key.method)
    if query_name not in prepared:
        base = "select %%s from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s'" % \
           (stream_key.stream.name, stream_key.subsite, stream_key.node, stream_key.sensor, stream_key.method)
        query = session.prepare(base % ','.join(cols) + ' and time>=? and time<=?')
        query.fetch_size = engine.app.config['CASSANDRA_FETCH_SIZE']
        prepared[query_name] = query
    query = prepared[query_name]
    futures_generator_exp = ( session.execute_async(query, (b, start, stop)) for b in xrange(time_to_bin(start), time_to_bin(stop) + 1) )

    return cols, JoinedFuture(futures_generator_exp)


@log_timing
@cassandra_session
def fetch_nth_data(stream_key, time_range, strict_range=False, num_points=1000, chunk_size=100, session=None, prepared=None):
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
    # first, fetch the stream_metadata record for this refdes/stream
    # to calculate an estimated data rate
    rows = session.execute(prepared[STREAM_EXISTS_PS], (stream_key.subsite, stream_key.node,
                                                        stream_key.sensor, stream_key.method,
                                                        stream_key.stream.name))

    if rows:
        stream, count, first, last = rows[0]
        elapsed = last - first
        if elapsed > 0:
            rate = count / elapsed

            # if we estimate a small number of rows we should just fetch everything
            estimated_count = time_range.secs() * rate
            if estimated_count < num_points * 4:
                return fetch_data(stream_key, time_range, strict_range)

    # lots of rows or we were unable to estimate, fetch every ~nth record
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

    rows = [r[1][0] for r in rows if r[0] and len(r[1]) > 0]
    return cols, FakeFuture(rows)


@cassandra_session
@log_timing
def execute_query(stream_key, cols, times, time_range, strict_range=False, session=None, prepared=None):
    if strict_range:
        query = session.prepare(
            "select %s from %s where subsite='%s' and node='%s'"
            " and sensor='%s' and bin=? and method='%s' and time>=%s and time<=?"
            " order by method desc limit 1" % (','.join(cols),
                stream_key.stream.name, stream_key.subsite, stream_key.node,
                stream_key.sensor, stream_key.method, time_range.start))
    else:
        query_name = '%s_%s_%s_%s_%s' % (stream_key.stream.name, stream_key.subsite,
                                         stream_key.node, stream_key.sensor, stream_key.method)
        if query_name not in prepared:
            base = "select %s from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s'" % \
                   (','.join(cols), stream_key.stream.name, stream_key.subsite,
                    stream_key.node, stream_key.sensor, stream_key.method)
            query = session.prepare(base + ' and time<=? order by method desc limit 1')
            prepared[query_name] = query
        query = prepared[query_name]

    result = list(execute_concurrent_with_args(session, query, times, concurrency=50))
    return result


@cassandra_session
def stream_exists(subsite, node, sensor, method, stream, session=None, prepared=None):
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
    for i in xrange(pool_size*2):
        futures.append(execution_pool.apply_async(connect_worker))

    [f.get() for f in futures]


def time_to_bin(t):
    return int(t / (24 * 60 * 60))

@cassandra_session
def get_available_time_range(stream_key, session=None, prepared=None):
    rows = session.execute(prepared[STREAM_EXISTS_PS], (stream_key.subsite, stream_key.node,
                                                    stream_key.sensor, stream_key.method,
                                                    stream_key.stream.name))
    stream, count, first, last = rows[0]
    return TimeRange(first, last+1)