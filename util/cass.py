import numpy
import engine

from functools import wraps
from multiprocessing.pool import Pool
from multiprocessing import BoundedSemaphore
from threading import Lock, Event
from itertools import count

from cassandra.cluster import Cluster, ResponseFuture
from cassandra.query import SimpleStatement, _clean_column_name, tuple_factory, named_tuple_factory
from cassandra.concurrent import execute_concurrent_with_args

from util.common import log_timing

from six.moves import queue


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
            session.default_timeout = engine.app.config['CASSANDRA_DEFAULT_TIMEOUT']
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
    # we don't need any parts of the key(1-6) except the time column(4) and deployment column(5)
    cols = cols[4:6] + cols[7:]
    return cols


@log_timing
@cassandra_session
def fetch_data_sync(stream_key, time_range, session=None, prepared=None):
    cols = get_query_columns(stream_key)

    # attempt to find one data point beyond the requested start/stop times
    start = time_range.start
    stop = time_range.stop
    base = "select %%s from %s where subsite='%s' and node='%s' and sensor='%s' and method='%s'" % \
           (stream_key.stream.name, stream_key.subsite, stream_key.node, stream_key.sensor, stream_key.method)

    first = session.execute(base % 'time' + ' and time<%s order by method desc limit 1', (start,))
    last = session.execute(base % 'time' + ' and time>%s limit 1', (stop,))

    if first:
        start = first[0][0]
    if last:
        stop = last[0][0]

    query = SimpleStatement(base % ','.join(cols) + ' and time>=%s and time<=%s', fetch_size=engine.app.config['CASSANDRA_FETCH_SIZE'])
    engine.app.logger.info('Executing cassandra query: %s %s', query, (start, stop))
    rows = session.execute(query, (start, stop))

    return cols, list(rows)


@log_timing
@cassandra_session
def fetch_nth_data(stream_key, time_range, num_points=50, chunk_size=100, session=None, prepared=None):
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
        stream, cnt, first, last = rows[0]
        elapsed = last - first
        if elapsed > 0:
            rate = cnt / elapsed

            # if we estimate a small number of rows we should just fetch everything
            estimated_count = time_range.secs() * rate
            if estimated_count < num_points * 4:
                return fetch_data_sync(stream_key, time_range)

    # lots of rows or we were unable to estimate, fetch every ~nth record
    cols = get_query_columns(stream_key)

    start = time_range.start
    stop = time_range.stop
    times = [(t,) for t in numpy.linspace(start, stop, num_points)]

    # Read more about the rational for this approach at
    # http://datastax.github.io/python-driver/performance.html
    sentinel = object()
    num_queries = len(times)
    num_started = count()
    num_finished = count()
    finished_event = Event()
    time_iter = iter(times)
    rows = []

    base = "select %%s from %s where subsite='%s' and node='%s' and sensor='%s' and method='%s'" % \
           (stream_key.stream.name, stream_key.subsite, stream_key.node, stream_key.sensor, stream_key.method)

    def query_next(previous_result=sentinel):
        if previous_result is not sentinel:
            if isinstance(previous_result, BaseException):
                engine.app.logger.error("Error on query: %r" % previous_result)
            elif isinstance(previous_result, list):
                if len(previous_result) > 0:
                    rows.extend(previous_result)
            else:
                engine.app.logger.error("Invalid query result: %r", type(previous_result))

            if num_finished.next() >= num_queries:
                finished_event.set()

        if num_started.next() <= num_queries:
            query = SimpleStatement(base % ','.join(cols) + ' and time<=%s order by method desc limit 1')

            try:
                future = session.execute_async(query, time_iter.next())
                future.add_callbacks(query_next, query_next)
            except StopIteration:
                num_finished.next()

    for i in range(min(120, num_queries)):
        query_next()

    finished_event.wait()


    engine.app.logger.info("Returning {} cassandra rows".format(len(rows)))
    return cols, rows


@cassandra_session
@log_timing
def execute_query(stream_key, cols, times, session=None, prepared=None):
    query_name = '%s_%s_%s_%s_%s' % (stream_key.stream.name, stream_key.subsite,
                                     stream_key.node, stream_key.sensor, stream_key.method)
    if query_name not in prepared:
        base = "select %s from %s where subsite='%s' and node='%s' and sensor='%s' and method='%s'" % \
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
