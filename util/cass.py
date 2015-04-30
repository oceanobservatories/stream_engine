from functools import wraps
from itertools import izip, chain
from multiprocessing.pool import Pool
from threading import Lock
from cassandra.cluster import Cluster, ResponseFuture
from cassandra.query import SimpleStatement, _clean_column_name, tuple_factory
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import dict_factory
import numpy
from pandas.util.testing import DataFrame
import engine
import pandas

from util.common import log_timing

# cassandra database handle
global_cassandra_state = {}

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
        engine.app.logger.debug('Creating cassandra session')
        global_cassandra_state['cluster'] = Cluster(engine.app.config['CASSANDRA_CONTACT_POINTS'],
                                                    control_connection_timeout=engine.app.config[
                                                        'CASSANDRA_CONNECT_TIMEOUT'],
                                                    compression=True)
    if global_cassandra_state.get('session') is None:
        session = global_cassandra_state['cluster'].connect(engine.app.config['CASSANDRA_KEYSPACE'])
        session.row_factory = tuple_factory
        global_cassandra_state['session'] = session
        prep = global_cassandra_state['prepared_statements'] = {}
        prep[STREAM_EXISTS_PS] = session.prepare(STREAM_EXISTS_RAW)
        prep[METADATA_FOR_REFDES_PS] = session.prepare(METADATA_FOR_REFDES_RAW)
        prep[DISTINCT_PS] = session.prepare(DISTINCT_RAW)
    return global_cassandra_state['session'], global_cassandra_state['prepared_statements']


def cassandra_session(func):
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
def fetch_data(stream_key, time_range, session=None, prepared=None):
    # grab the column names from our metadata
    cols = global_cassandra_state['cluster'].metadata.keyspaces[engine.app.config['CASSANDRA_KEYSPACE']]. \
        tables[stream_key.stream.name].columns.keys()
    cols = map(_clean_column_name, cols)
    # we don't need any parts of the key(1-5) except the time column(4)
    cols = cols[4:5] + cols[6:]

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
    future = session.execute_async(query, (start, stop))

    return cols, future


@log_timing
@cassandra_session
def fetch_nth_data(stream_key, time_range, num_points=1000, chunk_size=100, session=None, prepared=None):
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
                return fetch_data(stream_key, time_range)

    # lots of rows or we were unable to estimate, fetch 2n records
    cols = global_cassandra_state['cluster'].metadata.keyspaces[engine.app.config['CASSANDRA_KEYSPACE']]. \
        tables[stream_key.stream.name].columns.keys()
    cols = map(_clean_column_name, cols)

    start = time_range.start
    stop = time_range.stop

    times = [(t,) for t in numpy.linspace(start, stop, num_points)]

    futures = []
    for i in xrange(0, num_points, chunk_size):
        futures.append(execution_pool.apply_async(execute_query, (stream_key, 0, times[i:i + chunk_size])))
        futures.append(execution_pool.apply_async(execute_query, (stream_key, 1, times[i:i + chunk_size])))

    rows = []
    for i in xrange(0, len(futures), 2):
        rows.extend(list(chain.from_iterable(izip(*(f.get() for f in futures[i:i + 2])))))
    rows = [r[1][0] for r in rows if r[0] and len(r[1]) > 0]

    # return DataFrame(rows, columns=cols[4:5] + cols[6:])
    return cols[4:5] + cols[6:], FakeFuture(rows)


@cassandra_session
def get_nth_queries(stream_key, session=None, prepared=None):
    keys = [stream_key.stream.name, stream_key.subsite, stream_key.node, stream_key.sensor, stream_key.method]

    query_name = '%s_%s_%s_%s_%s' % (stream_key.stream.name, stream_key.subsite, stream_key.node, stream_key.sensor, stream_key.method)
    if query_name not in prepared:
        cols = global_cassandra_state['cluster'].metadata.keyspaces[engine.app.config['CASSANDRA_KEYSPACE']]. \
            tables[stream_key.stream.name].columns.keys()
        cols = map(_clean_column_name, cols)
        query_cols = ','.join(cols[4:5] + cols[6:])

        base = "select %s from %s where subsite='%s' and node='%s' and sensor='%s' and method='%s'" % \
               (query_cols, stream_key.stream.name, stream_key.subsite, stream_key.node, stream_key.sensor, stream_key.method)
        query1 = session.prepare(base + ' and time<=? order by method desc limit 1')
        query2 = session.prepare(base + ' and time>? limit 1')

        prepared[query_name] = (query1, query2)
    return prepared[query_name]


@cassandra_session
@log_timing
def execute_query(stream_key, q, times, session=None, prepared=None):
    statements = get_nth_queries(stream_key)
    result = list(execute_concurrent_with_args(session, statements[q], times, concurrency=50))
    return result


@cassandra_session
def stream_exists(subsite, node, sensor, method, stream, session=None, prepared=None):
    ps = prepared.get(STREAM_EXISTS_PS)
    rows = session.execute(ps, (subsite, node, sensor, method, stream))
    return len(rows) == 1


execution_pool = Pool(8, initializer=get_session)