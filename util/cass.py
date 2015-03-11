from functools import wraps
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import engine
from util.common import log_timing

# cassandra database handle
global_cassandra_state = {}

STREAM_EXISTS_PS = 'stream_exists'
METADATA_FOR_REFDES_PS = 'metadata_for_refdes'
DISTINCT_PS = 'distinct'

STREAM_EXISTS_RAW = \
'''
select * from STREAM_METADATA
where SUBSITE=? and NODE=? and SENSOR=? and METHOD=? and STREAM=?
'''

METADATA_FOR_REFDES_RAW = \
'''
SELECT * FROM STREAM_METADATA
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
                          control_connection_timeout=engine.app.config['CASSANDRA_CONNECT_TIMEOUT'])
    if global_cassandra_state.get('session') is None:
        session = global_cassandra_state['cluster'].connect(engine.app.config['CASSANDRA_KEYSPACE'])
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


@log_timing
@cassandra_session
def get_distinct_sensors(session=None, prepared=None):
    rows = session.execute(prepared.get(DISTINCT_PS))
    return [(row.subsite, row.node, row.sensor) for row in rows]


@log_timing
@cassandra_session
def get_streams(subsite, node, sensor, method, session=None, prepared=None):
    return session.execute(prepared[METADATA_FOR_REFDES_PS], (subsite, node, sensor, method))


@log_timing
@cassandra_session
def fetch_data(subsite, node, sensor, method, stream, start, stop, session=None, prepared=None):
    # attempt to find one data point beyond the requested start/stop times
    # TODO - I don't believe this works as written, as it will return the very first record
    # TODO - not the first record before the start time
    base = 'select * from %s where subsite=%%s and node=%%s and sensor=%%s and method=%%s' % stream
    # first = session.execute(base + ' and time<%s limit 1', (subsite, node, sensor, method, start))
    # last = session.execute(base + ' and time>%s limit 1', (subsite, node, sensor, method, stop))
    # if first:
    #     start = first[0].time
    # if last:
    #     stop = last[0].time

    query = SimpleStatement(base + ' and time>=%s and time<=%s', fetch_size=100)
    engine.app.logger.info('Executing cassandra query: %s %s', query, (subsite, node, sensor, method, start, stop))
    results = session.execute(query, (subsite, node, sensor, method, start, stop))
    return results


@cassandra_session
def stream_exists(subsite, node, sensor, method, stream, session=None, prepared=None):
    ps = prepared.get(STREAM_EXISTS_PS)
    rows = session.execute(ps, (subsite, node, sensor, method, stream))
    return len(rows) == 1