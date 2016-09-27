import logging
import time
import uuid
from collections import deque, namedtuple
from itertools import izip
from multiprocessing import BoundedSemaphore

import msgpack
import numpy
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, PagedResult
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import _clean_column_name, tuple_factory, BatchStatement

import engine
from util.common import log_timing
from util.datamodel import to_xray_dataset
from util.metadata_service import (CASS_LOCATION_NAME, get_location_metadata_by_store, get_location_metadata,
                                   metadata_service_api)

logging.getLogger('cassandra').setLevel(logging.WARNING)
log = logging.getLogger(__name__)

l0_stream_columns = ['time', 'id', 'driver_class', 'driver_host', 'driver_module', 'driver_version', 'event_json']
ProvTuple = namedtuple('provenance_tuple',
                       ['subsite', 'sensor', 'node', 'method', 'deployment', 'id', 'file_name', 'parser_name',
                        'parser_version'])
StreamProvTuple = namedtuple('stream_provenance_tuple', l0_stream_columns)

L0_DATASET = """select * from dataset_l0_provenance
where subsite=? and node=? and sensor=? and method=? and deployment=? and id=?"""

L0_STREAM_ONE = """SELECT {:s} FROM streaming_l0_provenance
WHERE refdes = ? AND method = ? and time <= ? ORDER BY time DESC LIMIT 1""".format(', '.join(l0_stream_columns))

L0_STREAM_RANGE = """SELECT {:s} FROM streaming_l0_provenance
WHERE refdes = ? AND method = ? and time >= ? and time <= ?""".format(', '.join(l0_stream_columns))


# noinspection PyUnresolvedReferences
class SessionManager(object):
    _prepared_statement_cache = {}
    _multiprocess_lock = BoundedSemaphore(4)

    @classmethod
    def create_pool(cls, cluster, keyspace, consistency_level=None, fetch_size=None,
                    default_timeout=None, process_count=None):
        # cls.__pool = Pool(processes=process_count, initializer=cls._setup,
        #                   initargs=(cluster, keyspace, consistency_level, fetch_size, default_timeout))
        cls._setup(cluster, keyspace, consistency_level, fetch_size, default_timeout)

    @classmethod
    def _setup(cls, cluster, keyspace, consistency_level, fetch_size, default_timeout):
        cls.cluster = cluster
        with cls._multiprocess_lock:
            cls.__session = cls.cluster.connect(keyspace)
        cls.__session.row_factory = tuple_factory
        if consistency_level is not None:
            cls.__session.default_consistency_level = consistency_level
        if fetch_size is not None:
            cls.__session.default_fetch_size = fetch_size
        if default_timeout is not None:
            cls.__session.default_timeout = default_timeout
        cls._prepared_statement_cache = {}

    @classmethod
    def prepare(cls, statement):
        if statement not in cls._prepared_statement_cache:
            cls._prepared_statement_cache[statement] = cls.__session.prepare(statement)
        return cls._prepared_statement_cache[statement]

    def close_pool(self):
        self.pool.close()
        self.pool.join()

    @classmethod
    def get_query_columns(cls, table):
        # grab the column names from our metadata
        cols = cls.cluster.metadata.keyspaces[cls.__session.keyspace].tables[table].columns.keys()
        cols = map(_clean_column_name, cols)
        unneeded = ['subsite', 'node', 'sensor', 'method']
        cols = [c for c in cols if c not in unneeded]
        return cols

    @classmethod
    def execute(cls, *args, **kwargs):
        return cls.__session.execute(*args, **kwargs)

    @classmethod
    def session(cls):
        return cls.__session

    @classmethod
    def pool(cls):
        return cls.__pool


def _init():
    consistency_str = engine.app.config['CASSANDRA_QUERY_CONSISTENCY']
    consistency = ConsistencyLevel.name_to_value.get(consistency_str)
    if consistency is None:
        log.warn('Unable to find consistency: %s defaulting to LOCAL_ONE', consistency_str)
        consistency = ConsistencyLevel.LOCAL_ONE
    cluster = Cluster(
            engine.app.config['CASSANDRA_CONTACT_POINTS'],
            control_connection_timeout=engine.app.config['CASSANDRA_CONNECT_TIMEOUT'],
            compression=True,
            protocol_version=3)
    SessionManager.create_pool(cluster,
                               engine.app.config['CASSANDRA_KEYSPACE'],
                               consistency_level=consistency,
                               fetch_size=engine.app.config['CASSANDRA_FETCH_SIZE'],
                               default_timeout=engine.app.config['CASSANDRA_DEFAULT_TIMEOUT'],
                               process_count=engine.app.config['POOL_SIZE'])


@log_timing(log)
def get_cass_lookback_dataset(stream_key, start_time, data_bin, deployments, request_id):
    # try to fetch the first n times to ensure we get a deployment value in there.
    cols, rows = fetch_with_func(query_n_before, stream_key,
                                 [(data_bin, start_time, engine.app.config['LOOKBACK_QUERY_LIMIT'])])
    needed = set(deployments)
    dep_idx = cols.index('deployment')
    ret_rows = []
    for r in rows:
        if r[dep_idx] in needed:
            ret_rows.append(r)
            needed.remove(r[dep_idx])
    return to_xray_dataset(cols, ret_rows, stream_key, request_id)


@log_timing(log)
def query_bin_first(stream_key, bins, cols=None):
    # attempt to find one data point beyond the requested start/stop times
    query = "select %s from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s' order by method, time limit 1" % \
            (', '.join(cols), stream_key.stream.name, stream_key.subsite, stream_key.node,
             stream_key.sensor, stream_key.method)
    query = SessionManager.prepare(query)
    result = []
    # prepare the arguments for cassandra. Each need to be in their own list
    bins = [[x] for x in bins]
    for success, rows in execute_concurrent_with_args(SessionManager.session(), query, bins, concurrency=50):
        if success:
            result.extend(list(rows))
    return result


@log_timing(log)
def query_first_after(stream_key, times_and_bins, cols):
    query = "select %s from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s' and time >= ? ORDER BY method ASC, time ASC LIMIT 1" % \
            (', '.join(cols), stream_key.stream.name, stream_key.subsite, stream_key.node, stream_key.sensor,
             stream_key.method)
    query = SessionManager.prepare(query)
    result = []
    for success, rows in execute_concurrent_with_args(SessionManager.session(), query, times_and_bins, concurrency=50):
        if success:
            result.extend(list(rows))
    return result


@log_timing(log)
def query_n_before(stream_key, query_arguments, cols):
    query = "select %s from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s' and time <= ? ORDER BY method DESC, time DESC LIMIT ?" % \
            (', '.join(cols), stream_key.stream.name, stream_key.subsite, stream_key.node,
             stream_key.sensor, stream_key.method)
    query = SessionManager.prepare(query)
    result = []
    for success, rows in execute_concurrent_with_args(SessionManager.session(), query, query_arguments, concurrency=50):
        if success:
            result.extend(list(rows))
    return result


@log_timing(log)
def query_full_bin(stream_key, bins_and_limit, cols):
    query = "select %s from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s' and time >= ? and time <= ?" % \
            (', '.join(cols), stream_key.stream.name, stream_key.subsite, stream_key.node,
             stream_key.sensor, stream_key.method)
    query = SessionManager.prepare(query)
    result = []
    for success, rows in execute_concurrent_with_args(SessionManager.session(), query, bins_and_limit, concurrency=50):
        if success:
            result.extend(list(rows))
    return result


@log_timing(log)
def fetch_concurrent(stream_key, cols, times, concurrency=50):  # TODO: remove - unused
    query = "select %s from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s' and time>=? and time<=?" % \
            (','.join(cols), stream_key.stream.name, stream_key.subsite, stream_key.node, stream_key.sensor,
             stream_key.method)
    query = SessionManager.prepare(query)
    results = execute_concurrent_with_args(SessionManager.session(), query, times, concurrency=concurrency)
    results = [list(r[1]) if type(r[1]) == PagedResult else r[1] for r in results if r[0]]
    return results


@log_timing(log)
def fetch_l0_provenance(stream_key, provenance_values, deployment):
    """
    Fetch the l0_provenance entry for the passed information.
    All of the necessary information should be stored as a tuple in the
    provenance metadata store.
    """
    # UUIDs are cast to strings so remove all 'None' values
    if stream_key.method.startswith('streamed'):
        deployment = 0

    prov_ids = []
    for each in set(provenance_values):
        try:
            prov_ids.append(uuid.UUID(each))
        except ValueError:
            pass

    provenance_arguments = [
        (stream_key.subsite, stream_key.node, stream_key.sensor,
         stream_key.method, deployment, prov_id) for prov_id in prov_ids]

    query = SessionManager.prepare(L0_DATASET)
    results = execute_concurrent_with_args(SessionManager.session(), query, provenance_arguments)
    records = [ProvTuple(*rows[0]) for success, rows in results if success and len(rows) > 0]

    if len(provenance_arguments) != len(records):
        log.warn("Could not find %d provenance entries", len(provenance_arguments) - len(records))

    prov_dict = {
        str(row.id): {'file_name': row.file_name,
                      'parser_name': row.parser_name,
                      'parser_version': row.parser_version}
        for row in records}
    return prov_dict


@log_timing(log)
def fetch_nth_data(stream_key, time_range, num_points=1000, location_metadata=None, request_id=None):
    """
    Given a time range, generate evenly spaced times over the specified interval. Fetch a single
    result from either side of each point in time.
    :param stream_key:
    :param time_range:
    :param num_points:
    :return:
    """
    cols = SessionManager.get_query_columns(stream_key.stream.name)

    if location_metadata is None:
        location_metadata, _, _ = get_location_metadata(stream_key, time_range)

    estimated_rate = location_metadata.particle_rate()
    estimated_particles = int(estimated_rate * time_range.secs())
    data_ratio = estimated_particles / num_points
    log.info("CASS: Estimated total number of points to be %d based on calculated mean rate of %f particles/s",
             estimated_particles, estimated_rate)
    # Fetch it all if it's gonna be close to the same size
    if data_ratio < engine.app.config['UI_FULL_RETURN_RATIO']:
        log.info(
                "CASS: Estimated points (%d) / the requested  number (%d) is less than ratio %f.  Returning all points.",
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
    return to_xray_dataset(cols, to_return, stream_key, request_id)


@log_timing(log)
def sample_full_bins(stream_key, time_range, num_points, metadata_bins, cols=None):
    # Read all the data and do sampling from there.
    # There may need to be de-duplicating done on this method
    _, all_data = fetch_with_func(query_full_bin, stream_key,
                                  [(x, time_range.start, time_range.stop) for x in metadata_bins], cols)
    if len(all_data) < num_points * 4:
        results = all_data
    else:
        indexes = numpy.linspace(0, len(all_data) - 1, num_points).astype(int)
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
    indexes = numpy.linspace(0, len(metadata_bins) - 1, num_points - 2).astype(int)
    bins_to_use = numpy.array(metadata_bins)[indexes].tolist()
    cols, rows = fetch_with_func(query_first_after, stream_key, [(sb, time_range.start)], cols=cols)
    results.extend(rows)

    # Get the first data point
    _, rows = fetch_with_func(query_bin_first, stream_key, bins_to_use, cols)
    results.extend(rows)

    # Get the last data point
    _, rows = fetch_with_func(query_n_before, stream_key, [(lb, time_range.stop, 1)], cols=cols)
    results.extend(rows)
    return cols, results


@log_timing(log)
def sample_n_points(stream_key, time_range, num_points, metadata_bins, bin_information, cols=None):
    results = []
    # get the first point
    cols, rows = fetch_with_func(query_first_after, stream_key, [(metadata_bins[0], time_range.start)], cols=cols)
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
    _, lin_sampled = fetch_with_func(query_n_before, stream_key, times, cols)
    results.extend(lin_sampled)
    # Sort the data
    results = sorted(results, key=lambda dat: dat[1])
    return cols, results


def fetch_with_func(f, stream_key, args, cols=None):
    if cols is None:
        cols = SessionManager.get_query_columns(stream_key.stream.name)
    return cols, f(stream_key, args, cols)


@log_timing(log)
def fetch_bin(stream_key, time_bin):
    """
    Fetch an entire bin
    """
    cols = SessionManager.get_query_columns(stream_key.stream.name)

    base = "select %s from %s where subsite=? and node=? and sensor=? and bin=? and method=?" \
           % (','.join(cols), stream_key.stream.name)
    query = SessionManager.prepare(base)
    return cols, list(SessionManager.execute(query, (stream_key.subsite,
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
    :return:
    """
    if location_metadata is None:
        location_metadata = get_location_metadata_by_store(stream_key, time_range, CASS_LOCATION_NAME)
    cols = SessionManager.get_query_columns(stream_key.stream.name)

    rows = []
    for bin_num in location_metadata.bin_list:
        rows.extend(execute_unlimited_query(stream_key, cols, bin_num, time_range))

    return cols, rows


@log_timing(log)
def get_full_cass_dataset(stream_key, time_range, location_metadata=None, request_id=None):
    cols, rows = fetch_all_data(stream_key, time_range, location_metadata)
    return to_xray_dataset(cols, rows, stream_key, request_id)


@log_timing(log)
def execute_unlimited_query(stream_key, cols, time_bin, time_range):
    base = ("select %s from %s where subsite=? and node=? and sensor=? and bin=? " +
            "and method=? and time>=? and time<=?") % (','.join(cols), stream_key.stream.name)
    query = SessionManager.prepare(base)
    return list(SessionManager.execute(query, (stream_key.subsite,
                                               stream_key.node,
                                               stream_key.sensor,
                                               time_bin,
                                               stream_key.method,
                                               time_range.start,
                                               time_range.stop)))


def _get_stream_row_count(stream_key, data_bin):
    COUNT_QUERY = "SELECT COUNT(*) FROM {:s} WHERE subsite = ? and node = ? and sensor = ? and bin = ? and method = ?"
    count_query = SessionManager.prepare(COUNT_QUERY.format(stream_key.stream.name))
    return SessionManager.execute(
        count_query, (stream_key.subsite, stream_key.node, stream_key.sensor, data_bin, stream_key.method)
    )[0][0]


@log_timing(log)
def insert_dataset(stream_key, dataset):
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
    # get the metadata partition
    bin_meta = metadata_service_api.get_partition_metadata_record(
        *(stream_key.as_tuple() + (data_bin, CASS_LOCATION_NAME))
    )
    if bin_meta is not None and not engine.app.config['SAN_CASS_OVERWRITE']:
        # If there is already data and we do not want overwriting return an error
        error_message = "Data present in Cassandra bin {:d} for {:s}. " + \
                        "Aborting operation!".format(data_bin, stream_key.as_refdes())
        log.error(error_message)
        return error_message
    # get the data in the correct format
    cols = SessionManager.get_query_columns(stream_key.stream.name)
    dynamic_cols = cols[1:]
    key_cols = ['subsite', 'node', 'sensor', 'bin', 'method']
    cols = key_cols + dynamic_cols
    arrays = {p.name for p in stream_key.stream.parameters
              if not p.is_function and p.parameter_type == 'array<quantity>'}
    data_lists['bin'] = [data_bin] * size
    # id and provenance are expected to be UUIDs so convert them to uuids
    data_lists['id'] = [uuid.UUID(x) for x in dataset['id'].values]
    data_lists['provenance'] = [uuid.UUID(x) for x in dataset['provenance'].values]
    for i in arrays:
        data_lists[i] = [msgpack.packb(x) for x in dataset[i].values.tolist()]
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
    if bin_meta is not None:
        log.warn("Data present in Cassandra bin %s for %s.  Overwriting old and adding new data.", data_bin,
                 stream_key.as_refdes())

    # get the query to insert information
    col_names = ', '.join(cols)
    # Take subsite, node, sensor, ?, and method
    key_str = "'{:s}', '{:s}', '{:s}', ?, '{:s}'".format(stream_key.subsite, stream_key.node, stream_key.sensor,
                                                         stream_key.method)
    # and the rest as ? for thingskey_cols[:3] +['?'] + key_cols[4:] + ['?' for _ in cols]
    data_str = ', '.join(['?' for _ in dynamic_cols])
    full_str = '{:s}, {:s}'.format(key_str, data_str)
    query = 'INSERT INTO {:s} ({:s}) VALUES ({:s})'.format(stream_key.stream.name, col_names, full_str)
    query = SessionManager.prepare(query)

    # make the data list
    to_insert = []
    data_names = ['bin'] + dynamic_cols
    for i in range(size):
        row = [data_lists[col][i] for col in data_names]
        to_insert.append(row)

    ###############################################################
    # Build & execute query to create rows and count the new rows #
    ###############################################################
    primary_key_columns = ['subsite', 'node', 'sensor', 'bin', 'method', 'time', 'deployment', 'id']
    create_rows_columns = ', '.join(primary_key_columns)
    # Fill in (subsite, node, sensor, method) leaving (bin, time, deployment, id) to be bound
    create_rows_values = "'{:s}', '{:s}', '{:s}', ?, '{:s}', ?, ?, ?".format(
        stream_key.subsite, stream_key.node, stream_key.sensor, stream_key.method
    )
    create_rows_query = 'INSERT INTO {:s} ({:s}) VALUES ({:s}) IF NOT EXISTS'.format(
        stream_key.stream.name, create_rows_columns, create_rows_values
    )
    create_rows_query = SessionManager.prepare(create_rows_query)
    # We only want (bin, time, deployment, id)
    create_rows_data = numpy.array(to_insert)[:, :4]
    # Execute query
    insert_count = 0
    fails = 0
    for success, result in execute_concurrent_with_args(SessionManager.session(), create_rows_query, create_rows_data, concurrency=50, raise_on_first_error=False):
        if not success:
            fails += 1
        elif result[0][0]:
            insert_count += 1
    if fails > 0:
        log.warn("Failed to create %d rows within Cassandra bin %d for %s!", fails, data_bin, stream_key.as_refdes())

    # Update previously existing rows and new mostly empty rows
    fails = 0
    for success, _ in execute_concurrent_with_args(SessionManager.session(), query, to_insert, concurrency=50, raise_on_first_error=False):
        if not success:
            fails += 1
    if fails > 0:
        log.warn("Failed to update %d rows within Cassandra bin %d for %s!", fails, data_bin, stream_key.as_refdes())
    update_count = len(to_insert) - fails - insert_count

    # Index the new data into the metadata record
    first = dataset['time'].min()
    last = dataset['time'].max()
    bin_meta = metadata_service_api.build_partition_metadata_record(
        *(stream_key.as_tuple() + (data_bin, CASS_LOCATION_NAME, first, last, insert_count))
    )
    metadata_service_api.index_partition_metadata_record(bin_meta)

    ret_val = 'Inserted {:d} and updated {:d} particles within Cassandra bin {:d} for {:s}.'.format(insert_count, update_count, data_bin, stream_key.as_refdes())
    log.info(ret_val)
    return ret_val


@log_timing(log)
def fetch_annotations(stream_key, time_range, with_mooring=True):
    # ------- Query 1 -------
    # query where annotation effectivity is within the query time range
    # or straddles the end points
    select_columns = "subsite, node, sensor, time, time2, parameters, provenance, annotation, method, deployment, id "
    select_clause = "select " + select_columns + "from annotations "
    where_clause = "where subsite=? and node=? and sensor=?"
    time_constraint = " and time>=%s and time<=%s"
    query_base = select_clause + where_clause + time_constraint
    query_string = query_base % (time_range.start, time_range.stop)

    query1 = SessionManager.prepare(query_string)

    # ------- Query 2 --------
    # Where annotation effectivity straddles the entire query time range
    # -- This is necessary because of the way the Cassandra uses the
    # start-time in the primary key
    time_constraint_wide = " and time<=%s"
    query_base_wide = select_clause + where_clause + time_constraint_wide
    query_string_wide = query_base_wide % time_range.start

    query2 = SessionManager.prepare(query_string_wide)

    # ----------------------------------------------------------------------
    # Prepare arguments for both query1 and query2
    # ----------------------------------------------------------------------
    # [(subsite,node,sensor),(subsite,node,''),(subsite,'','')
    tup1 = (stream_key.subsite, stream_key.node, stream_key.sensor)
    tup2 = (stream_key.subsite, stream_key.node, '')
    tup3 = (stream_key.subsite, '', '')
    args = [tup1]
    if with_mooring:
        args.append(tup2)
        args.append(tup3)

    result = []
    # query where annotation effectivity is within the query time range
    # or straddles the end points
    for success, rows in execute_concurrent_with_args(SessionManager.session(), query1, args, concurrency=3):
        if success:
            result.extend(list(rows))

    temp = []
    for success, rows in execute_concurrent_with_args(SessionManager.session(), query2, args, concurrency=3):
        if success:
            temp.extend(list(rows))

    for row in temp:
        time2 = row[4]
        if time_range.stop < time2:
            result.append(row)

    return result


@log_timing(log)
def store_qc_results(qc_results_values, pk, particle_ids, particle_bins, particle_deploys,  # TODO: remove - unused
                     param_name, strict_range=False):
    start_time = time.clock()
    if engine.app.config['QC_RESULTS_STORAGE_SYSTEM'] == CASS_LOCATION_NAME:
        log.info('Storing QC results in Cassandra.')
        insert_results = SessionManager.prepare(
                "insert into ooi.qc_results "
                "(subsite, node, sensor, bin, deployment, stream, id, parameter, results) "
                "values (?, ?, ?, ?, ?, ?, ?, ?, ?)")

        batch = BatchStatement()
        for (qc_results, particle_id, particle_bin, particle_deploy) in izip(qc_results_values, particle_ids,
                                                                             particle_bins, particle_deploys):
            batch.add(insert_results, (pk.get('subsite'), pk.get('node'), pk.get('sensor'),
                                       particle_bin, particle_deploy, pk.get('stream'),
                                       uuid.UUID(particle_id), param_name, str(qc_results)))
        SessionManager.session().execute_async(batch)
        log.info("QC results stored in {} seconds.".format(time.clock() - start_time))
    elif engine.app.config['QC_RESULTS_STORAGE_SYSTEM'] == 'log':
        log.info('Writing QC results to log file.')
        qc_log = logging.getLogger('qc.results')
        qc_log_string = ""
        for (qc_results, particle_id, particle_bin, particle_deploy) in izip(qc_results_values, particle_ids,
                                                                             particle_bins, particle_deploys):
            qc_log_string += "refdes:{0}-{1}-{2}, bin:{3}, stream:{4}, deployment:{5}, id:{6}, parameter:{7}, qc results:{8}\n" \
                .format(pk.get('subsite'), pk.get('node'), pk.get('sensor'), particle_bin,
                        pk.get('stream'), particle_deploy, particle_id, param_name, qc_results)
        qc_log.info(qc_log_string[:-1])
        log.info("QC results stored in {} seconds.".format(time.clock() - start_time))
    else:
        log.info("Configured storage system '{}' not recognized, qc results not stored.".format(
                engine.app.config['QC_RESULTS_STORAGE_SYSTEM']))


def initialize_worker():    # TODO: remove - unused
    _init()
