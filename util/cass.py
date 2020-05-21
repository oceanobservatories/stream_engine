import logging
import time
import uuid
from collections import deque, namedtuple
from itertools import izip
from multiprocessing import BoundedSemaphore

import msgpack
import numpy
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
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
def get_cass_lookback_dataset(stream_key, start_time, data_bin, deployment_start_time, request_id):
    # try to fetch the first n times before the request start time
    cols, rows = fetch_with_func(query_n_before, stream_key,
                                 [(data_bin, start_time, engine.app.config['LOOKBACK_QUERY_LIMIT'])])
    # Only return data gathered after the start of the first deployment
    # within the time range of this request
    time_idx = cols.index('time')
    ret_rows = []
    for r in rows:
        if r[time_idx] >= deployment_start_time:
            ret_rows.append(r)

    return to_xray_dataset(cols, ret_rows, stream_key, request_id)


@log_timing(log)
def get_cass_lookforward_dataset(stream_key, end_time, data_bin, deployment_stop_time, request_id):
    # try to fetch the first n times after the request end time
    cols, rows = fetch_with_func(query_n_after, stream_key,
                                 [(data_bin, end_time, engine.app.config['LOOKBACK_QUERY_LIMIT'])])
    # Only return data gathered before the end of the last deployment
    # within the time range of this request
    time_idx = cols.index('time')
    ret_rows = []
    for r in rows:
        if r[time_idx] < deployment_stop_time:
            ret_rows.append(r)
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
def query_n_after(stream_key, query_arguments, cols):
    query = "select %s from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s' and time >= ? ORDER BY method ASC, time ASC LIMIT ?" % \
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
    records = [ProvTuple(*rows[0]) for success, rows in results if success and rows]

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
                "CASS: Estimated points (%d) / the requested number (%d) is less than ratio %f. Returning all points.",
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
    tindex = cols.index('time')
    results = sorted(results, key=lambda dat: dat[tindex])
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
        if t < current[1]:
            continue
        if t < current[2]:
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
    tindex = cols.index('time')
    results = sorted(results, key=lambda dat: dat[tindex])
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
def get_full_cass_dataset(stream_key, time_range, location_metadata=None, request_id=None, keep_exclusions=False):
    cols, rows = fetch_all_data(stream_key, time_range, location_metadata)
    return to_xray_dataset(cols, rows, stream_key, request_id, keep_exclusions=keep_exclusions)


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
def insert_san_dataset(stream_key, dataset):
    """
    Insert an xray dataset back into CASSANDRA.
    First we check to see if there is data in the bin, if there is we either overwrite and update
    the values or fail and let the user known why
    :param stream_key: Stream that we are updating
    :param dataset: xray dataset we are updating
    :return:
    """
    # All of the bins on SAN data will be the same in the netcdf file take the first entry
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
    for success, result in execute_concurrent_with_args(SessionManager.session(), \
                            create_rows_query, create_rows_data, concurrency=50, raise_on_first_error=False):
        if not success:
            fails += 1
        elif result[0][0]:
            insert_count += 1
    if fails > 0:
        log.warn("Failed to create %d rows within Cassandra bin %d for %s!", fails, data_bin, stream_key.as_refdes())

    # Update previously existing rows and new mostly empty rows
    fails = 0
    for success, _ in execute_concurrent_with_args(SessionManager.session(), \
                        query, to_insert, concurrency=50, raise_on_first_error=False):
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
def insert_dataset(stream_key, dataset):
    """
    Insert an xray dataset into CASSANDRA using the specified stream_key.
    :param stream_key: Stream that we are using for the data insertion
    :param dataset: xray dataset we are inserting
    :return: str: results of insertion
    """
    # capture the current data from the stream metadata table for later processing
    cur_stream_meta = metadata_service_api.get_stream_metadata_record(*(stream_key.as_tuple()))

    # capture current partition metadata for each unique bin in the dataset for later processing
    cur_part_meta = {}
    for bin_val in numpy.unique(dataset['bin'].values).tolist():
        part_meta = metadata_service_api.get_partition_metadata_record(
            *(stream_key.as_tuple() + (bin_val, CASS_LOCATION_NAME))
        )
        if part_meta:
            # capture the parts that are useful for later
            cur_part_meta[bin_val] = (part_meta.get('count'), part_meta.get('first'), part_meta.get('last'))

    # get the data in the correct format
    cols = SessionManager.get_query_columns(stream_key.stream.name)
    dynamic_cols = cols[1:]
    key_cols = ['subsite', 'node', 'sensor', 'bin', 'method']
    cols = key_cols + dynamic_cols
    arrays = {p.name for p in stream_key.stream.parameters
              if not p.is_function and p.parameter_type == 'array<quantity>'}

    # build the data lists for all the particles to be populated into cassandra
    data_lists = {}
    # id and provenance are expected to be UUIDs so convert them to uuids
    data_lists['id'] = [uuid.UUID(x) for x in dataset['id'].values]
    data_lists['provenance'] = [uuid.UUID(x) for x in dataset['provenance'].values]
    data_lists['bin'] = [bin_val for bin_val in dataset['bin'].values]

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

    # get the query to insert information
    col_names = ', '.join(cols)
    # Take subsite, node, sensor, ?, and method: the ? is for bin
    key_str = "'{:s}', '{:s}', '{:s}', ?, '{:s}'".format(stream_key.subsite, stream_key.node, stream_key.sensor,
                                                         stream_key.method)
    # and the rest as ? for the dynamic columns
    data_str = ', '.join(['?' for _ in dynamic_cols])
    full_str = '{:s}, {:s}'.format(key_str, data_str)
    upsert_query = 'INSERT INTO {:s} ({:s}) VALUES ({:s})'.format(stream_key.stream.name, col_names, full_str)
    upsert_query = SessionManager.prepare(upsert_query)

    count_query = "select count(1) from %s where subsite=? and node=? and sensor=? and bin=? " \
            % (stream_key.stream.name)
    count_query = SessionManager.prepare(count_query)

    prov_query = "select * from dataset_l0_provenance where subsite=? and node=? and sensor=? and method=? " \
            + "and deployment=?"
    prov_query = SessionManager.prepare(prov_query)

    prov_insert = "insert into dataset_l0_provenance (subsite, sensor, node, method, deployment, id, filename, " \
            + "parsername, parserversion) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    prov_insert = SessionManager.prepare(prov_insert)

    # obtain the provenance data for the existing dataset
    deployments = numpy.unique(dataset['deployment'].values).tolist()
    prov_query_args = [(dataset.subsite, dataset.node, dataset.sensor, dataset.collection_method, deployment) \
                       for deployment in deployments]
    prov_query_results = execute_concurrent_with_args(SessionManager.session(), prov_query, prov_query_args)

    # get the number of particles from the first dataset variable
    first_data_var_name = dataset.data_vars.items()[0][0]
    size = dataset[first_data_var_name].size

    # create the data insertion list
    to_insert = []
    data_names = ['bin'] + dynamic_cols
    for i in range(size):
        row = [data_lists[col][i] for col in data_names]
        to_insert.append(row)

    # Build query to create rows
    cluster_key_columns = ['subsite', 'node', 'sensor', 'bin', 'method', 'time', 'deployment', 'id']
    create_rows_columns = ', '.join(cluster_key_columns)
    # Fill in (subsite, node, sensor, method) leaving (bin, time, deployment, id) to be bound
    create_rows_values = "'{:s}', '{:s}', '{:s}', ?, '{:s}', ?, ?, ?".format(
        stream_key.subsite, stream_key.node, stream_key.sensor, stream_key.method
    )
    create_rows_query = 'INSERT INTO {:s} ({:s}) VALUES ({:s}) IF NOT EXISTS'.format(
        stream_key.stream.name, create_rows_columns, create_rows_values
    )
    create_rows_query = SessionManager.prepare(create_rows_query)

    # summarize the partition_metadata information by bin from the insertion data
    count = prev_bin = 0
    first = last = 0.0
    new_part_meta = []
    for entry in to_insert:
        cur_bin = entry[0]
        time = entry[1]
        if prev_bin < 1:  # first iteration
            prev_bin = cur_bin
            first = last = time
        elif prev_bin < cur_bin:
            new_part_meta.append([prev_bin, count, first, last])
            count = 0
            first = last = time
            prev_bin = cur_bin
        else:
            last = time
        count += 1
    if 0 < prev_bin:
        new_part_meta.append([prev_bin, count, first, last])

    # combine current and new metadata as needed
    combined_part_meta = []
    for newmeta in new_part_meta:
        bin_val = newmeta[0]
        count = newmeta[1]
        first = newmeta[2]
        last = newmeta[3]
        curmeta = cur_part_meta.get(bin_val)
        if curmeta:
            curcount = curmeta[0]
            curfirst = curmeta[1]
            curlast = curmeta[2]
            count += curcount
            if curfirst < first:
                first = curfirst
            if curlast > last:
                last = curlast
        combined_part_meta.append((bin_val, count, first, last))

    # Delete the current data from the partition metadata table for the involved bins
    for pkey, pvals in cur_part_meta.items():
        del_resp = metadata_service_api.delete_partition_metadata_record(*(stream_key.as_tuple() +
                                                                           (pkey, 'cass')))
        del_stts = del_resp.get('statusCode')
        del_msg = del_resp.get('message')
        if del_stts == "OK" and del_msg.startswith("Successfully deleted"):
            log.info("Deleted partition_metadata on bin %d for %s: count %d, first %f, last %f",
                     pkey, stream_key.as_refdes(), pvals[0], pvals[1], pvals[2])
        else:
            error_msg = "Failed to delete partition_metadata on bin %d for %s", pkey, stream_key.as_refdes()
            log.error(error_msg)
            return error_msg

    # Insert the combined data into the partition metadata table
    for pmeta in combined_part_meta:
        bin_val = pmeta[0]
        count = pmeta[1]
        first = pmeta[2]
        last = pmeta[3]
        part_meta = metadata_service_api.build_partition_metadata_record(
            *(stream_key.as_tuple() + (bin_val, CASS_LOCATION_NAME, first, last, count)))
        metadata_service_api.index_partition_metadata_record(part_meta)

    # Combine the new data into the stream metadata table with any current metadata
    total = sum(row[1] for row in combined_part_meta)
    first = combined_part_meta[0][2]
    last = combined_part_meta[len(combined_part_meta) - 1][3]
    # if there's a current stream metadata involved do the following
    if cur_stream_meta:
        # adjust the count, first and last entries
        curtotal = cur_stream_meta.get('count')
        curfirst = cur_stream_meta.get('first')
        curlast = cur_stream_meta.get('last')
        total += curtotal
        if curfirst < first:
            first = curfirst
        if curlast > last:
            last = curlast
        # delete the existing stream_metadata row prior to adding the updated one
        del_resp = metadata_service_api.delete_stream_metadata_record(*(stream_key.as_tuple()))
        del_stts = del_resp.get('statusCode')
        del_msg = del_resp.get('message')
        if del_stts == "OK" and del_msg.startswith("Successfully deleted"):
            count = cur_stream_meta.get('count')
            first = cur_stream_meta.get('first')
            last = cur_stream_meta.get('last')
            log.info("Deleted stream_metadata that summarized the partition_metadata records for %s: " +
            "count %d, first %f, last %f", stream_key.as_refdes(), count, first, last)
        else:
            error_msg = "Failed to delete stream_metadata for %s", stream_key.as_refdes()
            log.error(error_msg)
            return error_msg

    smeta = metadata_service_api.build_stream_metadata_record(*(stream_key.as_tuple() + (first, last, total)))
    metadata_service_api.create_stream_metadata_record(smeta)

    # Segregate insertion data list into chunks by bin
    insert_chunks = []
    beg = end = 0
    for pmeta in new_part_meta:
        bin_val = pmeta[0]
        count = pmeta[1]
        end += count
        # capture the create and the upsert data into separate numpy arrays for easy access later
        insert_chunks.append([bin_val, numpy.array(to_insert)[beg:end, :4], numpy.array(to_insert)[beg:end]])
        beg = end

    # Process each chunk of the insertion data
    update_tracker = {}
    for insert_chunk in insert_chunks:
        data_bin = insert_chunk[0]
        # We only want (bin, time, deployment, id) for first insertion to create rows
        create_rows_data = insert_chunk[1]
        upsert_rows_data = insert_chunk[2]

        # captures insert results
        insert_rows = {}
        # Execute insert for each row: no individual insert is guaranteed due to
        #  potential of Coordinator node timeout waiting for replica node response, etc
        # tracking the insertion success isn't critical as the upsert step covers for failures
        idx = 0
        for success, _ in execute_concurrent_with_args(SessionManager.session(), create_rows_query, \
                create_rows_data, concurrency=50, raise_on_first_error=False):
            row = create_rows_data[idx]
            insert_rows[str(row.tolist())] = 1 if success else 0
            idx += 1

        rows_inserted = sum(r[1] for r in insert_rows.items())
        if rows_inserted < idx:
            fails = idx - rows_inserted
            log.info("Failed to insert %d rows within Cassandra bin %d for %s.", fails, data_bin,
                     stream_key.as_refdes())
            failed_rows = [r[0] for r in insert_rows.items() if r[1] == 0]
            log.info("Rows that failed: %s", failed_rows)

        # captures upsert results
        upsert_rows = {}
        # Update previously existing rows (if any) and newly added rows with complete set of data
        idx = 0
        for success, _ in execute_concurrent_with_args(SessionManager.session(), upsert_query, \
                upsert_rows_data, concurrency=50, raise_on_first_error=False):
            row = upsert_rows_data[idx]
            upsert_rows[str(row.tolist())] = 1 if success else 0
            idx += 1

        rows_upserted = sum(r[1] for r in upsert_rows.items())
        if rows_upserted < idx:
            fails = idx - rows_upserted
            log.warn("Failed to update %d rows within Cassandra bin %d for %s!", fails, data_bin,
                     stream_key.as_refdes())
            failed_rows = [r[0] for r in upsert_rows.items() if r[1] == 0]
            log.warn("Rows that failed: %s", failed_rows)

        # query the cassandra stream to capture its count for data in the stream for the stream key and bin
        rs = SessionManager.execute(count_query, (stream_key.subsite, stream_key.node, stream_key.sensor, data_bin))
        count = rs.current_rows[0][0]
        if count == idx:
            log.info("All rows were inserted into Cassandra on bin %d for %s.", data_bin, stream_key.as_refdes())
        else:
            fails = idx - count
            log.warn("There are %d rows missing in Cassandra on bin %d for %s!", fails, data_bin,
                    stream_key.as_refdes())

        update_tracker[data_bin] = (idx, rows_inserted, rows_upserted, count)

    # Process the provenance data, updating the refdes fields as they may have changed from the dataset values
    prov_modified = []
    if prov_query_results[0].success:
        prov_query_iter = prov_query_results[0].result_or_exc
        for row in prov_query_iter:
            pv = ProvTuple(*row)
            prov_modified.append(
                pv._replace(subsite=stream_key.subsite, sensor=stream_key.sensor, node=stream_key.node))

    # insert the modified provenance data
    prov_ins_args = [(row.subsite, row.sensor, row.node, row.method, row.deployment, row.id,
                      row.file_name, row.parser_name, row.parser_version) for row in prov_modified]
    prov_ins_results = execute_concurrent_with_args(SessionManager.session(), prov_insert, prov_ins_args)
    prov_insertions = list(filter(lambda r: r[0], prov_ins_results))
    mesg = 'Provenance row insertions: {:d} of {:d} succeeded'.format(len(prov_insertions), len(prov_modified))
    log.info(mesg)

    mesg = 'update_tracker length: {:d}'.format(len(update_tracker.items()))
    log.info(mesg)

    ret_val = ""
    for ut in update_tracker.items():
        bin_val = ut[0]
        counts = ut[1]
        inserted = counts[1]
        upserted = counts[2]
        in_table = counts[3]
        ret_val += '\n' if len(ret_val) > 0 else ''
        ret_val += 'Cassandra bin {:d} for {:s}: inserted {:d}, upserted {:d}, total rows in table {:d}.'. \
                format(bin_val, stream_key.as_refdes(), inserted, upserted, in_table)

    log.info(ret_val)
    return ret_val


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


def initialize_worker():
    _init()
