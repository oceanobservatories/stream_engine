#!/usr/bin/env python

import argparse
import os
import pandas as pd

from util.cass import initialize_worker, \
    fetch_l0_provenance, insert_provenance, fetch_all_data, SessionManager, _get_partition_row_count
from cassandra.concurrent import execute_concurrent_with_args
from util.common import StreamKey, TimeRange, zulu_timestamp_to_ntp_time
from util.metadata_service import (CASS_LOCATION_NAME, get_location_metadata_by_store, get_location_metadata,
                                   metadata_service_api)
from util.metadata_service.partition import _query_partition_metadata
from util.location_metadata import LocationMetadata
from ooi_data.postgres.model import *
from preload_database.database import create_engine_from_url, create_scoped_session
engine = create_engine_from_url(None)
session = create_scoped_session(engine)
MetadataBase.query = session.query_property()

date_str_format = "%Y-%m-%dT%H:%M:%S.%fZ"
log_level = logging.INFO
log_dir = "./logs"
log_to_console = False

# create logger
log = logging.getLogger(__name__)


def configure_logger(log_file_name):
    log.setLevel(log_level)

    if os.path.exists(log_dir):
        log_file_name = os.path.join(log_dir, log_file_name)

    # create handlers for file and console
    fh = logging.FileHandler(log_file_name)
    fh.setLevel(log_level)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch and fh
    fh.setFormatter(formatter)

    # add handlers to logger
    log.addHandler(fh)

    if log_to_console:
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        ch.setFormatter(formatter)
        log.addHandler(ch)

    # Also capture the logging from the util.cass module
    import util.cass
    util.cass.log.addHandler(fh)


def convert_zulu_to_ntp(zulu_timestamp):
    try:
        return zulu_timestamp_to_ntp_time(zulu_timestamp)
    except (ValueError) as e:
        usage(str(e))
        return None

def get_float(value):
    try:
        return float(value)
    except (ValueError):
        return None

def get_ntp_time(time_stamp):
    # If timestamp can be converted to float, it's assumed to be NTP
    # Otherwise it's assumed to be zulu. If it's neither None will be returned.
    float_time_stamp = get_float(time_stamp)
    return float_time_stamp if float_time_stamp else convert_zulu_to_ntp(time_stamp)

def get_stream_key(old_or_new, subsite, node, sensor, method, stream_id):
    # If stream id is all digits, it's assumed to be a valid stream id
    # else it's assumed to be a valid stream name
    if stream_id.isdigit():
        stream = Stream.query.get(int(stream_id))
    else:
        stream = Stream.query.filter(Stream.name == stream_id).first()

    if stream is None:
        usage(old_or_new + " stream id (" + stream_id + ") is invalid: must be existing stream id or name")
        return None

    stream_key =  StreamKey(subsite, node, sensor, method, stream)
    if stream_key is None:
        usage("Invalid values were used for the " + old_or_new + " stream key")

    return stream_key

def get_time_range(time_stamps):
    time_stamp_values = time_stamps.split("|")
    if len(time_stamp_values) != 2:
        usage("Timestamp argument incorrect")
        return None

    # Obtain NTP times from the time stamp values
    begin_time_stamp = get_ntp_time(time_stamp_values[0])
    end_time_stamp = get_ntp_time(time_stamp_values[1])

    # Confirm valid NTP times were obtained
    if begin_time_stamp is None or end_time_stamp is None:
        return None

    if begin_time_stamp > end_time_stamp:
        usage("Begin timestamp (" + time_stamp_values[0] + ") must not exceed end timestamp (" +
              time_stamp_values[1] + ")")
        return None

    log.info("time_range: %s %s" % (str(begin_time_stamp), str(end_time_stamp)))
    return TimeRange(begin_time_stamp, end_time_stamp)

def split_refdes(old_or_new, refdes):
    refdes_values = refdes.split("-", 2)
    if len(refdes_values) != 3:
        usage(old_or_new + " refdes (" + refdes + ") is incorrectly specified")
        return (None, None, None)
    subsite = refdes_values[0]
    node = refdes_values[1]
    sensor = refdes_values[2]
    dash_in_sensor = sensor.find("-")
    another_dash = sensor.rfind("-")
    if dash_in_sensor < 0 or dash_in_sensor == len(sensor) - 1 or dash_in_sensor != another_dash:
        usage(old_or_new + " sensor (" + sensor + ") is incorrectly specified")
        return (None, None, None)
    return (subsite, node, sensor)


def split_sk_vals(old_or_new, skentry):
    skvals = skentry.split(":")
    if len(skvals) != 3:
        usage(old_or_new + " stream-key argument incorrect")
        return (None, None, None)
    return (skvals[0], skvals[1], skvals[2])


def usage(mesg):
    print("BAD INPUT: " + str(mesg))
    print("USAGE: copy_cass_data.py '<oldsk>' '<newsk>' '<timestamp_range>'")
    print("<oldsk>,<newsk> as <subsite>-<node>-<sensor>:<method>:<stream>")
    print("<stream> as <stream_nbr> or <stream_name>")
    print("<timestamp_range> as <beg_ntp>|<end_ntp> or <beg_zulu>|<end_zulu>")


def insert_dataframe(stream_key, dataframe, data_bin=None):
    """
    Insert a dataframe back into CASSANDRA.
    First we check to see if there is data in the bin, if there is we either overwrite and update
    the values or fail and let the user known why
    :param stream_key: Stream that we are updating
    :param dataframe: xray dataset we are updating
    :param data_bin: dataframe contains one bin
    :return:
    """
    # All of the bins on SAN data will be the same in the netcdf file take the first entry
    if not data_bin:
        data_bin = dataframe['bin'].values[0]

    data_lists = {}
    size = dataframe['time'].size

    # get the data in the correct format
    cols = SessionManager.get_query_columns(stream_key.stream.name)
    # remove bin since it is used as a key col below
    dynamic_cols = [c for c in cols if c not in ['bin']]

    key_cols = ['subsite', 'node', 'sensor', 'bin', 'method']

    data_lists['bin'] = [data_bin] * size

    data_notnull = {'bin': [True] * size}

    # Iterate over a copy of the list so that removal of element from original list is safe
    for dc in list(dynamic_cols):
        data_lists[dc] = dataframe[dc].values
        data_notnull[dc] = dataframe[dc].notnull().values

    to_insert = {}
    for i in range(size):
        notnull_dynamic_cols = [col for col in dynamic_cols if data_notnull[col][i]]
        if notnull_dynamic_cols:
            row = [data_lists['bin'][i]] + [data_lists[col][i] for col in notnull_dynamic_cols]
            to_insert.setdefault(tuple(notnull_dynamic_cols), []).append(row)

    total_size = 0
    for k, v in to_insert.items():
        total_size += len(v)
        # log.info("num insert columns: %d, num rows: %d, insert columns: %s" % (len(k), len(v), str(k)))
        log.info("num insert columns: %d, num rows: %d" % (len(k), len(v)))
    log.info("num unique insert statements: %d, total num rows: %d" % (len(to_insert.keys()), total_size))

    # Get the number of records in the bin already
    initial_count = _get_partition_row_count(stream_key, data_bin)
    log.info("initial_count: %d" % initial_count)

    for notnull_dynamic_cols_tup in to_insert.keys():
        #cols = key_cols + dynamic_cols
        cols = key_cols + list(notnull_dynamic_cols_tup)
        # get the query to insert information
        col_names = ', '.join(cols)
        # Take subsite, node, sensor, ?, and method
        key_str = "'{:s}', '{:s}', '{:s}', ?, '{:s}'".format(stream_key.subsite, stream_key.node, stream_key.sensor,
                                                             stream_key.method)
        # and the rest as ? for thingskey_cols[:3] +['?'] + key_cols[4:] + ['?' for _ in cols]
        data_str = ', '.join(['?' for _ in notnull_dynamic_cols_tup])
        full_str = '{:s}, {:s}'.format(key_str, data_str)
        query = 'INSERT INTO {:s} ({:s}) VALUES ({:s})'.format(stream_key.stream.name, col_names, full_str)
        # log.info("sql_stmnt: %s" % query)
        query = SessionManager.prepare(query)

        data_rows = to_insert[notnull_dynamic_cols_tup]

        # Run insert query. Cassandra will handle already existing records as updates
        fails = 0
        for success, _ in execute_concurrent_with_args(SessionManager.session(), query, data_rows, concurrency=50,
                                                       raise_on_first_error=False):
            if not success:
                fails += 1
        if fails > 0:
            log.warn("Failed to insert/update %d out of %d rows within Cassandra bin %d for %s. Columns: %s.",
                     fails, len(data_rows), data_bin, stream_key.as_refdes(), str(cols))

    # Get the number of records in the bin after inserts
    final_count = _get_partition_row_count(stream_key, data_bin)
    log.info("final_count: %d" % final_count)

    insert_count = max(final_count - initial_count, 0)
    update_count = total_size - insert_count

    if insert_count > 0:
        # Index the new data into the metadata record
        first = dataframe['time'].min()
        last = dataframe['time'].max()
        bin_meta = metadata_service_api.build_partition_metadata_record(
            *(stream_key.as_tuple() + (data_bin, CASS_LOCATION_NAME, first, last, insert_count)))
        metadata_service_api.index_partition_metadata_record(bin_meta)

        stream_meta = metadata_service_api.build_stream_metadata_record(
            *(stream_key.as_tuple() + (first, last, insert_count)))
        metadata_service_api.index_stream_metadata_record(stream_meta)

    ret_val = 'Inserted {:d} and updated {:d} particles within Cassandra bin {:d} for {:s}.'\
        .format(insert_count, update_count, data_bin, stream_key.as_refdes())
    log.info(ret_val)
    return ret_val


def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]

    parser = argparse.ArgumentParser(description="Copy cassandra data from one stream key and deployment to another.")
    parser.add_argument("--src_stream_key", type=str,
                        help="--src_stream_key=<subsite>-<node>-<sensor>:<method>:<stream>")
    parser.add_argument("--dest_stream_key", type=str,
                        help="--dest_stream_key=<subsite>-<node>-<sensor>:<method>:<stream>")
    parser.add_argument("--src_dep", type=str,
                        help="--src_dep=<source_deployment_number>")
    parser.add_argument("--dest_dep", type=str,
                        help="--dest_dep=<destination_deployment_number>")
    parser.add_argument("--time_range", type=str,
                        help="--time_range=<beg_ntp>|<end_ntp> or <beg_zulu>|<end_zulu>")
    parser.add_argument("--check_existing", type=bool,
                        help="--check_existing=<True|False>")

    args = parser.parse_args()

    # Extract the arguments
    old_sk_vals = args.src_stream_key
    new_sk_vals = args.dest_stream_key

    configure_logger("%s_%s.log" % (split_sk_vals("old", old_sk_vals)[0], script_name))
    log.info("starting %s pid: %d" % (script_name, os.getpid()))

    if not new_sk_vals:
        log.info("Setting dest_stream_key to same as src_stream_key since it was not specified.")
        new_sk_vals = old_sk_vals
    old_dep = args.src_dep
    new_dep = args.dest_dep
    time_stamps = args.time_range
    check_existing = False if args.check_existing is None else args.check_existing
    log.info("Check existing records: %s" % check_existing)

    if new_sk_vals == old_sk_vals and new_dep == old_dep:
        log.error("New stream_key or deployment must be different from old")
        return

    # Extract the refdes, method and stream id from the stream_key values
    (old_refdes, old_method, old_stream_id) = split_sk_vals("old", old_sk_vals)
    (new_refdes, new_method, new_stream_id) = split_sk_vals("new", new_sk_vals)
    if old_refdes is None or new_refdes is None:
        return

    # Extract the subsite, node and sensor from the refdes values
    (old_subsite, old_node, old_sensor) = split_refdes("old", old_refdes)
    (new_subsite, new_node, new_sensor) = split_refdes("new", new_refdes)
    if old_subsite is None or new_subsite is None:
        return

    # Create StreamKeys
    old_stream_key = get_stream_key("old", old_subsite, old_node, old_sensor, old_method, old_stream_id)
    new_stream_key = get_stream_key("new", new_subsite, new_node, new_sensor, new_method, new_stream_id)
    if old_stream_key is None or new_stream_key is None:
        return

    # Use the time stamp ranges to create a TimeRange
    time_range = get_time_range(time_stamps)
    if time_range is None:
        return

    initialize_worker()

    # Do inserts one bin at a time
    results = _query_partition_metadata(old_stream_key, time_range)
    for bin, store, count, first, last in results:
        if store != CASS_LOCATION_NAME:
            continue
        bin_dict = {bin: (count, first, last)}

        # get the existing deployment dataframes for the old stream key.
        cols, rows = fetch_all_data(old_stream_key, time_range, LocationMetadata(bin_dict))

        dataframe = pd.DataFrame(data=rows, columns=cols)

        dep_dataframe_dict = {}
        for dep, dataframe_group in dataframe.groupby('deployment'):
            dep_dataframe_dict[dep] = dataframe_group

        for dep, dataframe_group in dataframe.groupby('deployment'):
            log.info("Existing dataset for deployment %d bin %d is size %d."
                     % (dep, bin, dataframe_group.time.size))
            if old_dep is None or int(old_dep) == dep:
                if new_dep is not None and int(new_dep) != dep:
                    log.info("Changing deployment number from %d to %d" % (dep, int(new_dep)))
                    dataframe_group['deployment'].values[:] = int(new_dep)

                    existing_df = dep_dataframe_dict.get(int(new_dep))
                    if existing_df is not None and check_existing:
                        orig_size = dataframe_group.time.size
                        dataframe_group = dataframe_group[np.isin(dataframe_group['time'], existing_df['time'], invert=True)]
                        new_size = dataframe_group.time.size
                        if new_size != orig_size:
                            log.info("%d records were excluded since they already exist for deployment %d bin %d. New size of dataframe: %d."
                                     % (orig_size-new_size, int(new_dep), bin, new_size))
                        if new_size == 0:
                            log.info("Nothing left to insert, continuing.")
                            continue

                insert_dataframe(new_stream_key, dataframe_group, bin)

                if 'provenance' in dataframe_group:
                    provenance = np.unique(dataframe_group.provenance.values).astype('str')
                    existing_old_prov = fetch_l0_provenance(old_stream_key, provenance, dep,
                                                            allow_deployment_change=False)
                    existing_new_prov = fetch_l0_provenance(new_stream_key, provenance,
                                                            int(new_dep) if new_dep is not None else dep,
                                                            allow_deployment_change=False)
                    new_prov_ids = set(existing_old_prov.keys()) - set(existing_new_prov.keys())
                    if len(new_prov_ids) > 0:
                        new_prov_dict = {prov_id: existing_old_prov[prov_id] for prov_id in new_prov_ids}
                        insert_provenance(new_stream_key, int(new_dep) if new_dep is not None else dep, new_prov_dict)
            else:
                log.info("Skipping deployment %d" % dep)
    log.info("exiting %s pid: %d" % (script_name, os.getpid()))


if __name__ == "__main__":
    main()
