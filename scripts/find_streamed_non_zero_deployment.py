
"""
General:
Script to find time ranges of streamed data in cassandra where the deployment number is something other than 0.
The intention is for these time ranges to be used to purge out data with the improper deployment numbers.
The script produces a csv file containing both good time ranges (at least 10 consecutive records having deployment 0)
and bad time ranges (less than 10 consecutive records having deployment 0).

Usage:
    1). log onto a stream_engine server
    2). cd to stream_engine directory
    3). conda activate engine
    4). run command with desired option.

Example commands:
    1). Entire array for and specific time range:
    nohup python -u -m scripts.find_streamed_non_zero_deployment --array_prefix=CE --start_date=2016-01-01T00:00:00.000Z --end_date=2017-01-01T00:00:00.000Z > logs/find_streamed_non_zero_deployment.out 2>&1 &
    2). Specific reference designator and specific time range:
    nohup python -u -m scripts.find_streamed_non_zero_deployment --subsite=CE02SHBP --node=LJ01D --sensor=09-PCO2WB103 --start_date=2016-07-01T00:00:00.000Z --end_date=2016-09-01T00:00:00.000Z > logs/find_streamed_non_zero_deployment.out 2>&1 &
"""

import argparse
import os
import operator
import logging
from datetime import datetime
import ntplib
import requests
from collections import namedtuple
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import tuple_factory

import numpy as np
import pandas as pd


date_str_format = "%Y-%m-%dT%H:%M:%S.%fZ"
log_level = logging.INFO
log_dir = "./logs"
log_to_console = False

defer_threshold = 10

# create logger
log = logging.getLogger(__name__)


def configure_logger(log_file_name):
    log.setLevel(log_level)

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


def date_str_to_ntp(date_str):
    dt = datetime.strptime(date_str, date_str_format)
    # python 3 utc_ts = dt.timestamp()
    utc_ts = (dt - datetime(1970, 1, 1)).total_seconds()
    ntp_ts = ntplib.system_to_ntp_time(utc_ts)
    return ntp_ts


def init_cassandra():
    log.info("Initializing Cassandra ...")
    import config.default as config_default
    cassandra_contact_points = config_default.CASSANDRA_CONTACT_POINTS
    cassandra_query_consistency = config_default.CASSANDRA_QUERY_CONSISTENCY
    cassandra_connect_timeout = config_default.CASSANDRA_CONNECT_TIMEOUT
    cassandra_keyspace = config_default.CASSANDRA_KEYSPACE
    cassandra_fetch_size = config_default.CASSANDRA_FETCH_SIZE
    cassandra_default_timeout = config_default.CASSANDRA_DEFAULT_TIMEOUT

    import config.local as config_local
    if hasattr(config_local, 'CASSANDRA_CONTACT_POINTS'):
        log.info("Initializing Cassandra with local.py")
        cassandra_contact_points = config_local.CASSANDRA_CONTACT_POINTS
        cassandra_query_consistency = config_local.CASSANDRA_QUERY_CONSISTENCY

    consistency_str = cassandra_query_consistency
    consistency_level = ConsistencyLevel.name_to_value.get(consistency_str)
    if consistency_level is None:
        log.warn('Unable to find consistency: %s defaulting to LOCAL_ONE', consistency_str)
        consistency_level = ConsistencyLevel.LOCAL_ONE
    cluster = Cluster(
        cassandra_contact_points,
        control_connection_timeout=cassandra_connect_timeout,
        compression=True,
        protocol_version=4)

    session = cluster.connect(cassandra_keyspace)
    session.row_factory = tuple_factory
    session.default_consistency_level = consistency_level
    if cassandra_fetch_size is not None:
        session.default_fetch_size = cassandra_fetch_size
    if cassandra_default_timeout is not None:
        session.default_timeout = cassandra_default_timeout

    log.debug("Cassandra initialized")
    return session


def get_metadata_service_urls():
    log.info("Initializing MetadataServiceAPI ...")
    import config.default as config_default
    stream_metadata_service_url = config_default.STREAM_METADATA_SERVICE_URL
    partition_metadata_service_url = config_default.PARTITION_METADATA_SERVICE_URL

    import config.local as config_local
    if hasattr(config_local, 'STREAM_METADATA_SERVICE_URL'):
        log.info("Initializing MetadataServiceAPI with local.py")
        stream_metadata_service_url = config_local.STREAM_METADATA_SERVICE_URL
        partition_metadata_service_url = config_local.PARTITION_METADATA_SERVICE_URL

    return stream_metadata_service_url, partition_metadata_service_url


StreamMetadataRecord = namedtuple("StreamMetadataRecord", ['subsite', 'node', 'sensor', 'method', 'stream'])
PartitionMetadataRecord = namedtuple("PartitionMetadataRecord", ['stream_metadata_record', 'bin'])
ReferenceDesignator = namedtuple("ReferenceDesignator", ['subsite', 'node', 'sensor'])


def get_stream_metadata_record_list(stream_metadata_service_url, array_prefix, subsite, node, sensor, start_ntp, end_ntp):
    stream_metadata_record_list = []

    sensors = []
    if subsite and node and sensor:
        sensors.append(ReferenceDesignator(subsite=subsite, node=node, sensor=sensor))
    else:
        subsites = []
        if subsite:
            subsites.append(subsite)
        elif array_prefix:
            response = requests.get('/'.join((stream_metadata_service_url, 'inv')))
            if response.status_code == requests.codes.ok:
                ss_list = response.json()
                subsites = [ss for ss in ss_list if ss.startswith(array_prefix)]

        nodes = []
        for ss in subsites:
            if node:
                nodes.append(ReferenceDesignator(subsite=ss, node=node, sensor=None))
            else:
                response = requests.get('/'.join((stream_metadata_service_url, 'inv', ss)))
                if response.status_code == requests.codes.ok:
                    n_list = response.json()
                    for n in n_list:
                        nodes.append(ReferenceDesignator(subsite=ss, node=n, sensor=None))
        for n in nodes:
            response = requests.get('/'.join((stream_metadata_service_url, 'inv', n.subsite, n.node)))
            if response.status_code == requests.codes.ok:
                s_list = response.json()
                for s in s_list:
                    sensors.append(ReferenceDesignator(subsite=n.subsite, node=n.node, sensor=s))

    for s in sensors:
        smr_url = '/'.join((stream_metadata_service_url, 'inv', s.subsite, s.node, s.sensor))
        response = requests.get(smr_url)
        if response.status_code == requests.codes.ok:
            sm_list = response.json()
            filtered_sm_list = [
                StreamMetadataRecord(method=sm['method'], stream=sm['stream'], **sm['referenceDesignator'])
                for sm in sm_list
                if sm['first'] < end_ntp and sm['last'] >= start_ntp and sm['method'] == 'streamed']
            log.info("%s streams found: %d" % (str(s), len(filtered_sm_list)))
            stream_metadata_record_list.extend(filtered_sm_list)

    log.info("Total stream_metadata_record_list size after filtering by time and method: %d"
             % len(stream_metadata_record_list))
    stream_metadata_record_list = sorted(stream_metadata_record_list,
                                         key=operator.attrgetter('subsite', 'node', 'sensor', 'stream', 'method'))
    log.debug("stream_metadata_record_list: %s" % str(stream_metadata_record_list))
    return stream_metadata_record_list


def get_bin_list(partition_metadata_service_url, stream_metadata_record, start_ntp, end_ntp):
    bin_list = []
    partition_metadata_request_url = '/'.join((partition_metadata_service_url, 'inv',
                                               stream_metadata_record.subsite,
                                               stream_metadata_record.node,
                                               stream_metadata_record.sensor,
                                               stream_metadata_record.method,
                                               stream_metadata_record.stream))

    log.debug("partition_metadata_request_url: %s" % partition_metadata_request_url)

    response = requests.get(partition_metadata_request_url)
    if response.status_code == requests.codes.ok:
        response_str = response.json()
        bin_list = [rec['bin'] for rec in response_str if rec['first'] < end_ntp and rec['last'] >= start_ntp]
        log.debug("bin_list size after filtering by time: %d" % len(bin_list))
        bin_list.sort()
        log.debug("bin_list: %s" % str(bin_list))
    return bin_list


def query_full_bin(session, stream_metadata_record, bins_and_limit, cols):
    query = "select %s from %s where subsite='%s' and node='%s' and sensor='%s' and bin=? and method='%s' and time >= ? and time <= ?" % \
            (', '.join(cols), stream_metadata_record.stream, stream_metadata_record.subsite, stream_metadata_record.node,
             stream_metadata_record.sensor, stream_metadata_record.method)
    query = session.prepare(query)
    result = []
    for success, rows in execute_concurrent_with_args(session, query, bins_and_limit, concurrency=50):
        if success:
            result.extend(list(rows))
        else:
            log.error("Cassandra query was not successful")
    return result


def get_ref_des(array_prefix, subsite, node, sensor):
    refdes = None
    if subsite:
        refdes = subsite
    elif array_prefix:
        refdes = array_prefix

    if refdes:
        if node:
            refdes = '-'.join((refdes, node))
            if sensor:
                refdes = '-'.join((refdes, sensor))
    return refdes


def get_base_output_filename(ref_des, start_ntp, end_ntp):
    return os.path.join(log_dir, "%s_%d_%d_%s" %
                        (ref_des, start_ntp, end_ntp, datetime.utcnow().strftime("%Y%m%dT%H%M%S")))


def additional_columns_header():
    return 'time_utc', 'end_time_utc', 'end_time', 'end_bin', 'end_id', 'stream',\
        'count_all_dep', 'count_curr_dep', 'count_other_dep', 'dupe_count'


def additional_columns_data(ntp_time, end_ntp_time, end_bin, end_id, stream,
                            total_count, count_curr_dep, count_next_dep, dupe_count):
    ts = ntplib.ntp_to_system_time(ntp_time)
    # python3 return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(date_str_format), stream
    end_ts = ntplib.ntp_to_system_time(end_ntp_time)
    return datetime.utcfromtimestamp(ts).strftime(date_str_format),\
        datetime.utcfromtimestamp(end_ts).strftime(date_str_format),\
        end_ntp_time, end_bin, end_id, stream, total_count, count_curr_dep, count_next_dep, dupe_count


def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    print("%s pid: %d" % (script_name, os.getpid()))

    parser = argparse.ArgumentParser(description="Parse a CSV indicating QARTOD test records.")
    parser.add_argument("--array_prefix", type=str, help="array_prefix")
    parser.add_argument("--subsite", type=str, help="subsite")
    parser.add_argument("--node", type=str, help="node")
    parser.add_argument("--sensor", type=str, help="sensor")
    parser.add_argument("--start_date", type=str, help="start of time range. format: 2000-01-01T00:00:00.000Z")
    parser.add_argument("--end_date", type=str, help="end of time range. format: 2000-01-01T00:00:00.000Z")

    args = parser.parse_args()

    array_prefix = args.array_prefix
    subsite = args.subsite
    node = args.node
    sensor = args.sensor
    start_date_str = args.start_date
    end_date_str = args.end_date

    if not (array_prefix or subsite) or not start_date_str or not end_date_str:
        print("Must specify at least (array_prefix or subsite) and start_date and end_date")
        return

    try:
        start_ntp = date_str_to_ntp(start_date_str)
        end_ntp = date_str_to_ntp(end_date_str)
    except ValueError as e:
        print(e)
        return

    ref_des = get_ref_des(array_prefix, subsite, node, sensor)

    base_output_filename = get_base_output_filename(ref_des, start_ntp, end_ntp)

    configure_logger(base_output_filename + '.log')

    log.info("%s pid: %d" % (script_name, os.getpid()))

    log.info("Searching for deployments for %s from %s (%d) to %s (%d)" %
             (ref_des, start_date_str, start_ntp, end_date_str, end_ntp))

    stream_metadata_service_url, partition_metadata_service_url = get_metadata_service_urls()

    stream_metadata_record_list = get_stream_metadata_record_list(stream_metadata_service_url,
                                                                  array_prefix, subsite, node, sensor,
                                                                  start_ntp, end_ntp)

    if not stream_metadata_record_list:
        log.info("No stream metadata records found in time range")
        return

    cassandra_session = init_cassandra()

    query_cols = ('subsite', 'node', 'sensor', 'bin', 'method', 'time', 'deployment', 'id')

    dep_col_idx = query_cols.index('deployment')
    time_col_idx = query_cols.index('time')
    bin_col_idx = query_cols.index('bin')
    id_col_idx = query_cols.index('id')

    excluded_stream_list = ['metbk_hourly', 'botpt_nano_sample_15s', 'botpt_nano_sample_24hr']

    write_header_output_file = True
    for stream_metadata_record in stream_metadata_record_list:
        if stream_metadata_record.stream in excluded_stream_list:
            continue

        s_data = []

        first_rec_curr_dep = None
        last_rec_curr_dep = None
        first_rec_next_dep = None
        last_rec_prev_bin = None
        cnt_rec_next_dep = 0

        total_cnt_curr_dep = 0
        dupe_cnt_curr_dep = 0

        total_cnt_next_dep = 0

        bin_list = get_bin_list(partition_metadata_service_url, stream_metadata_record, start_ntp, end_ntp)

        log.info("Number of bins found for %s: %d" % (stream_metadata_record, len(bin_list)))

        # log interval set based on:
        #     ctdbp_no_sample (binsize minutes 360), 5268 partitions took 80 minutes
        #     do_stable_sample (bin size minutes 1440), 1256 partitions took 64 minutes
        log_interval = 50

        len_bin_list = len(bin_list)
        for bin_idx in range(len_bin_list):
            if bin_idx % log_interval == 0 or bin_idx == len_bin_list-1:
                log.info("Processing bin_idx %d of %d" % (bin_idx, len_bin_list))

            bins_and_limit = [(bin_list[bin_idx], start_ntp, end_ntp)]
            p_data = query_full_bin(cassandra_session, stream_metadata_record, bins_and_limit, query_cols)

            deployments = [row_tup[dep_col_idx] for row_tup in p_data]
            dep_change_idx = np.where(np.diff(deployments, prepend=np.nan))[0]
            if len(dep_change_idx) > 1:
                log.info("Number of changes of deployment number in bin %d of %s: %d" %
                          (bin_list[bin_idx], stream_metadata_record, len(dep_change_idx)-1))
                log.debug("Indexes in data where deployment has changed for bin %d of %s: %s" %
                          (bin_list[bin_idx], stream_metadata_record, dep_change_idx))

            for i in range(len(dep_change_idx)):
                idx = dep_change_idx[i]

                if i < len(dep_change_idx)-1:
                    cnt_working_dep = dep_change_idx[i+1] - idx
                else:
                    cnt_working_dep = len(p_data) - idx

                if idx == 0:
                    prev_rec = last_rec_prev_bin
                else:
                    prev_rec = p_data[idx-1]

                is_dupe = False
                if prev_rec and p_data[idx][dep_col_idx] != prev_rec[dep_col_idx] \
                        and p_data[idx][time_col_idx] - prev_rec[time_col_idx] < 0.001:
                    is_dupe = True

                if not first_rec_curr_dep:
                    first_rec_curr_dep = p_data[idx]

                if p_data[idx][dep_col_idx] == first_rec_curr_dep[dep_col_idx]:
                    if is_dupe:
                        dupe_cnt_curr_dep = dupe_cnt_curr_dep + 1
                    total_cnt_curr_dep = total_cnt_curr_dep + cnt_working_dep

                    last_rec_curr_dep = p_data[idx+cnt_working_dep-1]
                    first_rec_next_dep = None
                    cnt_rec_next_dep = 0
                else:
                    if not first_rec_next_dep:
                        first_rec_next_dep = p_data[idx]

                    threshold = 1
                    if p_data[idx][dep_col_idx] == 0:
                        threshold = 10

                    cnt_rec_next_dep = cnt_rec_next_dep + cnt_working_dep
                    if cnt_rec_next_dep >= threshold:
                        s_data.append(first_rec_curr_dep + additional_columns_data(first_rec_curr_dep[time_col_idx],
                                                                                   last_rec_curr_dep[time_col_idx],
                                                                                   last_rec_curr_dep[bin_col_idx],
                                                                                   last_rec_curr_dep[id_col_idx],
                                                                                   stream_metadata_record.stream,
                                                                                   total_cnt_curr_dep + total_cnt_next_dep,
                                                                                   total_cnt_curr_dep,
                                                                                   total_cnt_next_dep,
                                                                                   dupe_cnt_curr_dep))

                        cnt_rec_next_dep = 0
                        first_rec_curr_dep = first_rec_next_dep
                        last_rec_curr_dep = p_data[idx+cnt_working_dep-1]
                        first_rec_next_dep = None

                        total_cnt_curr_dep = cnt_working_dep
                        total_cnt_next_dep = 0

                        if is_dupe:
                            dupe_cnt_curr_dep = 1
                        else:
                            dupe_cnt_curr_dep = 0
                    else:
                        total_cnt_next_dep = total_cnt_next_dep + cnt_working_dep

            if bin_idx == len(bin_list)-1:
                log.debug("appending last record")
                s_data.append(first_rec_curr_dep + additional_columns_data(first_rec_curr_dep[time_col_idx],
                                                                           last_rec_curr_dep[time_col_idx],
                                                                           last_rec_curr_dep[bin_col_idx],
                                                                           last_rec_curr_dep[id_col_idx],
                                                                           stream_metadata_record.stream,
                                                                           total_cnt_curr_dep + total_cnt_next_dep,
                                                                           total_cnt_curr_dep,
                                                                           total_cnt_next_dep,
                                                                           dupe_cnt_curr_dep))

            last_rec_prev_bin = p_data[-1]

        log.debug("s_data: ")
        for record in s_data:
            log.debug(record)

        df = pd.DataFrame(data=s_data, columns=query_cols + additional_columns_header())

        df.to_csv(path_or_buf=base_output_filename + '.csv', sep=',', mode='a', header=write_header_output_file)
        write_header_output_file = False

    log.info("Exiting main for pid %d" % os.getpid())
    print("Exiting main pid: %d" % os.getpid())


if __name__ == '__main__':
    main()
