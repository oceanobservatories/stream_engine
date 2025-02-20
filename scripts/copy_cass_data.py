#!/usr/bin/env python

import argparse
import os

from util.cass import get_full_cass_dataset, insert_san_dataset, initialize_worker, \
    fetch_l0_provenance, insert_provenance
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
    log.error("BAD INPUT: " + str(mesg))
    log.error("USAGE: copy_cass_data.py '<oldsk>' '<newsk>' '<timestamp_range>'")
    log.error("<oldsk>,<newsk> as <subsite>-<node>-<sensor>:<method>:<stream>")
    log.error("<stream> as <stream_nbr> or <stream_name>")
    log.error("<timestamp_range> as <beg_ntp>|<end_ntp> or <beg_zulu>|<end_zulu>")


def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]

    configure_logger(script_name + '.log')
    log.info("starting %s pid: %d" % (script_name, os.getpid()))

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

    args = parser.parse_args()

    # Extract the arguments
    old_sk_vals = args.src_stream_key
    new_sk_vals = args.dest_stream_key
    if not new_sk_vals:
        log.info("Setting dest_stream_key to same as src_stream_key since it was not specified.")
        new_sk_vals = old_sk_vals
    old_dep = args.src_dep
    new_dep = args.dest_dep
    time_stamps = args.time_range

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

        # get the existing deployment datasets for the old stream key. Set keep_exclusions to False
        # so that the "bin" variable is removed from the dataset so that it does not conflict with
        # the "bin" dimension of adcp streams (adcp_velocity_beam)
        dep_datasets = get_full_cass_dataset(old_stream_key, time_range, LocationMetadata(bin_dict),
                                             keep_exclusions=False)

        for dep, dataset in dep_datasets.iteritems():
            log.info("Existing dataset for deployment %d bin %d is size %d."
                     % (dep, bin, dataset.variables['time'].size))
            if old_dep is None or int(old_dep) == dep:
                if new_dep is not None and int(new_dep) != dep:
                    log.info("Changing deployment number from %d to %d" % (dep, int(new_dep)))
                    dataset.variables['deployment'].values[:] = int(new_dep)

                insert_san_dataset(new_stream_key, dataset, bin)

                if 'provenance' in dataset:
                    provenance = np.unique(dataset.provenance.values).astype('str')
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
