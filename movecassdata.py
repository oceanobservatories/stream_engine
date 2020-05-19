#!/usr/bin/env python

import sys
from util.cass import get_full_cass_dataset, insert_dataset, initialize_worker
from util.common import StreamKey, TimeRange, ZULU_TIMESTAMP_FORMAT, zulu_timestamp_to_ntp_time
from ooi_data.postgres.model import *
from preload_database.database import create_engine_from_url, create_scoped_session
engine = create_engine_from_url(None)
session = create_scoped_session(engine)
MetadataBase.query = session.query_property()

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

    print str(begin_time_stamp), str(end_time_stamp)
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
    print("USAGE: movecassdata.py '<oldsk>' '<newsk>' '<timestamp_range>'")
    print("<oldsk>,<newsk> as <subsite>-<node>-<sensor>:<method>:<stream>")
    print("<stream> as <stream_nbr> or <stream_name>")
    print("<timestamp_range> as <beg_ntp>|<end_ntp> or <beg_zulu>|<end_zulu>")

def main(args):
    if len(args) != 3:
        usage("Expecting 3 arguments, got " + str(len(args)))
        return

    # Extract the arguments
    old_sk_vals = args[0]
    new_sk_vals = args[1]
    time_stamps = args[2]

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
    dataset = get_full_cass_dataset(old_stream_key, time_range, keep_exclusions=True)
    insert_dataset(new_stream_key, dataset)

if __name__ == "__main__":
    # forward all but the program name argument
    main(sys.argv[1:])
