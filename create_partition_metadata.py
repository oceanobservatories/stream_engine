#!/usr/bin/env python

__author__ = 'ataylor'
from util.cass import get_session, get_distinct_entries, get_bin_stats, insert_partition_metadata
import preload_database.database
from preload_database.model.preload import  Stream
import traceback

# Need to fill this in with all valid methods
valid_methods = ['telemetered', 'recovered', 'streamed', 'streaming', 'recored']


def update_metadata(stream, subsite, node, sensor, data_bin):
    for method in valid_methods:
        count, st, et = get_bin_stats(stream, subsite,  node, sensor, data_bin, method)
        if count is not None:
            refdes = "{:s}-{:s}-{:s}".format(subsite, node, sensor)
            print "Inserting data into metadata: ", stream, refdes, method, data_bin, count, st, et
            insert_partition_metadata(stream, refdes, method, data_bin, count, st, et)



def do_work():
    preload_database.database.initialize_connection(preload_database.database.PreloadDatabaseMode.POPULATED_FILE)
    preload_database.database.open_connection()
    get_session()
    for stream in Stream.query.all():
        try:
            distinct = get_distinct_entries(stream.name)
            for subsite,  node, sensor, data_bin in distinct:
                update_metadata(stream.name, subsite, node, sensor, data_bin)
        except Exception as e:
            print "\t\tError migrating table: " + stream.name + " "  + traceback.format_exc()




if __name__ == '__main__':
    do_work()

