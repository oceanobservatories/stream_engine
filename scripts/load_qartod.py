import csv
import collections
import argparse
import getpass
import json
import psycopg2
from psycopg2 import sql


# WARNING: This script is meant as a temporary way to load QartodTestRecord objects into Postgres. It should be removed from this repository as soon as a user interface is in place for loading QARTOD configuration data!

# Load QARTOD configurations into the qartod_tests table of the PostgreSQL database by calling this script on the applicable CSV files. The CSVs are created by downloading to CSV from the google sheets.
# Climatology Test Inputs - Test Values.csv:  This contains climatology config data to date.
# QARTOD Requirements and Action Items - CTD Gross Range Lookup.csv:  This contains gross range test config data to date. It should include the CGSN data as well.
# QARTOD Requirements and Action Items - CGSN CTD Gross Range Lookup.csv:  This is a working sheet of CGSN that was used to collect gross range config data. The prior CSV should contain this data, but in case there is a need to load from this CSV, support for its format has been included in this script.
# ClimatologyTestInputs.csv:  New format for climatology config data - the climatologyTable column points to individual CSV files for each climatology qcconfig - see RS01SBPS-SF01A-2A-CTDPFA102-practical_salinity.csv for an example.

def parse_number(string):
    try:
        return int(string)
    except ValueError:
        return float(string)


def parse_qartod_dict(qartod_dict):
    subsite = qartod_dict.get('subsite')
    refdes = qartod_dict.get('Reference Designator')
    if subsite:
        # Climatology Test Inputs
        node = qartod_dict.get('node')
        sensor = qartod_dict.get('sensor')
    elif refdes:
        # CTD Gross Range Lookup
        subsite, node, sensor = refdes.split('-', 2)
    else:
        # CGSN CTD Gross Range Lookup
        refdes = qartod_dict.get('refdes')
        subsite, node, sensor = refdes.split('-', 2)
        
    stream = qartod_dict.get('stream')
    if not stream:
        stream = qartod_dict.get('Stream Identifier')
    
    parameters = qartod_dict.get('parameters')
    if not parameters:
        # CTD Gross Range Lookup
        param = qartod_dict.get('Parameter Name')
        if not param:
            # CGSN CTD Gross Range Lookup
            param = qartod_dict.get('particleKey')
        # convert legacy 'parameter' field into new 'parameters' dictionary
        parameters = "{'inp': '%s'}" % param

    qcconfig = qartod_dict.get('qcConfig')
    climatologyTable = qartod_dict.get('climatologyTable')
    if not qcconfig and climatologyTable:
         # Extract configuration details for test inputs referring to dataset variables
        params = parameters
        # single quoted strings in parameters (i.e. from the database field) will mess up the json.loads call
        params = params.replace("'", "\"")
        param_dict = {}
        try:
            param_dict = json.loads(params)      
        except ValueError:
            print('Failure deserializing QC parameter configuration ', params)
            return        
        # climatology qcconfig is encoded in a separate CSV file pointed to by climatologyTable
        qcconfig = parse_climatology_table(climatologyTable, param_dict)
    elif not qcconfig:
        qcconfig = {'qartod': {'gross_range_test': {}}}
        # suspect_span is optional
        suspect_span_string = qartod_dict.get('User (min, max)')
        if not suspect_span_string:
            # CGSN CTD Gross Range Lookup
            suspect_span_string = qartod_dict.get('user_range')
        if suspect_span_string:
            suspect_span = [parse_number(x) for x in suspect_span_string[1:-1].split(', ')]
            qcconfig['qartod']['gross_range_test']['suspect_span'] = suspect_span
    
        # fail_span is not optional
        fail_span_string = qartod_dict.get('Sensor (min, max)')
        if not fail_span_string:
            fail_span_string = qartod_dict['gross_range']
        fail_span = [parse_number(x) for x in fail_span_string[1:-1].split(', ')]
        qcconfig['qartod']['gross_range_test']['fail_span'] = fail_span
        qcconfig = str(qcconfig)

    # do some data sanitation - valid JSON uses null instead None
    qcconfig = qcconfig.replace('None', 'null')
    parameters = parameters.replace('None', 'null')
    source = qartod_dict.get('source')
    if not source:
        source = qartod_dict.get('Source')

    return collections.OrderedDict({'id': None, 'subsite': subsite, 'node': node, 'sensor': sensor, 'stream': stream, 
                                    'parameters': parameters, 'qcconfig': qcconfig, 'source': source})


def parse_qartod_file(filepath):
    qartod_list = []
    with open(filepath, mode='r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            qartod_list.append(parse_qartod_dict(row))
    return qartod_list


def parse_climatology_table(filepath, param_dict):
    prefix = "{'qartod': {'climatology_test': {'config': ["
    suffix = "]}}}"

    configs = []
    with open(filepath, mode='r') as csvfile:
        reader = csv.reader(csvfile)
        # NOTE: time_bins[0] is empty for alignment purposes!
        time_bins = reader.next()

        #don't add a zspan if there is no value for zinp, doing so will cause tests to  not run
        includeZSpan = True
        if param_dict and (param_dict['zinp'] == 'null' or param_dict['zinp'] == 'None'):
               includeZSpan = False

        for row in reader:
            if row:
                pressure_bin = row[0]
                # skip first row which is blank for time_bins and the pressure_bin data for other rows
                for i in range(1, len(time_bins)):
                    if includeZSpan:
                        data = "{'tspan': %s, 'zspan': %s, 'vspan': %s, 'period': 'month'}" % (time_bins[i], pressure_bin, row[i])
                    else:
                        data = "{'tspan': %s, 'vspan': %s, 'period': 'month'}" % (time_bins[i], row[i])
                    configs.append(data)
    return '%s%s%s' % (prefix, ', '.join(configs), suffix)


def insert_qartod_records(qartod_list):
    username = raw_input('Connecting to PostgreSQL to insert QARTOD records...\nusername: ')
    password = getpass.getpass('password: ')
    connection = None
    try:
        connection = psycopg2.connect(user=username, password=password, host='localhost', port='5432', database='metadata')
        for parsed_qartod_dict in qartod_list:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT nextval('qartod_test_sequence')")
                    qartod_id = cursor.fetchone()
                    parsed_qartod_dict['id'] = qartod_id
                    fields = sql.SQL(', ').join(map(sql.Identifier, parsed_qartod_dict.keys()))
                    values = sql.SQL(', ').join(map(sql.Placeholder, parsed_qartod_dict.keys()))
                    cursor.execute(sql.SQL("INSERT INTO qartod_tests ({}) VALUES ({})").format(fields, values), parsed_qartod_dict)
    finally:
        if connection:
            connection.close()


def main():
    parser = argparse.ArgumentParser(description="Parse a CSV indicating QARTOD test records.")
    parser.add_argument("file", type=str, help="filepath for the QARTOD test CSV file to parse")
    args = parser.parse_args()
    data = parse_qartod_file(args.file)
    insert_qartod_records(data)


if __name__ == '__main__':
    main()
