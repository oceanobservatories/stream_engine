import base64
import importlib
import json
import struct
import sys
import time
from functools import wraps

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import msgpack
import numpy

from app import app
from model.preload import Parameter


sys.path.append('ion-functions')

cluster = Cluster()
session = cluster.connect('ooi2')

distinct_sensors_ps = session.prepare('SELECT DISTINCT subsite, node, sensor FROM stream_metadata')
metadata_refdes_ps = session.prepare('SELECT * FROM STREAM_METADATA where SUBSITE=? and NODE=? and SENSOR=?')
FUNCTION = 'function'


class DataParameter(object):
    def __init__(self, subsite, node, sensor, stream, parameter):
        self.parameter = parameter  # parameter definition from preload
        self.subsite = subsite
        self.node = node
        self.sensor = sensor
        self.stream = stream
        self.stream_key = (subsite, node, sensor, stream)
        self.data = None
        self.shape = None
        self.times = None

    def __eq__(self, other):
        return self.parameter.id == other.parameter.id

    def __repr__(self):
        return json.dumps({
            'name': self.parameter.name,
            'data': self.data
        })


class CalibrationParameter(object):
    def __init__(self, subsite, node, sensor, name, value):
        self.subsite = subsite
        self.node = node
        self.sensor = sensor
        self.name = name
        self.value = value
        self.times = None

    def __eq__(self, other):
        return all([self.subsite == other.subsite,
                    self.node == other.node,
                    self.sensor == other.sensor,
                    self.name == other.name])

    def __repr__(self):
        return json.dumps({
            'name': self.name,
            'value': self.value
        })


class FunctionParameter(object):
    def __init__(self, parameter):
        self.parameter = parameter
        self.data = None
        self.shape = None

    def __eq__(self, other):
        return self.parameter.id == other.parameter.id

    def __repr__(self):
        return json.dumps({
            'name': self.parameter.name,
            'data': self.data,
            'shape': self.shape
        })


class StreamRequest(object):
    def __init__(self):
        self.data = []
        self.coeffs = []
        self.functions = []

    def update(self, other):
        for each in other.data:
            if each not in self.data:
                self.data.append(each)
        for each in other.coeffs:
            if each not in self.coeffs:
                self.coeffs.append(each)
        for each in other.functions:
            if each not in self.functions:
                self.functions.append(each)

    def __repr__(self):
        return json.dumps({'data': str(self.data),
                           'coeffs': str(self.coeffs),
                           'functions': str(self.functions)})


def log_timing(func):
    @wraps(func)
    def inner(*args, **kwargs):
        app.logger.info('Entered method: %s', func)
        start = time.time()
        results = func(*args, **kwargs)
        elapsed = time.time() - start
        app.logger.info('Completed method: %s in %.2f', func, elapsed)
        return results
    return inner


def parse_pdid(pdid_string):
    try:
        return int(pdid_string.split()[0][2:])
    except ValueError:
        app.logger.warn('Unable to parse PDID: %s', pdid_string)
        return None


@log_timing
def find_needed_params(subsite, node, sensor, stream):
    stream_request = StreamRequest()
    for parameter in stream.parameters:
        if parameter.parameter_type.value == FUNCTION:
            stream_request.functions.append(FunctionParameter(parameter))
            # find needed parameters and streams
            for name, value in parameter.parameter_function_map.iteritems():
                if value.startswith('CC'):
                    cc = CalibrationParameter(subsite, node, sensor, value, 1.0)
                    stream_request.coeffs.append(cc)
                elif value.startswith('PD'):
                    pdid = parse_pdid(value)
                    p = Parameter.query.get(pdid)
                    if p in stream.parameters:
                        continue

                    # now see if we can fetch the this supporting data
                    sensor1, first_stream = find_stream(subsite, node, parameter.streams)
                    stream_request.update(find_needed_params(subsite, node, sensor1, first_stream))
        else:
            dparam = DataParameter(subsite, node, sensor, stream.name, parameter)
            stream_request.data.append(dparam)

    return stream_request


@log_timing
def get_distinct_sensors():
    rows = session.execute(distinct_sensors_ps)
    return [(row.subsite, row.node, row.sensor) for row in rows]


@log_timing
def find_stream(subsite, node, streams):
    """
    Attempt to find a "related" sensor which provides one of these streams
    :param subsite:
    :param node:
    :param streams:
    :return:
    """
    stream_map = {s.name: s for s in streams}
    # try to use the reference designator as a literal reference designator
    for subsite, node, sensor in get_distinct_sensors():
        for row in session.execute(metadata_refdes_ps, (subsite, node, sensor)):
            if row.stream in stream_map:
                return sensor, stream_map[row.stream]


@log_timing
def calculate(subsite, node, parameter):
    name = parameter.name
    streams = [stream.name for stream in parameter.streams]
    stream = find_stream(subsite, node, streams)
    rows = session.execute('select %s from %s' % (name, stream))
    data = [row[0] for row in rows]
    return json.dumps(data)


@log_timing
def get_stream(subsite, node, sensor, stream):
    stream_request = find_needed_params(subsite, node, sensor, stream)
    get_data(stream_request)
    execute_dpas(stream_request)
    data = to_msgpack(stream_request)
    return json.dumps(data)


@log_timing
def fetch_data(subsite, node, sensor, stream, limit=5):
    query = SimpleStatement('select * from %s where subsite=%%s and node=%%s and sensor=%%s limit %d' % (stream, limit), fetch_size=100)
    results = session.execute(query, (subsite, node, sensor))
    return results


@log_timing
def pack_data(result_set):
    if isinstance(result_set, list):
        row = result_set[0]
        result_set = result_set[1:]
    else:
        row = result_set.next()

    fields = row._fields
    data = []
    for index, value in enumerate(row):
        data.append([value])

    for row in result_set:
        for index, value in enumerate(row):
            data[index].append(value)
    return {fields[i]: data[i] for i, _ in enumerate(fields)}


@log_timing
def get_data(stream_request):
    needed_streams = {each.stream_key for each in stream_request.data}

    for stream_key in needed_streams:
        subsite, node, sensor, stream = stream_key
        data = pack_data(fetch_data(subsite, node, sensor, stream))
        del(data['id'])
        for each in stream_request.data:
            if each.stream_key == stream_key:
                # this stream contains this data, fetch it
                mytime = data['time']
                mydata = data[each.parameter.name]
                shape = data.get(each.parameter.name + '_shape')
                if shape is not None:
                    shape = [len(mytime)] + shape[0]
                    encoding = each.parameter.value_encoding.value
                    mydata = ''.join(mydata)
                    if encoding in ['int8', 'int16', 'int32', 'uint8', 'uint16']:
                        format_string = 'i'
                        count = len(mydata) / 4
                    elif encoding in ['uint32', 'int64']:
                        format_string = 'l'
                        count = len(mydata) / 8
                    elif 'float' in encoding:
                        format_string = 'd'
                        count = len(mydata) / 8
                    else:
                        app.log.error('Unknown encoding: %s', encoding)
                        continue

                    mydata = numpy.array(struct.unpack('>%d%s' % (count, format_string), mydata))
                    mydata = mydata.reshape(shape)
                else:
                    mydata = numpy.array(mydata)
                each.data = mydata
                each.times = mytime


@log_timing
def execute_dpas(stream_request):
    for each in stream_request.functions:
        func = each.parameter.parameter_function
        func_map = each.parameter.parameter_function_map
        module = importlib.import_module(func.owner)
        args = {}
        for key in func_map:
            found = False
            if func_map[key].startswith('PD'):
                pdid = parse_pdid(func_map[key])
                for data_param in stream_request.data:
                    if pdid == data_param.parameter.id:
                        args[key] = data_param.data
                        found = True
                if not found:
                    app.logger.error('Unable to execute function: %s missing value: PD%d', func.name, pdid)
            elif func_map[key].startswith('CC'):
                app.logger.warn('FIX CCs')
                args[key] = 1.0
        each.data = getattr(module, func.function)(**args)
        each.shape = each.data.shape
        each.data = each.data


@log_timing
def to_msgpack(stream_request):
    d = {}
    for each in stream_request.data:
        d[each.parameter.id] = {
            'data': base64.b64encode(msgpack.packb(list(each.data.flatten()))),
            'shape': each.data.shape,
            'name': each.parameter.name,
            'source': each.stream_key
        }
    for each in stream_request.functions:
        d[each.parameter.id] = {
            'data': base64.b64encode(msgpack.packb(list(each.data.flatten()))),
            'shape': each.shape,
            'name': each.parameter.name,
            'source': 'derived'
        }
    return d