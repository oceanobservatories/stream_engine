import base64
import importlib
import json
import struct
import sys
import time
from functools import wraps

from cassandra.query import SimpleStatement
import msgpack
import numexpr
import numpy

from app import app, session
from model.preload import Stream, Parameter


sys.path.append('ion-functions')

distinct_sensors_ps = session.prepare('SELECT DISTINCT subsite, node, sensor FROM stream_metadata')
metadata_refdes_ps = session.prepare('SELECT * FROM STREAM_METADATA where SUBSITE=? and NODE=? and SENSOR=? and METHOD=?')
FUNCTION = 'function'


class DataParameter(object):
    def __init__(self, subsite, node, sensor, stream, method, parameter):
        self.parameter = parameter  # parameter definition from preload
        self.subsite = subsite
        self.node = node
        self.sensor = sensor
        self.stream = stream
        self.stream_key = (subsite, node, sensor, stream, method)
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
        self.subsite = None
        self.node = None
        self.sensor = None
        self.stream = None
        self.method = None
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
        app.logger.debug('Entered method: %s', func)
        start = time.time()
        results = func(*args, **kwargs)
        elapsed = time.time() - start
        app.logger.debug('Completed method: %s in %.2f', func, elapsed)
        return results

    return inner


def parse_pdid(pdid_string):
    try:
        return int(pdid_string.split()[0][2:])
    except ValueError:
        app.logger.warn('Unable to parse PDID: %s', pdid_string)
        return None


@log_timing
def find_needed_params(subsite, node, sensor, stream, method, parameters):
    stream_request = StreamRequest()
    needed = []
    needed_cc = []

    if len(parameters) == 0:
        for parameter in stream.parameters:
            if parameter.parameter_type.value == FUNCTION:
                needed.extend(parameter.needs())
                needed_cc.extend(parameter.needs_cc())

    else:
        for parameter in parameters:
            parameter = Parameter.query.filter(Parameter.id == parameter).first()
            if parameter is not None and parameter in stream.parameters:
                if parameter.parameter_type.value == FUNCTION:
                    needed.extend(parameter.needs())
                    needed_cc.extend(parameter.needs_cc())

    needed = set(needed)
    distinct_sensors = get_distinct_sensors()

    for parameter in needed:
        if parameter in stream.parameters:
            if parameter.parameter_type.value == FUNCTION:
                stream_request.functions.append(FunctionParameter(parameter))
            else:
                stream_request.data.append(DataParameter(subsite, node, sensor, stream.name, method, parameter))

        else:
            sensor1, stream1 = find_stream(subsite, node, sensor, method, parameter.streams, distinct_sensors)
            if not any([sensor1 is None, stream1 is None]):
                stream_request.data.append(DataParameter(subsite, node, sensor1, stream1.name, method, parameter))

    return stream_request


@log_timing
def get_distinct_sensors():
    rows = session.execute(distinct_sensors_ps)
    return [(row.subsite, row.node, row.sensor) for row in rows]


def find_stream(subsite, node, sensor, method, streams, distinct_sensors):
    """
    Attempt to find a "related" sensor which provides one of these streams
    :param subsite:
    :param node:
    :param streams:
    :return:
    """
    stream_map = {s.name: s for s in streams}

    # check our specific reference designator first
    for row in session.execute(metadata_refdes_ps, (subsite, node, sensor, method)):
        if row.stream in stream_map:
            return sensor, stream_map[row.stream]

    # check other reference designators in the same family
    for subsite1, node1, sensor in distinct_sensors:
        if subsite1 == subsite and node1 == node:
            for row in session.execute(metadata_refdes_ps, (subsite, node, sensor, method)):
                if row.stream in stream_map:
                    return sensor, stream_map[row.stream]

    return None, None


@log_timing
def calculate(request, start, stop, coefficients):
    data = get_stream(request['subsite'], request['node'], request['sensor'],
                      request['stream'], request['method'], request['parameters'], start, stop, coefficients)
    return json.dumps(data, indent=2)


@log_timing
def get_stream(subsite, node, sensor, stream, method, parameters, start, stop, coefficients):
    stream = Stream.query.filter(Stream.name == stream).first()
    stream_request = find_needed_params(subsite, node, sensor, stream, method, parameters)
    get_data(stream_request, start, stop)
    execute_dpas(stream_request, coefficients)
    data = to_msgpack(stream_request, parameters)
    return data


@log_timing
def fetch_data(subsite, node, sensor, stream, method, start, stop, limit=5):
    query = SimpleStatement(
        'select * from %s where subsite=%%s and node=%%s and sensor=%%s and method=%%s and time>%%s and time<%%s'
        % stream, fetch_size=100)
    app.logger.debug('Executing cassandra query: %s', query)
    results = session.execute(query, (subsite, node, sensor, method, start, stop))
    return results


@log_timing
def pack_data(result_set):
    if isinstance(result_set, list):
        if len(result_set) == 0:
            return {}
        row = result_set[0]
        result_set = result_set[1:]
    else:
        row = result_set.next()

    fields = row._fields
    data = []
    for index, value in enumerate(row):
        data.append([value])

    for row in result_set:
        print row
        for index, value in enumerate(row):
            data[index].append(value)
    return {fields[i]: data[i] for i, _ in enumerate(fields)}


@log_timing
def get_data(stream_request, start, stop):
    needed_streams = {each.stream_key for each in stream_request.data}

    for stream_key in needed_streams:
        subsite, node, sensor, stream, method = stream_key
        data = pack_data(fetch_data(subsite, node, sensor, stream, method, start, stop))
        if data:
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
def execute_dpas(stream_request, coefficients):
    parameter_data_map = {}
    for each in stream_request.data + stream_request.functions:
        parameter_data_map[each.parameter.id] = each

    for execute_pass in range(5):
        app.logger.info('Pass %d - attempt to create derived products', execute_pass)
        for each in stream_request.functions:
            app.logger.info('Create %s', each.parameter.name)
            if each.data is not None:
                app.logger.info('Already computed %s, skipping', each.parameter.name)
                continue

            func = each.parameter.parameter_function
            func_map = each.parameter.parameter_function_map

            args = {}
            for key in func_map:
                if func_map[key].startswith('PD'):
                    pdid = parse_pdid(func_map[key])

                    if pdid not in parameter_data_map:
                        app.logger.error('Unable to execute function: %s missing value: PD%d', func.name, pdid)
                        break

                    data_item = parameter_data_map[pdid]
                    if data_item.data is not None:
                        args[key] = data_item.data

                elif func_map[key].startswith('CC'):
                    name = func_map[key]
                    if name in coefficients:
                        args[key] = coefficients.get(name)
                    else:
                        app.logger.warn('Missing CC: %s', name)

            if len(args) == len(func_map):
                try:
                    if func.function_type.value == 'PythonFunction':
                        module = importlib.import_module(func.owner)
                        each.data = getattr(module, func.function)(**args)
                        each.shape = each.data.shape
                        app.logger.warn('dtype: %s', each.data.dtype)
                    elif func.function_type.value == 'NumexprFunction':
                        each.data = numexpr.evaluate(func.function, args)
                        each.shape = each.data.shape
                        app.logger.warn('dtype: %s', each.data.dtype)
                except Exception as e:
                    app.logger.error('Exception creating derived product: %s %s %s', func.owner, func.function, e)


@log_timing
def to_msgpack(stream_request, parameters):
    d = {}
    for each in stream_request.data:
        if each.data is not None and (len(parameters) == 0 or each.parameter.id in parameters):
            d[each.parameter.id] = {
                'data': base64.b64encode(msgpack.packb(list(each.data.flatten()))),
                'shape': each.data.shape,
                'name': each.parameter.name,
                'source': each.stream_key
            }
    for each in stream_request.functions:
        if each.data is not None and (len(parameters) == 0 or each.parameter.id in parameters):
            d[each.parameter.id] = {
                'data': base64.b64encode(msgpack.packb(list(each.data.flatten()))),
                'shape': each.shape,
                'name': each.parameter.name,
                'source': 'derived'
            }
    return d