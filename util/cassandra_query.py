import base64
import importlib
import json
import struct
import time
from functools import wraps
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import msgpack
import numexpr
import numpy
from scipy.interpolate import griddata

from engine import app
from model.preload import Stream, Parameter

# cassandra database handle
session = None

FUNCTION = 'function'


def get_session():
    global session
    global distinct_sensors_ps
    global metadata_refdes_ps
    if session is None:
        cluster = Cluster(app.config['CASSANDRA_CONTACT_POINTS'],
                          control_connection_timeout=app.config['CASSANDRA_CONNECT_TIMEOUT'])
        session = cluster.connect(app.config['CASSANDRA_KEYSPACE'])
        distinct_sensors_ps = session.prepare('SELECT DISTINCT subsite, node, sensor FROM stream_metadata')
        metadata_refdes_ps = session.prepare('SELECT * FROM STREAM_METADATA where SUBSITE=? and NODE=? and SENSOR=? and METHOD=?')
    return session


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
        self.dtype = None

    def __eq__(self, other):
        return self.parameter.id == other.parameter.id

    def __repr__(self):
        data = None
        if self.data is not None:
            data = self.data.tolist()
        return json.dumps({
            'name': self.parameter.name,
            'data': data
        })

    def interpolate(self, times):
        try:
            self.data = self.data.astype('f64')
            self.data = griddata(self.times, self.data, times, method='linear')
        except ValueError:
            self.data = self.last_seen(times)

    def last_seen(self, times):
        if len(self.times) == 1:
            return numpy.tile(self.data, len(times))
        time_index = 0
        last = self.data[0]
        next_time = self.times[1]
        new_data = []
        for t in times:
            while t >= next_time:
                time_index += 1
                if time_index+1 < len(self.times):
                    next_time = self.times[time_index+1]
                    last = self.data[time_index]
                else:
                    last = self.data[time_index]
                    break
            new_data.append(last)
        return numpy.array(new_data)


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
            'value': repr(self.value)
        })


class FunctionParameter(DataParameter):
    pass


class StreamRequest(object):
    def __init__(self, subsite, node, sensor, method, stream, parameters):
        self.subsite = subsite
        self.node = node
        self.sensor = sensor
        self.stream = stream
        self.method = method
        self.parameters = parameters
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

    def get_data_map(self):
        parameter_data_map = {}
        for each in self.data + self.functions:
            parameter_data_map[each.parameter.id] = each

        for each in self.coeffs:
            parameter_data_map[each.name] = each

        return parameter_data_map

    def add_parameter(self, p, subsite, node, sensor, stream, method):
        if p.parameter_type.value == FUNCTION:
            self.functions.append(FunctionParameter(subsite, node, sensor, stream, method, p))
        else:
            self.data.append(DataParameter(subsite, node, sensor, stream, method, p))

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
    stream_request = StreamRequest(subsite, node, sensor, method, stream, parameters)
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
                stream_request.functions.append(FunctionParameter(subsite, node, sensor, stream.name, method, parameter))
            else:
                stream_request.data.append(DataParameter(subsite, node, sensor, stream.name, method, parameter))

        else:
            app.logger.debug('NEED PARAMETER FROM OTHER STREAM: %s', parameter.name)
            sensor1, stream1 = find_stream(subsite, node, sensor, method, parameter.streams, distinct_sensors)
            if not any([sensor1 is None, stream1 is None]):
                stream_request.data.append(DataParameter(subsite, node, sensor1, stream1.name, method, parameter))

    return stream_request


@log_timing
def get_distinct_sensors():
    rows = get_session().execute(distinct_sensors_ps)
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
    for row in get_session().execute(metadata_refdes_ps, (subsite, node, sensor, method)):
        if row.stream in stream_map:
            return sensor, stream_map[row.stream]

    # check other reference designators in the same family
    for subsite1, node1, sensor in distinct_sensors:
        if subsite1 == subsite and node1 == node:
            for row in get_session().execute(metadata_refdes_ps, (subsite, node, sensor, method)):
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
    interpolate(stream_request)
    execute_dpas(stream_request, coefficients)
    data = msgpack_all(stream_request, parameters)
    return data


@log_timing
def fetch_data(subsite, node, sensor, stream, method, start, stop, limit=5):
    base_query = "select %%ss from %s where subsite='%s' and node='%s' and method='%s'" % (stream, subsite, node, method)
    # attempt to find one data point beyond the requested start/stop times
    first = get_session().execute(
        'select time from %s where subsite=%%s and node=%%s and sensor=%%s and method=%%s and time<%%s limit 1' % stream,
        (subsite, node, sensor, method, start))
    last = get_session().execute(
        'select time from %s where subsite=%%s and node=%%s and sensor=%%s and method=%%s and time>%%s limit 1' % stream,
        (subsite, node, sensor, method, stop))
    if first:
        start = first[0].time
    if last:
        stop = last[0].time

    query = SimpleStatement(
        'select * from %s where subsite=%%s and node=%%s and sensor=%%s and method=%%s and time>%%s and time<%%s'
        % stream, fetch_size=100)
    app.logger.info('Executing cassandra query: %s', query)
    results = get_session().execute(query, (subsite, node, sensor, method, start, stop))

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
        for index, value in enumerate(row):
            data[index].append(value)
    d = {field: data[i] for i, field in enumerate(fields)}
    app.logger.warn(d)
    return d


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
                    each.dtype = mydata.dtype
                    each.data = mydata
                    each.times = mytime


@log_timing
def interpolate(stream_request):
    """
    Interpolate all data contained in stream_request to the master stream
    :param stream_request:
    :return:
    """
    # first, find times from the primary stream
    times = None
    for each in stream_request.data:
        if stream_request.stream.name == each.stream and each.times is not None:
            times = each.times
            break

    if times is not None:
        # found primary time source, interpolate remaining records
        for each in stream_request.data:
            if stream_request.stream.name != each.stream:
                try:
                    each.interpolate(times)
                except Exception as e:
                    app.logger.warn('%s %s %s', each.parameter.name, each.data, e)

        for each in stream_request.coeffs:
            if each.times is None:
                each.value = numpy.tile(each.value, len(times))


@log_timing
def execute_dpas(stream_request):
    parameter_data_map = stream_request.get_data_map()

    needed = range(len(stream_request.functions))
    for execute_pass in range(5):
        if not needed:
            break
        app.logger.info('Pass %d - attempt to create derived products', execute_pass)

        for index in needed[:]:
            try:
                pf = stream_request.functions[index]
                kwargs = build_func_map(pf, parameter_data_map)
                execute_one_dpa(pf, kwargs)
            except DataUnavailableException:
                # we will never be able to compute this
                needed.remove(index)
                continue
            except DataNotReadyException:
                continue

            needed.remove(index)


@log_timing
def execute_one_dpa(pf, kwargs):
    func = pf.parameter.parameter_function
    func_map = pf.parameter.parameter_function_map

    if len(kwargs) == len(func_map):
        if func.function_type.value == 'PythonFunction':
            module = importlib.import_module(func.owner)
            pf.data = getattr(module, func.function)(**kwargs)
            pf.shape = pf.data.shape
            app.logger.debug('dtype: %s', pf.data.dtype)
        elif func.function_type.value == 'NumexprFunction':
            pf.data = numexpr.evaluate(func.function, kwargs)
            pf.shape = pf.data.shape
            app.logger.debug('dtype: %s', pf.data.dtype)


@log_timing
def build_func_map(parameter_function, data_map):

    func_map = parameter_function.parameter.parameter_function_map
    args = {}
    for key in func_map:
        if func_map[key].startswith('PD'):
            pdid = parse_pdid(func_map[key])

            if pdid not in data_map:
                raise DataUnavailableException(pdid)

            data_item = data_map[pdid]
            if data_item.data is None:
                raise DataNotReadyException(pdid)

            args[key] = data_item.data

        elif func_map[key].startswith('CC'):
            name = func_map[key]
            if name in data_map:
                args[key] = data_map.get(name).value
            else:
                raise CoefficientUnavailableException(name)
    return args


@log_timing
def msgpack_one(item):
    if isinstance(item, DataParameter):
        source = item.stream_key
    else:
        source = 'derived'

    return {
        'data': base64.b64encode(msgpack.packb(item.data.flatten().tolist())),
        'shape': item.data.shape,
        'name': item.parameter.name,
        'source': source
    }


@log_timing
def msgpack_all(stream_request, parameters):
    # TODO, filter based on parameters
    d = {}
    for each in stream_request.data + stream_request.functions:
        d[each.parameter.id] = msgpack_one(each)
    return d


class DataUnavailableException(Exception):
    pass


class DataNotReadyException(Exception):
    pass


class CoefficientUnavailableException(Exception):
    pass