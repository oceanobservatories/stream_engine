import base64
import copy
import importlib
import json
import msgpack
from multiprocessing import Event
from Queue import Queue
import numexpr
import numpy
from scipy.interpolate import griddata
import struct
from werkzeug.exceptions import abort
import engine
from util.cass import fetch_data, get_distinct_sensors, get_streams
from util.common import log_timing, FUNCTION, CoefficientUnavailableException, DataNotReadyException, \
    DataUnavailableException, parse_pdid, UnknownEncodingException, StreamKey, TimeRange, \
    CachedParameter, UnknownFunctionTypeException
from util.streams import StreamRequest2


class PagedResultHandler(object):
    """
    This class handles the results from an asynchronous cassandra query
    As each page of data retrieved it is placed in a Queue. These results
    can be retrieved from this queue from the main thread as they arrive
    or the can be retrieved in bulk with the get_data method.
    """
    def __init__(self, future):
        self.error = None
        self.finished_event = Event()
        self.future = future
        self.queue = Queue()
        self.data = []
        self.fields = []
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_error)

    def handle_page(self, rows):
        """
        Append a single page of data into the queue, request the next page.
        We put the retrieved rows into the queue prior to requesting the next page
        to ensure results are returned in order.
        """
        self.queue.put(rows)
        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
        else:
            self.finished_event.set()

    def handle_error(self, exc):
        """
        Terminate on error, store the error for handling outside this class.
        """
        self.error = exc
        self.finished_event.set()

    def get_chunk(self):
        rows = []
        data = []
        chunk = self.queue.get_nowait()
        if hasattr(chunk, '_asdict'):
            rows.append(chunk)
        else:
            rows.extend(chunk)

        return rows

    def get_data(self):
        """
        Block until all results are retrieved, then return the field names and data.
        """
        rows = []
        # make sure we've finished
        self.finished_event.wait()

        while not self.queue.empty():
            rows.extend(self.get_chunk())

        self.fields = rows[0]._fields
        # for _ in self.fields:
        #     self.data.append([])
        #
        # for row in rows:
        #     for index, value in enumerate(row):
        #         self.data[index].append(value)

        data = numpy.array(rows)
        for i, _ in enumerate(self.fields):
            self.data.append(numpy.array(data[:, i]).tolist())

        return self.fields, self.data


class DataParameter(object):
    def __init__(self, stream_key, parameter):
        self.parameter = parameter  # parameter definition from preload
        self.stream_key = stream_key
        self.data = None
        self.shape = None
        self.times = None
        self.dtype = None

    def __eq__(self, other):
        return self.parameter.id == other.parameter.id

    def __repr__(self):
        return json.dumps({
            'name': self.parameter.name,
            'data': repr(self.data)
        })

    def stretch(self, times):
        """
        If necessary, stretch the edges of this block of data to match the new time series
        """
        temp_data = self.data
        temp_times = self.times
        if times[0] < temp_times[0] or times[-1] > temp_times[-1]:
            temp_data = self.data.tolist()
            if times[0] < temp_times[0]:
                temp_times.insert(0, times[0])
                temp_data.insert(0, temp_data[0])
            if times[-1] > self.times[-1]:
                temp_times.append(times[-1])
                temp_data.append(temp_data[-1])
            temp_data = numpy.array(temp_data)
        return temp_data, temp_times

    def interpolate(self, times):
        if len(self.data) == 1:
            engine.app.logger.warn('One element dataset %s, tiling to new time series', self.parameter.name)
            self.data = numpy.tile(self.data, len(times))
            self.times = times
            return

        temp_data, temp_times = self.stretch(times)
        try:
            temp_data = temp_data.astype('f64')

        except ValueError:
            engine.app.logger.warn('Unable to cast %s to f64 for interpolation, using last seen', self.parameter.name)
            self.data = self.last_seen(times)
            self.times = times
            return

        self.data = griddata(temp_times, temp_data, times, method='linear')
        self.times = times

    def last_seen(self, times):
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
    def __init__(self, stream_key, parameters, coefficients):
        self.stream_key = stream_key
        self.supporting_streams = []
        self.parameters = parameters
        self.data = []
        self.coeffs = []
        self.functions = []
        for each in coefficients:
            self.add_coefficient(each, coefficients[each])

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

    def add_parameter(self, p, stream_key):
        if p.parameter_type == FUNCTION:
            self.functions.append(FunctionParameter(stream_key, p))
        else:
            self.data.append(DataParameter(stream_key, p))

    def add_coefficient(self, name, value):
        self.coeffs.append(CalibrationParameter(self.stream_key.subsite, self.stream_key.node,
                                                self.stream_key.sensor, name, value))

    def __repr__(self):
        return json.dumps({'data': str(self.data),
                           'coeffs': str(self.coeffs),
                           'functions': str(self.functions)})


@log_timing
def find_needed_params(stream_key, parameters, coefficients):
    stream_request = StreamRequest(stream_key, parameters, coefficients)
    needed = []
    needed_cc = []

    if parameters is None or len(parameters) == 0:
        parameters = stream_key.stream.parameters
    else:
        parameters = [p for p in stream_key.stream.parameters if p.id in parameters]

    for parameter in parameters:
        if parameter is not None:
            if parameter.parameter_type == FUNCTION:
                needed.extend([CachedParameter.from_id(pdid) for pdid in parameter.needs])
                needed_cc.extend(parameter.needs_cc)
            else:
                needed.append(parameter)

    needed = set(needed)
    distinct_sensors = get_distinct_sensors()

    for parameter in needed:
        if parameter in stream_key.stream.parameters:
            stream_request.add_parameter(parameter, stream_key)

        else:
            engine.app.logger.debug('NEED PARAMETER FROM OTHER STREAM: %s', parameter.name)
            sensor1, stream1 = find_stream(stream_key, parameter.streams, distinct_sensors)
            if not any([sensor1 is None, stream1 is None]):
                new_stream_key = StreamKey(stream_key.subsite, stream_key.node, sensor1,
                                           stream_key.method, stream1.name)
                stream_request.data.append(DataParameter(new_stream_key, parameter))

    return stream_request


def find_stream(stream_key, streams, distinct_sensors):
    """
    Attempt to find a "related" sensor which provides one of these streams
    :param stream_key
    :return:
    """
    stream_map = {s.name: s for s in streams}

    # check our specific reference designator first
    for row in get_streams(stream_key.subsite, stream_key.node, stream_key.sensor, stream_key.method):
        if row.stream in stream_map:
            return stream_key.sensor, stream_map[row.stream]

    # check other reference designators in the same family
    for subsite1, node1, sensor in distinct_sensors:
        if subsite1 == stream_key.subsite and node1 == stream_key.node:
            for row in get_streams(stream_key.subsite, stream_key.node, sensor, stream_key.method):
                if row.stream in stream_map:
                    return sensor, stream_map[row.stream]

    return None, None


@log_timing
def calculate(request, start, stop, coefficients, particles=False):
    subsite = request.get('subsite')
    node = request.get('node')
    sensor = request.get('sensor')
    stream = request.get('stream')
    method = request.get('method')
    parameters = request.get('parameters', [])
    if any([subsite is None,
            node is None,
            sensor is None,
            stream is None,
            method is None]):
        abort(400)
    if particles:
        data = get_particles(subsite, node, sensor, stream, method, parameters, start, stop, coefficients)
    else:
        data = get_stream(subsite, node, sensor, stream, method, parameters, start, stop, coefficients)
    return json.dumps(data, indent=2)


@log_timing
def get_stream(subsite, node, sensor, stream, method, parameters, start, stop, coefficients):
    stream_key = StreamKey(subsite, node, sensor, method, stream)
    stream_request = find_needed_params(stream_key, parameters, coefficients)
    get_all_data(stream_request, start, stop)
    interpolate(stream_request)
    execute_dpas(stream_request)
    data = msgpack_all(stream_request, parameters)
    return data


@log_timing
def get_particles(streams, start, stop, coefficients):
    stream_keys = [StreamKey.from_dict(d) for d in streams]
    parameters = []
    for s in streams:
        for p in s.get('parameters', []):
            parameters.append(CachedParameter.from_id(p))
    time_range = TimeRange(start, stop)
    stream_request = StreamRequest2(stream_keys, parameters, coefficients, time_range)
    return stream_request.particle_generator()


@log_timing
def handle_bytebuffer(data, encoding, shape):
    if encoding in ['int8', 'int16', 'int32', 'uint8', 'uint16']:
        format_string = 'i'
        count = len(data) / 4
    elif encoding in ['uint32', 'int64']:
        format_string = 'l'
        count = len(data) / 8
    elif 'float' in encoding:
        format_string = 'd'
        count = len(data) / 8
    else:
        engine.app.log.error('Unknown encoding: %s', encoding)
        raise UnknownEncodingException()

    data = numpy.array(struct.unpack('>%d%s' % (count, format_string), data))
    data = data.reshape(shape)
    return data


@log_timing
def get_data(stream_key, time_range):
    future = fetch_data(stream_key, time_range)
    handler = PagedResultHandler(future)
    return handler


@log_timing
def fill_stream_request(stream_key, stream_request, fields, data):
    mytime = data[fields.index('time')]

    for each in stream_request.data:
        if each.stream_key == stream_key:
            # this stream contains this data, fetch it
            index = fields.index(each.parameter.name.lower())
            mydata = data[index]

            shape_name = each.parameter.name + '_shape'
            if shape_name in fields:
                shape_index = fields.index(each.parameter.name + '_shape')
                shape = [len(mytime)] + data[shape_index][0]
                encoding = each.parameter.value_encoding.value
                mydata = ''.join(mydata)
                mydata = handle_bytebuffer(mydata, encoding, shape)

            else:
                mydata = numpy.array(mydata)
            each.dtype = mydata.dtype
            each.data = mydata
            each.times = copy.copy(mytime)


@log_timing
def get_all_data(stream_request, start, stop):
    needed_streams = {each.stream_key for each in stream_request.data}
    handlers = {}
    for stream_key in needed_streams:
        time_range = TimeRange(start, stop)
        handlers[stream_key] = get_data(stream_key, time_range)

    for stream_key in handlers:
        fields, data = handlers[stream_key].get_data()
        fill_stream_request(stream_key, stream_request, fields, data)


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
        if stream_request.stream_key == each.stream_key and each.times is not None:
            times = each.times
            break

    if times is not None:
        # found primary time source, interpolate remaining records
        for each in stream_request.data:
            if stream_request.stream_key != each.stream_key:
                each.interpolate(times)

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
        engine.app.logger.info('Pass %d - attempt to create derived products', execute_pass)

        for index in needed[:]:
            try:
                pf = stream_request.functions[index]
                kwargs = build_func_map(pf, parameter_data_map)
                execute_one_dpa(pf, kwargs)
            except DataUnavailableException:
                # we will never be able to compute this
                needed.remove(index)
                continue
            except CoefficientUnavailableException as e:
                engine.app.logger.error('Unable to generate data product, missing CC: %s', e)
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
        if func.function_type == 'PythonFunction':
            module = importlib.import_module(func.owner)
            pf.data = getattr(module, func.function)(**kwargs)
        elif func.function_type == 'NumexprFunction':
            pf.data = numexpr.evaluate(func.function, kwargs)
        else:
            raise UnknownFunctionTypeException(func.function_type)
        pf.dtype = pf.data.dtype
        pf.shape = pf.data.shape


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
        source = str(item.stream_key)
    else:
        source = 'derived'

    return {
        'data': base64.b64encode(msgpack.packb(item.data.flatten().tolist())),
        'dtype': item.dtype.str,
        'shape': item.data.shape,
        'name': item.parameter.name,
        'source': source
    }


@log_timing
def msgpack_all(stream_request, parameters):
    # TODO, filter based on parameters
    d = {}
    for each in stream_request.data + stream_request.functions:
        if each.data is None:
            continue
        if not parameters or each.parameter.id in parameters:
            d[each.parameter.id] = msgpack_one(each)
    return d