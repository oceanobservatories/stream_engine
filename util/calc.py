from Queue import Queue
import importlib
import json
import struct
import tempfile
import netCDF4
from threading import Event
import numexpr
import numpy
from scipy.interpolate import griddata
from werkzeug.exceptions import abort
from engine import app
from collections import OrderedDict
from util.cass import get_streams, fetch_data, get_distinct_sensors
from util.common import log_timing, StreamKey, TimeRange, CachedParameter, UnknownEncodingException, \
    FUNCTION, CoefficientUnavailableException, parse_pdid, UnknownFunctionTypeException, \
    CachedStream, StreamEngineException, StreamUnavailableException, InvalidStreamException


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
def get_particles(streams, start, stop, coefficients):
    stream_keys = [StreamKey.from_dict(d) for d in streams]
    parameters = []
    for s in streams:
        for p in s.get('parameters', []):
            parameters.append(CachedParameter.from_id(p))
    time_range = TimeRange(start, stop)
    stream_request = StreamRequest(stream_keys, parameters, coefficients, time_range)
    return stream_request.particle_generator()


@log_timing
def get_netcdf(streams, start, stop, coefficients):
    stream_keys = [StreamKey.from_dict(d) for d in streams]
    parameters = []
    for s in streams:
        for p in s.get('parameters', []):
            parameters.append(CachedParameter.from_id(p))
    time_range = TimeRange(start, stop)
    stream_request = StreamRequest(stream_keys, parameters, coefficients, time_range)
    return stream_request.netcdf_generator()


@log_timing
def get_needs(streams):
    stream_keys = [StreamKey.from_dict(d) for d in streams]
    parameters = []
    for s in streams:
        for p in s.get('parameters', []):
            parameters.append(CachedParameter.from_id(p))
    #time_range = TimeRange(0, 1)
    stream_request = StreamRequest(stream_keys, parameters, [], None)

    stream_list = []
    for stream in stream_request.streams:
        sk = stream.stream_key
        needs = stream.needs_cc
        d = sk.as_dict()
        d['coefficients'] = needs
        stream_list.append(d)
    return stream_list


def stretch(times, data, interp_times):
    if len(times) == 1:
        return interp_times, data * len(interp_times)
    if interp_times[0] < times[0]:
        times.insert(0, interp_times[0])
        data.insert(0, data[0])
    if interp_times[-1] > times[-1]:
        times.append(interp_times[-1])
        data.append(data[-1])
    return times, data


def interpolate(times, data, interp_times):
    data = numpy.array(data)

    if numpy.array_equal(times, interp_times):
        return times, data
    try:
        # data = data.astype('f64')
        data = griddata(times, data, interp_times, method='linear')
    except ValueError:
        data = last_seen(times, data, interp_times)
    return interp_times, data


def last_seen(times, data, interp_times):
    time_index = 0
    last = data[0]
    next_time = times[1]
    new_data = []
    for t in interp_times:
        while t >= next_time:
            time_index += 1
            if time_index + 1 < len(times):
                next_time = times[time_index + 1]
                last = data[time_index]
            else:
                last = data[time_index]
                break
        new_data.append(last)
    return numpy.array(new_data)


def handle_byte_buffer(data, encoding, shape):
    if encoding in ['int8', 'int16', 'int32', 'uint8', 'uint16']:
        format_string = 'i'
        count = len(data) / 4
    elif encoding in ['uint32', 'int64']:
        format_string = 'q'
        count = len(data) / 8
    elif 'float' in encoding:
        format_string = 'd'
        count = len(data) / 8
    else:
        raise UnknownEncodingException(encoding)

    data = numpy.array(struct.unpack('>%d%s' % (count, format_string), data))
    data = data.reshape(shape)
    return data


def execute_dpa(parameter, kwargs):
    func = parameter.parameter_function
    func_map = parameter.parameter_function_map

    if len(kwargs) == len(func_map):
        if func.function_type == 'PythonFunction':
            module = importlib.import_module(func.owner)
            result = getattr(module, func.function)(**kwargs)
        elif func.function_type == 'NumexprFunction':
            result = numexpr.evaluate(func.function, kwargs)
        else:
            raise UnknownFunctionTypeException(func.function_type)
        return result


def build_func_map(parameter, chunk, coefficients):
    func_map = parameter.parameter_function_map
    args = {}
    data_length = len(chunk[7]['data'])
    for key in func_map:
        if func_map[key].startswith('PD'):
            pdid = parse_pdid(func_map[key])

            if pdid not in chunk:
                raise StreamEngineException('Internal error: unable to find parameter in stream')

            args[key] = chunk[pdid]['data']

        elif func_map[key].startswith('CC'):
            name = func_map[key]
            if name in coefficients:
                value = coefficients[name]
                if type(value) == list:
                    data = numpy.array(value)
                    shape = [data_length] + [1 for _ in data.shape]
                    args[key] = numpy.tile(data, shape)
                else:
                    args[key] = numpy.tile(value, data_length)
            else:
                raise CoefficientUnavailableException(name)
    return args


class DataStream(object):
    def __init__(self, stream_key, time_range):
        self.stream_key = stream_key
        self.query_time_range = time_range
        self.available_time_range = TimeRange(0, 0)
        self.future = None
        self.row_cache = []
        self.queue = Queue()
        self.finished_event = Event()
        self.error = None
        self.data_cache = {}
        self.id_map = {}
        self.param_map = {}
        self.func_params = []
        self.times = []
        self.needs_cc = []
        self._initialize()

    def _initialize(self):
        needs_cc = set()
        for param in self.stream_key.stream.parameters:
            if not param.parameter_type == FUNCTION:
                self.id_map[param.id] = param.name
            else:
                needs_cc = needs_cc.union(param.needs_cc)
        self.needs_cc = list(needs_cc)

    def async_query(self):
        self.future = fetch_data(self.stream_key, self.query_time_range)
        self.future.add_callbacks(callback=self.handle_page, errback=self.handle_error)

    def handle_page(self, rows):
        self.queue.put(rows)
        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
        else:
            self.finished_event.set()

    def handle_error(self, exc):
        self.error = exc
        self.finished_event.set()

    def _get_chunk(self):
        chunk = self.queue.get()
        if hasattr(chunk, '_asdict'):
            self.row_cache.append(chunk)
        else:
            self.row_cache.extend(chunk)

        self.available_time_range.start = self.row_cache[0].time
        self.available_time_range.stop = self.row_cache[-1].time

    def create_generator(self, parameters):
        """
        generator to return the data from a single chunk
        dropping the previous row cache each cycle
        this is the preferred method of retrieving data
        from the primary stream
        """
        if parameters is None or len(parameters) == 0:
            parameters = [p for p in self.stream_key.stream.parameters if p.parameter_type != FUNCTION]

        while True:
            if self.queue.empty() and self.finished_event.is_set():
                raise StopIteration()

            source = self.stream_key.as_refdes()

            self.row_cache = []
            self.data_cache = {p.id: [] for p in parameters}
            self._get_chunk()
            self.data_cache[7] = {
                'data': [],
                'source': source
            }

            if len(self.row_cache) == 0:
                raise StopIteration()

            fields = self.row_cache[0]._fields
            array = numpy.array(self.row_cache)

            for p in parameters:
                index = fields.index(p.name.lower())
                data_slice = array[:, index]
                shape_name = p.name + '_shape'
                if shape_name in fields:
                    shape = [len(array)] + array[0, fields.index(shape_name)]
                    data_slice = handle_byte_buffer(''.join(data_slice), p.value_encoding, shape)
                data_slice = numpy.array(data_slice.tolist())
                self.data_cache[p.id] = {
                    'data': data_slice,
                    'source': source
                }

            yield self.data_cache

    def get_param(self, pdid, time_range):
        if pdid not in self.id_map:
            raise StreamEngineException('Internal error: unable to find parameter in stream')

        if not all([self.queue.empty(), self.finished_event.is_set()]):
            while time_range.stop >= self.available_time_range.stop:
                if self.queue.empty() and self.finished_event.is_set():
                    break
                # grabbing new data, invalidate the old cache
                self.data_cache = {}
                self._get_chunk()

        if pdid not in self.data_cache:
            self._fill_cache(pdid)

        # copy the data in case of interpolation
        return self.data_cache[7]['data'][:], self.data_cache[pdid]['data'][:]

    def _fill_cache(self, pdid):
        name = self.id_map[pdid]
        if 7 not in self.data_cache:
            self.data_cache[7] = {'data': [], 'source': self.stream_key.as_refdes()}
            for row in self.row_cache:
                self.data_cache[7]['data'].append(row.time)

        self.data_cache[pdid] = {'data': [], 'source': self.stream_key.as_refdes()}
        for row in self.row_cache:
            if hasattr(row, name):
                item = getattr(row, name)
                if hasattr(row, name + '_shape'):
                    shape = getattr(row, name + '_shape')
                    item = handle_byte_buffer(item, self.param_map[name].value_encoding, shape)
                self.data_cache[pdid]['data'].append(item)
            else:
                self.data_cache[pdid]['data'].append(None)

    def get_param_interp(self, pdid, interp_times):
        times, data = self.get_param(pdid, TimeRange(interp_times[0], interp_times[-1]))
        times, data = stretch(times, data, interp_times)
        times, data = interpolate(times, data, interp_times)
        return times, data


class StreamRequest(object):
    def __init__(self, stream_keys, parameters, coefficients, time_range):
        self.stream_keys = stream_keys
        self.time_range = time_range
        self.parameters = parameters
        self.coefficients = coefficients
        self.streams = []
        self._initialize()

    def _initialize(self):
        if len(self.stream_keys) == 0:
            abort(400)

        # no duplicates allowed
        handled = []
        for key in self.stream_keys:
            if key in handled:
                abort(400)
            self.streams.append(self._create_data_stream(key))
            handled.append(key)

        # populate self.parameters if empty or None
        if self.parameters is None or len(self.parameters) == 0:
            self.parameters = set()
            for each in self.stream_keys:
                self.parameters = self.parameters.union(each.stream.parameters)

        # sort parameters by name for particle output
        params = [(p.name, p) for p in self.parameters]
        params.sort()
        self.parameters = [p[1] for p in params]

        # determine if any other parameters are needed
        distinct_sensors = get_distinct_sensors()
        needs = set()
        for each in self.parameters:
            if each.parameter_type == FUNCTION:
                needs = needs.union([p for p in each.needs if p not in self.parameters])

        # available in the specified streams?
        provided = []
        for stream_key in self.stream_keys:
            provided.extend([p.id for p in stream_key.stream.parameters])

        needs = needs.difference(provided)

        # find the available streams which provide any needed parameters
        found = set()
        for each in needs:
            each = CachedParameter.from_id(each)
            if each in found:
                continue
            streams = [CachedStream.from_id(sid) for sid in each.streams]
            sensor1, stream1 = find_stream(self.stream_keys[0], streams, distinct_sensors)
            if not any([sensor1 is None, stream1 is None]):
                new_stream_key = StreamKey.from_stream_key(self.stream_keys[0], sensor1, stream1.name)
                self.stream_keys.append(new_stream_key)
                self.streams.append(self._create_data_stream(new_stream_key))
                found = found.union(stream1.parameters)
        found = [p.id for p in found]

        if len(needs.difference(found)) > 0:
            app.logger.error('Unable to find needed parameters: %s', needs.difference(found))
            raise StreamUnavailableException('Unable to find a stream to provide needed parameters',
                                             payload={'parameters': list(needs.difference(found))})

        needs_cc = set()
        for stream in self.streams:
            needs_cc = needs_cc.union(stream.needs_cc)

        needs_cc = needs_cc.difference(self.coefficients.keys())
        if len(needs_cc) > 0:
            app.logger.error('Missing calibration coefficients: %s', needs_cc)
            raise CoefficientUnavailableException('Missing calibration coefficients',
                                                  payload={'coefficients': list(needs_cc)})

    def _create_data_stream(self, stream_key):
        if stream_key.stream is None:
            raise InvalidStreamException('The requested stream does not exist in preload',
                                         payload={'stream': stream_key.as_dict()})

        return DataStream(stream_key, self.time_range)

    def _query_all(self):
        for stream in self.streams:
            stream.async_query()

    def _calculate(self, parameter, chunk):
        needs = [CachedParameter.from_id(p) for p in parameter.needs if p not in chunk.keys()]
        if parameter in needs:
            needs.remove(parameter)
        for each in needs:
            # this should descend through any L2 functions to
            # calculate the underlying L1 functions first
            if each.parameter_type == FUNCTION:
                self._calculate(each, chunk)
            for stream in self.streams[1:]:
                # we may have already inserted this during recursion
                if each.id not in chunk:
                    times, data = stream.get_param_interp(each.id, chunk[7]['data'])
                    chunk[each.id] = {
                        'data': data,
                        'source': stream.stream_key.as_refdes()
                    }


        args = build_func_map(parameter, chunk, self.coefficients)
        chunk[parameter.id] = {'data': execute_dpa(parameter, args), 'source': 'derived'}

    def chunk_to_particles(self, chunk):
        pk = self.stream_keys[0].as_dict()

        for index, t in enumerate(chunk[7]['data']):
            particle = OrderedDict()
            particle['pk'] = pk
            pk['time'] = t
            for param in self.parameters:
                value = chunk[param.id]['data'][index]
                if type(value) == numpy.ndarray:
                    value = value.tolist()
                particle[param.name] = value
            yield json.dumps(particle, indent=2)

    def _execute_dpas_chunk(self, chunk):
        for parameter in self.parameters:
            if parameter.id not in chunk:
                if parameter.parameter_type == FUNCTION:
                    self._calculate(parameter, chunk)
                else:
                    for stream in self.streams[1:]:
                        chunk[parameter.id] = {
                            'data': stream.get_param_interp(parameter.id, chunk[7]['data'])[1],
                            'source': stream.stream_key.as_refdes()
                        }

    def particle_generator(self):
        # plan of attack
        # start queries
        # fetch chunk from primary stream
        # retrieve times for chunk
        # fetch chunk from each secondary stream until time parity reached or chunks exhausted
        # retrieve raw data from primary stream, interpolated data from secondary streams
        # calculate derived products
        # yield one or more particles
        self._query_all()
        yield '[ '
        first = True
        for chunk in self.streams[0].create_generator(None):
            self._execute_dpas_chunk(chunk)
            for particle in self.chunk_to_particles(chunk):
                if first:
                    first = False
                else:
                    yield ', '
                yield particle
        yield ' ]'


    def netcdf_generator(self):
        self._query_all()
        with tempfile.NamedTemporaryFile() as tf:
            with netCDF4.Dataset(tf.name, 'w', format='NETCDF4') as ncfile:
                ncfile.subsite = self.stream_keys[0].subsite
                ncfile.node = self.stream_keys[0].node
                ncfile.sensor = self.stream_keys[0].sensor
                ncfile.collection_method = self.stream_keys[0].method
                ncfile.stream = self.stream_keys[0].stream.name

                time_dim = ncfile.createDimension('time', None)
                groups = {
                    'derived': ncfile.createGroup('derived')
                }

                variables = {}
                chunk_generator = self.streams[0].create_generator(None)
                first_chunk = chunk_generator.next()
                chunk_size = len(first_chunk[7]['data'])
                self._execute_dpas_chunk(first_chunk)
                for param_id in first_chunk:

                    param = CachedParameter.from_id(param_id)
                    data = first_chunk[param_id]['data']
                    source = first_chunk[param_id]['source']
                    if param_id == 7:
                        group = ncfile
                    elif param.parameter_type == FUNCTION:
                        group = groups['derived']
                    else:
                        if source not in groups:
                            groups[source] = ncfile.createGroup(source)
                        group = groups[source]

                    if len(data.shape) == 1:
                        variables[param_id] = group.createVariable(param.name,
                                                                   data.dtype,
                                                                   ('time',),
                                                                   zlib=True)
                    else:
                        dims = ['time']
                        for index, dimension in enumerate(data.shape[1:]):
                            name = '%s_dim_%d' % (param.name, index)
                            group.createDimension(name, dimension)
                            dims.append(name)
                        variables[param_id] = group.createVariable(param.name,
                                                                   data.dtype,
                                                                   dims,
                                                                   zlib=True)

                    variables[param_id].units = param.unit
                    if param.description is not None:
                        variables[param_id].long_name = param.description
                    if param.fill_value is not None:
                        variables[param_id].fill_value = param.fill_value
                    if param.display_name is not None:
                        variables[param_id].display_name = param.display_name
                    if param.data_product_identifier is not None:
                        variables[param_id].data_product_identifier = param.data_product_identifier

                    variables[param_id][:] = data

                for index, chunk in enumerate(chunk_generator):
                    index = (index+1) * chunk_size
                    self._execute_dpas_chunk(chunk)
                    for param_id in chunk:
                        data = chunk[param_id]['data']
                        variables[param_id][index:] = data

            return tf.read()

