from Queue import Queue
import importlib
import json
import struct
import tempfile
import netCDF4
from threading import Event
import numexpr
import numpy
from werkzeug.exceptions import abort
from engine import app
from collections import OrderedDict
from util.cass import get_streams, fetch_data, get_distinct_sensors
from util.common import log_timing, StreamKey, TimeRange, CachedParameter, UnknownEncodingException, \
    FUNCTION, CoefficientUnavailableException, parse_pdid, UnknownFunctionTypeException, \
    CachedStream, StreamEngineException, StreamUnavailableException, InvalidStreamException
import bisect
from util import common
from util import chunks
from util.chunks import Chunk


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


def get_generator(custom_times=None, custom_type=None):
    if custom_times is None:
        return Chunk_Generator()
    else:
        # Make sure custom_times is strictly increasing and has at least
        # two values
        if len(custom_times) < 2:
            abort(400)
        if any(custom_times[i] >= custom_times[i + 1] for i in range(len(custom_times) - 1)):
            abort(400)

        if custom_type != 'average':
            return Interpolation_Generator(Chunk_Generator())
        else:
            return Average_Generator(Chunk_Generator())


@log_timing
def get_particles(streams, start, stop, coefficients, limit=None, custom_times=None, custom_type=None):
    stream_keys = [StreamKey.from_dict(d) for d in streams]
    parameters = []
    for s in streams:
        for p in s.get('parameters', []):
            parameters.append(CachedParameter.from_id(p))
    time_range = TimeRange(start, stop)
    stream_request = StreamRequest(stream_keys, parameters, coefficients, time_range, limit=limit, times=custom_times)
    return Particle_Generator(get_generator(custom_times, custom_type)).chunks(stream_request)


@log_timing
def get_netcdf(streams, start, stop, coefficients, limit=None, custom_times=None, custom_type=None):
    stream_keys = [StreamKey.from_dict(d) for d in streams]
    parameters = []
    for s in streams:
        for p in s.get('parameters', []):
            parameters.append(CachedParameter.from_id(p))
    time_range = TimeRange(start, stop)
    stream_request = StreamRequest(stream_keys, parameters, coefficients, time_range, limit=limit, times=custom_times)
    return NetCDF_Generator(get_generator(custom_times, custom_type)).chunks(stream_request)


@log_timing
def get_needs(streams):
    stream_keys = [StreamKey.from_dict(d) for d in streams]
    parameters = []
    for s in streams:
        for p in s.get('parameters', []):
            parameters.append(CachedParameter.from_id(p))
    stream_request = StreamRequest(stream_keys, parameters, {}, None, needs_only=True)

    stream_list = []
    for sk in stream_request.stream_keys:
        needs = list(sk.needs_cc)
        d = sk.as_dict()
        d['coefficients'] = needs
        stream_list.append(d)
    return stream_list


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
    times = chunk[7]['data']
    for key in func_map:
        if str(func_map[key]).startswith('PD'):
            pdid = parse_pdid(func_map[key])

            if pdid not in chunk:
                raise StreamEngineException('Internal error: unable to find parameter in stream')

            args[key] = chunk[pdid]['data']

        elif str(func_map[key]).startswith('CC'):
            name = func_map[key]
            if name in coefficients:
                framed_CCs = coefficients[name]
                CC_argument = build_CC_argument(framed_CCs, times)
                if(numpy.isnan(numpy.min(CC_argument))):
                    raise CoefficientUnavailableException('Coefficient %s missing times in range (%s, %s) for parameter %s' % (name, times[0], times[-1], parameter.name))
                else:
                    args[key] = CC_argument
            else:
                raise CoefficientUnavailableException(name)
        elif isinstance(func_map[key], (int, float, long, complex)):
            args[key] = func_map[key]
        else:
            raise StreamEngineException('Unable to resolve parameter \'%s\' in PD%s %s' % (func_map[key], parameter.id, parameter.name))
    return args


def in_range(frame, times):
    """
    Returns boolean masking array for times in range.

      frame is a tuple such that frame[0] is the inclusive start time and
      frame[1] is the exclusive stop time.  None for any of these indices
      indicates unbounded.

      times is a numpy array of ntp times.

      returns a bool numpy array the same shape as times
    """
    if(frame[0] is None and frame[1] is None):
        mask = numpy.ones(times.shape, dtype=bool)
    elif(frame[0] is None):
        mask = (times < frame[1])
    elif(frame[1] is None):
        mask = (times >= frame[0])
    elif(frame[0] == frame[1]):
        mask = (times == frame[0])
    else:
        mask = numpy.logical_and(times >= frame[0], times < frame[1])
    return mask


def build_CC_argument(frames, times):
    sample_value = frames[0]['value']
    if(type(sample_value) == list) :
        cc = numpy.empty(times.shape + numpy.array(sample_value).shape)
    else:
        cc = numpy.empty(times.shape)
    cc[:] = numpy.NAN
    
    frames = [(f.get('start'), f.get('stop'), f['value']) for f in frames]
    frames.sort()

    for frame in frames[::-1]:
        mask = in_range(frame, times)
        cc[mask] = frame[2]

    return cc


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
        self.terminate = False
        self._initialize()

    def _initialize(self):
        needs_cc = set()
        for param in self.stream_key.stream.parameters:
            if not param.parameter_type == FUNCTION:
                self.id_map[param.id] = param.name
            else:
                needs_cc = needs_cc.union(param.needs_cc)
        self.needs_cc = list(needs_cc)

    def provides(self, key):
        return key in self.id_map

    def async_query(self):
        self.future = fetch_data(self.stream_key, self.query_time_range)
        self.future.add_callbacks(callback=self.handle_page, errback=self.handle_error)

    def handle_page(self, rows):
        self.queue.put(rows)
        if self.future.has_more_pages and not self.terminate:
            self.future.start_fetching_next_page()
        else:
            self.finished_event.set()

    def terminate_query(self):
        self.terminate = True

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
                    if p.value_encoding == 'string':
                        temp = [item for sublist in data_slice for item in sublist]
                        data_slice = numpy.array(temp).reshape(shape)
                    else:
                        data_slice = handle_byte_buffer(''.join(data_slice), p.value_encoding, shape)

                nones = numpy.equal(data_slice, None)
                if numpy.any(nones):
                    data_slice[nones] = p.fill_value

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
        times, data = common.stretch(times, data, interp_times)
        times, data = common.interpolate(times, data, interp_times)
        return times, data


class StreamRequest(object):

    def __init__(self, stream_keys, parameters, coefficients, time_range, needs_only=False, limit=None, times=None):
        self.stream_keys = stream_keys
        self.time_range = time_range
        self.parameters = parameters
        self.coefficients = coefficients
        self.needs_cc = None
        self.needs_params = None
        self.limit = limit
        self.times = times
        self._initialize(needs_only)

    def _initialize(self, needs_only):
        if len(self.stream_keys) == 0:
            abort(400)

        # no duplicates allowed
        handled = []
        for key in self.stream_keys:
            if key in handled:
                abort(400)
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
                found = found.union(stream1.parameters)
        found = [p.id for p in found]

        self.needs_params = needs.difference(found)

        needs_cc = set()
        for sk in self.stream_keys:
            needs_cc = needs_cc.union(sk.needs_cc)

        self.needs_cc = needs_cc.difference(self.coefficients.keys())

        # if not needs_only:
        #     self._abort_if_missing_params()

    def _abort_if_missing_params(self):
        if len(self.needs_params) > 0:
            app.logger.error('Unable to find needed parameters: %s', self.needs_params)
            raise StreamUnavailableException('Unable to find a stream to provide needed parameters',
                                             payload={'parameters': list(self.needs_params)})

        if len(self.needs_cc) > 0:
            app.logger.error('Missing calibration coefficients: %s', self.needs_cc)
            raise CoefficientUnavailableException('Missing calibration coefficients',
                                                  payload={'coefficients': list(self.needs_cc)})


class Chunk_Generator(object):

    def __init__(self):
        self.streams = None
        self.parameters = None
        self.coefficients = None
        self.time_range = None

    def _create_data_stream(self, stream_key):
        if stream_key.stream is None:
            raise InvalidStreamException('The requested stream does not exist in preload',
                                         payload={'stream': stream_key.as_dict()})

        return DataStream(stream_key, self.time_range)

    def _query_all(self):
        for stream in self.streams:
            stream.async_query()

    def _terminate_all(self):
        for stream in self.streams:
            stream.terminate_query()

    def _calculate(self, parameter, chunk):
        needs = [CachedParameter.from_id(p) for p in parameter.needs if p not in chunk.keys()]
        if parameter in needs:
            needs.remove(parameter)
        for each in needs:
            # this should descend through any L2 functions to
            # calculate the underlying L1 functions first
            if each.parameter_type == FUNCTION:
                self._calculate(each, chunk)
            # we may have already inserted this during recursion
            if each.id not in chunk:
                self._get_param(each.id, chunk)

        try:
            args = build_func_map(parameter, chunk, self.coefficients)
            chunk[parameter.id] = {'data': execute_dpa(parameter, args), 'source': 'derived'}
        except StreamEngineException:
            pass

    def _get_param(self, pdid, chunk):
        for stream in self.streams[1:]:
            if stream.provides(pdid):
                chunk[pdid] = {
                    'data': stream.get_param_interp(pdid, chunk[7]['data'])[1],
                    'source': stream.stream_key.as_refdes()
                }

    def _execute_dpas_chunk(self, chunk):
        for parameter in self.parameters:
            if parameter.id not in chunk:
                if parameter.parameter_type == FUNCTION:
                    self._calculate(parameter, chunk)
                else:
                    self._get_param(parameter.id, chunk)

    def chunks(self, r):
        self.parameters = r.parameters
        self.coefficients = r.coefficients
        self.time_range = r.time_range
        self.streams = [self._create_data_stream(key) for key in r.stream_keys]

        self._query_all()
        for chunk in self.streams[0].create_generator(None):
            self._execute_dpas_chunk(chunk)
            yield(chunk)


class Particle_Generator(object):

    def __init__(self, generator):
        self.generator = generator

    def chunk_to_particles(self, stream_key, parameters, chunk):
        pk = stream_key.as_dict()

        for index, t in enumerate(chunk[7]['data']):
            particle = OrderedDict()
            particle['pk'] = pk
            pk['time'] = t
            for param in parameters:
                if param.id in chunk:
                    value = chunk[param.id]['data'][index]
                    if type(value) == numpy.ndarray:
                        value = value.tolist()
                    particle[param.name] = value
            yield json.dumps(particle, indent=2)

    def chunks(self, r):
        count = 0
        yield '[ '
        try:
            first = True
            for chunk in self.generator.chunks(r):
                for particle in self.chunk_to_particles(r.stream_keys[0], r.parameters, chunk):
                    count += 1
                    if first:
                        first = False
                    else:
                        yield ', '
                    yield particle
                if r.limit is not None :
                    if r.limit <= count :
                        break
        except Exception as e:
            yield repr(e)
        finally:
            yield ' ]'
        self.generator._terminate_all()


class NetCDF_Generator(object):

    def __init__(self, generator):
        self.generator = generator

    def chunks(self, r):
        with tempfile.NamedTemporaryFile() as tf:
            with netCDF4.Dataset(tf.name, 'w', format='NETCDF4') as ncfile:
                # set up file level attributes
                ncfile.subsite = r.stream_keys[0].subsite
                ncfile.node = r.stream_keys[0].node
                ncfile.sensor = r.stream_keys[0].sensor
                ncfile.collection_method = r.stream_keys[0].method
                ncfile.stream = r.stream_keys[0].stream.name

                # create the time dimension in the root group
                ncfile.createDimension('time', None)
                # create a derived group
                groups = {
                    'derived': ncfile.createGroup('derived')
                }

                variables = {}

                # grab the first chunk of data
                chunk_generator = self.generator.chunks(r)
                first_chunk = chunk_generator.next()

                # sometimes we will get duplicate timestamps
                # INITIAL solution is to remove any duplicate timestamps
                # and the corresponding data.

                # create a mask to match only valid INCREASING times
                # we will insert the last valid timestamp from the previous
                # chunk at the beginning of the array
                chunk_times = first_chunk[7]['data']
                chunk_valid = numpy.diff(numpy.insert(chunk_times, 0, 0.0)) != 0

                # We will need to keep track of the last timestamp of each chunk so
                # that we can apply this logic across chunks
                last_timestamp = chunk_times[chunk_valid][-1]

                # create the netcdf variables and any extra dimensions
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

                    dims = ['time']
                    if len(data.shape) > 1:
                        for index, dimension in enumerate(data.shape[1:]):
                            name = '%s_dim_%d' % (param.name, index)
                            group.createDimension(name, dimension)
                            dims.append(name)

                    variables[param_id] = group.createVariable(param.name,
                                                               data.dtype,
                                                               dims,
                                                               zlib=True)

                    if param.unit is not None:
                        variables[param_id].units = param.unit
                    if param.fill_value is not None:
                        variables[param_id].fill_value = param.fill_value
                    if param.description is not None:
                        variables[param_id].long_name = param.description
                    if param.display_name is not None:
                        variables[param_id].display_name = param.display_name
                    if param.data_product_identifier is not None:
                        variables[param_id].data_product_identifier = param.data_product_identifier

                    variables[param_id][:] = data[chunk_valid]

                # iterate through the chunks and populate the data
                for chunk in chunk_generator:
                    # see comment in first_chunk for explanation of this logic
                    chunk_times = chunk[7]['data']
                    chunk_valid = numpy.diff(numpy.insert(chunk_times, 0, last_timestamp)) != 0
                    last_timestamp = chunk_times[chunk_valid][-1]

                    index = len(variables[7])
                    for param_id in chunk:
                        data = chunk[param_id]['data']
                        variables[param_id][index:] = data[chunk_valid]

            return tf.read()


class Average_Generator(object):
    """
    Generator to average chunks into time bins.
    """

    def __init__(self, generator):
        self.generator = generator

    def chunks(self, r):
        """
        Generator to average chunks into time bins.  Expects a list of times
        to be provided in the request r.  Times is a monotonically increasing
        list of ntp times.  Each time bin i that will be averaged is defined by
        (times[i] inclusively,times[j] exclusively).

        If the underlying generator has no chunks this generator will return
        no chunks.  In the future it may be better to return a empty chunk
        extended to the times specified, but without something to use as a
        template, this code would not know how to generate a chunk compatible
        with the data stream.

        :param r:
        :return:
        """
        times = r.times
        first_chunk = None
        remaining_chunk = None
        for chunk in self.generator.chunks(r):
            chunk = Chunk(chunk)
            if first_chunk is None:
                first_chunk = chunk

            if remaining_chunk:
                chunk = chunks.concatenate(remaining_chunk, chunk)

            # Find bin for the last time in the chunk
            # Find rightmost index of times less than or equal to last_time
            last_time = chunk.times()[-1]
            i = bisect.bisect_right(times, last_time) - 1
            if i >= 0:
                # Find index in chunk that starts the bin identified above and
                # split the chunk into averaging and remaining chunks
                j = numpy.searchsorted(chunk.times(), times[i])
                avg_chunk = chunk[:j]
                remaining_chunk = chunk[j:]
                avg_times = times[:i]
                times = times[i:]

                # Yield the average
                if avg_times:
                    avg_chunk = avg_chunk.with_times(avg_times, strategy='Average')
                    yield avg_chunk.to_dict()

            # Break when done processing last bin
            if len(times) < 2:
                break

        # Average any remaining data
        if (first_chunk or remaining_chunk) and len(times) >= 2:
            if remaining_chunk is None:
                remaining_chunk = first_chunk
            yield remaining_chunk.with_times(times[:-1], strategy='Average').to_dict()

    def _terminate_all(self):
        self.generator._terminate_all()


class Interpolation_Generator(object):
    """
    Generator to interpolate chunks to times.
    """

    def __init__(self, generator):
        self.generator = generator

    def chunks(self, r):
        """
        Generator to interpolate chunks to times.  Expects a list of times
        to be provided in the request r.  Times is a monotonically increasing
        list of ntp times.

        If the underlying generator has no chunks this generator will return
        no chunks.  In the future it may be better to return a empty chunk
        extended to the times specified, but without something to use as a
        template, this code would not know how to generate a chunk compatible
        with the data stream.

        :param r:
        :return:
        """
        times = r.times
        # Save the last data point of chunk i to help interpolate times[0] of
        # iteration j.  Chunk i comes before time[0] of iteration j, but chunk j
        # may come after.
        remaining_chunk = None
        for chunk in self.generator.chunks(r):
            chunk = Chunk(chunk)
            if remaining_chunk:
                chunk = chunks.concatenate(remaining_chunk, chunk)
            remaining_chunk = chunk[-1]

            # Find the times that can be interpolated by this chunk,
            # Find rightmost index of times less than or equal to last_time
            last_time = chunk.times()[-1]
            i = bisect.bisect_right(times, last_time) - 1
            if i >= 0:
                yield chunk.with_times(times[:i+1]).to_dict()
                times = times[i+1:]

            # Break if we have processed all the times.
            if len(times) == 0:
                break

        # Interpolate any times we have left
        if remaining_chunk and len(times) > 0:
            yield remaining_chunk.with_times(times).to_dict()

    def _terminate_all(self):
        self.generator._terminate_all()
