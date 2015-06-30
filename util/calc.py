from Queue import Queue, Empty
import importlib
import msgpack
import json
import struct
import tempfile
import netCDF4
from threading import Event
import traceback
import numexpr
import numpy
import time
from werkzeug.exceptions import abort
from engine import app
from collections import OrderedDict, namedtuple
from util.cass import get_streams, fetch_data, get_distinct_sensors, fetch_nth_data, get_available_time_range
from util.common import log_timing, StreamKey, TimeRange, CachedParameter, UnknownEncodingException, \
    FUNCTION, CoefficientUnavailableException, parse_pdid, UnknownFunctionTypeException, \
    CachedStream, StreamEngineException, StreamUnavailableException, InvalidStreamException, CachedFunction
import bisect
from util import common
from util import chunks
from util.chunks import Chunk
import pandas as pd
import logging
import uuid
from parameter_util import PDRef

log = logging.getLogger(__name__)

def find_stream(stream_key, streams, distinct_sensors):
    """
    Attempt to find a "related" sensor which provides one of these streams
    :param stream_key
    :return:
    """
    stream_map = {s.name: s for s in streams}

    # check our specific reference designator first
    for stream in get_streams(stream_key.subsite, stream_key.node, stream_key.sensor, stream_key.method):
        if stream in stream_map:
            return stream_key.sensor, stream_map[stream]

    # check other reference designators in the same family
    for subsite1, node1, sensor in distinct_sensors:
        if subsite1 == stream_key.subsite and node1 == stream_key.node:
            for stream in get_streams(stream_key.subsite, stream_key.node, sensor, stream_key.method):
                if stream in stream_map:
                    return sensor, stream_map[stream]

    return None, None


def get_generator(time_range, custom_times=None, custom_type=None):
    if custom_type is None:
        return Chunk_Generator()
    else:
        # Make sure custom_times is strictly increasing and has at least
        # two values
        if len(custom_times) < 2:
            abort(400)
        if any(numpy.diff(custom_times) <= 0):
            abort(400)

        if custom_type != 'average':
            return Interpolation_Generator(Chunk_Generator())
        else:
            if custom_times[-1] < time_range.stop:
                custom_times.append(time_range.stop)
            return Average_Generator(Chunk_Generator())


@log_timing
def get_particles(streams, start, stop, coefficients, qc_parameters, limit=None, custom_times=None, custom_type=None):
    stream_keys = [StreamKey.from_dict(d) for d in streams]
    parameters = []
    for s in streams:
        for p in s.get('parameters', []):
            parameters.append(CachedParameter.from_id(p))
    time_range = TimeRange(start, stop)

    qc_stream_parameters = {}
    for qc_parameter in qc_parameters:
        qc_pk = qc_parameter.get('qcParameterPK')
        current_stream_parameter = qc_pk.get('streamParameter').encode('ascii', 'ignore')
        qc_ids = qc_stream_parameters.get(current_stream_parameter)
        if qc_ids is None:
            qc_ids = {}
            qc_stream_parameters[current_stream_parameter] = qc_ids
        current_qc_id = qc_pk.get('qcId').encode('ascii', 'ignore')
        parameter_dict = qc_ids.get(current_qc_id)
        if parameter_dict is None:
            parameter_dict = {}
            qc_ids[current_qc_id] = parameter_dict
        current_parameter_name = qc_pk.get('parameter').encode('ascii', 'ignore')
        if current_parameter_name is not None:
            qc_parameter_value = qc_parameter.get('value').encode('ascii', 'ignore')
            if qc_parameter.get('valueType').encode('ascii', 'ignore') == 'INT':
                parameter_dict[current_parameter_name] = int(qc_parameter_value)
            elif qc_parameter.get('valueType').encode('ascii', 'ignore') == 'FLOAT':
                parameter_dict[current_parameter_name] = float(qc_parameter_value)
            else:
                parameter_dict[current_parameter_name] = qc_parameter_value

    # limit defined by user without custom times
    # we're going to return every nth point unless the dataset is sufficiently small
    if custom_times is None and limit is not None:
        custom_times = numpy.linspace(start, stop, num=limit)

    stream_request = StreamRequest(stream_keys, parameters, coefficients, time_range, qc_parameters=qc_stream_parameters, limit=limit, times=custom_times)
    return Particle_Generator(get_generator(time_range, custom_times, custom_type)).chunks(stream_request)


@log_timing
def get_netcdf(streams, start, stop, coefficients, limit=None, custom_times=None, custom_type=None):
    stream_keys = [StreamKey.from_dict(d) for d in streams]
    parameters = []
    for s in streams:
        for p in s.get('parameters', []):
            parameters.append(CachedParameter.from_id(p))
    time_range = TimeRange(start, stop)
    stream_request = StreamRequest(stream_keys, parameters, coefficients, time_range, limit=limit, times=custom_times)
    return NetCDF_Generator(get_generator(time_range, custom_times, custom_type)).chunks(stream_request)


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


def handle_byte_buffer(data):
    return numpy.array([msgpack.unpackb(x) for x in data])


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
        if PDRef.is_pdref(func_map[key]):
            pdRef = PDRef.from_str(func_map[key])

            if pdRef.chunk_key not in chunk:
                raise StreamEngineException('Needed parameter %s not found in chunk when calculating PD%s %s' % (func_map[key], parameter.id, parameter.name))

            args[key] = chunk[pdRef.chunk_key]['data']

        elif str(func_map[key]).startswith('CC'):
            name = func_map[key]
            if name in coefficients:
                framed_CCs = coefficients[name]
                CC_argument = build_CC_argument(framed_CCs, times)
                if(numpy.isnan(numpy.min(CC_argument))):
                    raise CoefficientUnavailableException('Coefficient %s missing times in range (%s, %s) for PD%s %s' % (name, times[0], times[-1], parameter.id, parameter.name))
                else:
                    args[key] = CC_argument
            else:
                raise CoefficientUnavailableException('Coefficient %s not provided for PD%s %s' % (name, parameter.id, parameter.name))
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
    frames = [(f.get('start'), f.get('stop'), f['value']) for f in frames]
    frames.sort()
    frames = [f for f in frames if any(in_range(f, times))]

    sample_value = frames[0][2]
    if(type(sample_value) == list) :
        cc = numpy.empty(times.shape + numpy.array(sample_value).shape)
    else:
        cc = numpy.empty(times.shape)
    cc[:] = numpy.NAN
    
    for frame in frames[::-1]:
        mask = in_range(frame, times)
        cc[mask] = frame[2]

    return cc


class DataStream(object):
    def __init__(self, stream_key, time_range, limit=None):
        self.stream_key = stream_key
        self.query_time_range = time_range
        self.strict_range = False
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
        self.cols = None
        self.terminate = False
        self.limit = limit
        self.nf_parameters = None
        self._initialize()

    def _initialize(self):
        self.nf_parameters = [p for p in self.stream_key.stream.parameters if p.parameter_type != FUNCTION]

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
        if self.limit is not None:
            self.cols, self.future = fetch_nth_data(self.stream_key, self.query_time_range, strict_range=self.strict_range, num_points=self.limit)
        else:
            self.cols, self.future = fetch_data(self.stream_key, self.query_time_range, strict_range=self.strict_range)
        self.future.add_callbacks(callback=self.handle_page, errback=self.handle_error)

    def handle_page(self, rows):
        if rows:
            Row = namedtuple('Row', self.cols, rename=True)
            rows = [Row(*row) for row in rows]
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
        # try/except because we can reach this code occasionally
        # when the query is complete but the complete flag hasn't
        # been set.
        try:
            chunk = self.queue.get_nowait()
            if hasattr(chunk, '_asdict'):
                self.row_cache.append(chunk)
            else:
                self.row_cache.extend(chunk)

            self.available_time_range.start = self.row_cache[0].time
            self.available_time_range.stop = self.row_cache[-1].time
        except Empty:
            time.sleep(.001)

    def create_generator(self):
        """
        generator to return the data from a single chunk
        dropping the previous row cache each cycle
        this is the preferred method of retrieving data
        from the primary stream
        """
        while True:
            if self.queue.empty() and self.finished_event.is_set():
                raise StopIteration()

            self.row_cache = []
            self._get_chunk()

            if len(self.row_cache) == 0:
                continue

            fields = self.row_cache[0]._fields
            df = pd.DataFrame(self.row_cache, columns=fields)

            deployments = df.groupby('deployment')
            for dep_num, data_frame in deployments:
                self.populate_data_cache(data_frame)
                yield self.data_cache

    def populate_data_cache(self, df):
        source = self.stream_key.as_refdes()

        self.data_cache = {}

        # special cases for deployment number and provenance key
        self.data_cache['deployment'] = {
            'data': df.deployment.values,
            'source': source
        }

        self.data_cache['provenance'] = {
            'data': df.provenance.values.astype('str'),
            'source': source
        }

        for p in self.nf_parameters:
            data_slice = df[p.name].values
            shape_name = p.name + '_shape'
            if shape_name in df:
                shape = [len(data_slice)] + df[shape_name].iloc[0]
                if p.value_encoding == 'string':
                    temp = [item for sublist in data_slice for item in sublist]
                    data_slice = numpy.array(temp).reshape(shape)
                else:
                    data_slice = handle_byte_buffer(data_slice)

            # Nones can only be in ndarrays with dtype == object.  NetCDF
            # doesn't like objects.  First replace Nones with the
            # appropriate fill value.
            # pandas does some funny things to missing values if the whole column is missing it becomes a None filled object
            # Otherwise pandas will replace floats with Not A Number correctly.
            # Integers are cast as floats and missing values replaced with Not A Number
            # The below case will take care of instances where the whole series is missing or if it is an array or
            # some other object we don't know how to fill.
            if data_slice.dtype == 'object':
                nones = numpy.equal(data_slice, None)
                if numpy.any(nones):
                    # If there are nones either fill with specific vlaue for ints, floats, string, or throw an error
                    if p.value_encoding in ['int', 'uint8', 'uint16', 'uint32', 'uint64', 'int8', 'int16', 'int32', 'int64']:
                        data_slice[nones] =  -999999999
                        data_slice = data_slice.astype('int64')
                    elif p.value_encoding in ['float16', 'float32', 'float64', 'float96']:
                        data_slice[nones] = numpy.nan
                        data_slice = data_slice.astype('float64')
                    elif p.value_encoding == 'string':
                        data_slice[nones] = ''
                        data_slice = data_slice.astype('str')
                    else:
                        log.error("Do not know how to fill type: {:s}".format(p.value_encoding))
                        raise StreamEngineException('Do Not Know how to fill for data type ' + str(p.value_encoding))
            #otherwise if the returned data is a float we need to check and make sure it is not supposed to be an int
            elif data_slice.dtype == 'float64':
                #Int's are upcast to floats if there is a missing value.
                if p.value_encoding in ['int', 'uint8', 'uint16', 'uint32', 'uint64', 'int8', 'int16', 'int32', 'int64']:
                    # We had a missing value because it was upcast
                    indexes = numpy.where(numpy.isnan(data_slice))
                    data_slice[indexes] = -999999999
                    data_slice = data_slice.astype('int64')

            # Pandas also treats strings as objects.  NetCDF doesn't
            # like objects.  So convert objects to strings.
            # if it is still an object we need to convert it here as a final catch all
            if data_slice.dtype == object:
                data_slice = data_slice.astype('str')

            self.data_cache[p.id] = {
                'data': data_slice,
                'source': source
            }

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
                    item = handle_byte_buffer([item])
                self.data_cache[pdid]['data'].append(item)
            else:
                self.data_cache[pdid]['data'].append(None)

    def get_param_interp(self, pdid, interp_times):
        times, data = self.get_param(pdid, TimeRange(interp_times[0], interp_times[-1]))
        times, data = common.stretch(times, data, interp_times)
        times, data = common.interpolate(times, data, interp_times)
        return times, data


class StreamRequest(object):

    def __init__(self, stream_keys, parameters, coefficients, time_range, qc_parameters={}, needs_only=False, limit=None, times=None):
        self.stream_keys = stream_keys
        self.time_range = time_range
        self.parameters = parameters
        self.coefficients = coefficients
        self.qc_parameters = qc_parameters
        self.needs_cc = None
        self.needs_params = None
        self.limit = limit
        self.times = times
        self._initialize(needs_only)

    def _initialize(self, needs_only):
        if len(self.stream_keys) == 0:
            abort(400)

        if not needs_only:
            self._fit_time_range()

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
                needs = needs.union([pdref for pdref in each.needs])

        needs_fqn = set([pdref for pdref in needs if pdref.is_fqn()])
        needs = set([pdref.pdid for pdref in needs if not pdref.is_fqn()])

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

        log.debug('Found FQN needs %s' % (', '.join(map(str, needs_fqn))))
        # find the available streams which provide any needed fqn parameters
        found = set([stream_key.stream_name for stream_key in self.stream_keys])
        for pdref in needs_fqn:
            if pdref.stream_name in found:
                continue
            parameter = CachedParameter.from_id(pdref.pdid)
            streams = [CachedStream.from_id(sid) for sid in parameter.streams]
            streams = [s for s in streams if s.name == pdref.stream_name]
            sensor1, stream1 = find_stream(self.stream_keys[0], streams, distinct_sensors)
            if not any([sensor1 is None, stream1 is None]):
                new_stream_key = StreamKey.from_stream_key(self.stream_keys[0], sensor1, stream1.name)
                self.stream_keys.append(new_stream_key)
                found = found.union(stream1.name)

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

    def _fit_time_range(self):
        # assumes start <= stop for time ranges
        try:
            available_time_range = get_available_time_range(self.stream_keys[0])
        except IndexError:
            log.info('No stream metadata in cassandra for %s', self.stream_keys[0])
            abort(400)
        else:
            if self.time_range.start >= available_time_range.stop or self.time_range.stop <= available_time_range.start:
                log.info('No data in requested time range (%s, %s) for %s ', self.time_range.start, self.time_range.stop, self.stream_keys[0])
                abort(400)

            start = max(self.time_range.start, available_time_range.start)
            stop = min(self.time_range.stop, available_time_range.stop)
            log.debug('fit (%s, %s) to (%s, %s) for %s', self.time_range.start, self.time_range.stop, start, stop, self.stream_keys[0])
            self.time_range = TimeRange(start, stop)


class Chunk_Generator(object):

    def __init__(self):
        self.streams = None
        self.parameters = None
        self.coefficients = None
        self.time_range = None
        self.qc_functions = None

    def _create_data_stream(self, stream_key, limit):
        if stream_key.stream is None:
            raise InvalidStreamException('The requested stream does not exist in preload',
                                         payload={'stream': stream_key.as_dict()})

        return DataStream(stream_key, self.time_range, limit)

    def _query_all(self):
        for stream in self.streams:
            stream.async_query()

    def _terminate_all(self):
        for stream in self.streams:
            stream.terminate_query()

    def _calculate(self, parameter, chunk):
        this_ref = PDRef(None, parameter.id)
        needs = [pdref for pdref in parameter.needs if pdref.chunk_key not in chunk.keys()]
        # prevent loops since they are only warned against
        if this_ref in needs:
            needs.remove(this_ref)
        for pdref in needs:
            # this should descend through any L2 functions to
            # calculate the underlying L1 functions first
            needed_parameter = CachedParameter.from_id(pdref.pdid)
            if needed_parameter.parameter_type == FUNCTION:
                self._calculate(needed_parameter, chunk)
            # we may have already inserted this during recursion
            if pdref.chunk_key not in chunk:
                self._get_param(pdref, chunk)

        try:
            args = build_func_map(parameter, chunk, self.coefficients)
            chunk[parameter.id] = {'data': execute_dpa(parameter, args), 'source': 'derived'}
        except StreamEngineException as e:
            log.warning(e.message)


    def _get_param(self, pdref, chunk):
        found_stream = None
        if pdref.is_fqn():
            log.debug('Requesting FQN Parameter %s' % (pdref))
            for stream in self.streams:
                if stream.stream_key.stream_name == pdref.stream_name:
                    log.debug('Found FQN Parameter %s' % (pdref))
                    found_stream = stream
                    break
        else:
            for stream in self.streams[1:]:
                if stream.provides(pdref.pdid):
                    found_stream = stream
                    break

        if found_stream is None:
            log.warning('Failed to resolve %s' % (pdref))
        elif found_stream is not self.streams[0]:
            chunk[pdref.chunk_key] = {
                'data': found_stream.get_param_interp(pdref.pdid, chunk[7]['data'])[1],
                'source': found_stream.stream_key.as_refdes()
            }
        else:
            chunk[pdref.chunk_key] = chunk[pdref.pdid]

    def _execute_dpas_chunk(self, chunk):
        for parameter in self.parameters:
            if parameter.id not in chunk:
                if parameter.parameter_type == FUNCTION:
                    self._calculate(parameter, chunk)
                else:
                    self._get_param(PDRef(None, parameter.id), chunk)
                if self.qc_functions.get(parameter.name) is not None:
                    self._qc_check(parameter, chunk)


    def _qc_check(self, parameter, chunk):
        qcs = self.qc_functions.get(parameter.name)
        for function_name in qcs:
            if qcs.get(function_name).get('strict_validation') is None:
                qcs.get(function_name)['strict_validation'] = 'False'
            qcs.get(function_name)['dat'] = chunk.get(parameter.id).get('data')
            module = importlib.import_module(CachedFunction.from_qc_function(function_name).owner)
            chunk['%s_%s' %(parameter.name.encode('ascii', 'ignore'), function_name)] = \
                {'data': getattr(module, function_name)(**qcs.get(function_name)), 'source': 'qc'}

    def chunks(self, r):
        self.parameters = r.parameters
        self.coefficients = r.coefficients
        self.time_range = r.time_range
        self.streams = [self._create_data_stream(key, r.limit) for key in r.stream_keys]
        self.streams[0].strict_range = True
        self.qc_functions = r.qc_parameters

        self._query_all()
        for chunk in self.streams[0].create_generator():
            self._execute_dpas_chunk(chunk)
            yield(chunk)


class Particle_Generator(object):

    def __init__(self, generator):
        self.generator = generator

    def chunk_to_particles(self, stream_key, parameters, chunk, qc_parameters={}):
        pk = stream_key.as_dict()
        #pprint.pprint(qc_parameters)
        for index, t in enumerate(chunk[7]['data']):
            particle = OrderedDict()
            particle['pk'] = pk
            pk['time'] = t

            # special cases for deployment number and provenance key
            pk['deployment'] = chunk['deployment']['data'][index]
            particle['provenance'] = str(chunk['provenance']['data'][index])
            for param in parameters:
                if param.id in chunk:
                    value = chunk[param.id]['data'][index]
                    if type(value) == numpy.ndarray:
                        value = value.tolist()
                    particle[param.name] = value
                if qc_parameters.get(param.name) is not None:
                    for qc_function_name in qc_parameters.get(param.name):
                        qc_function_results = '%s_%s' %(param.name, qc_function_name)
                        if qc_function_results in chunk:
                            value = chunk[qc_function_results]['data'][index]
                            qc_results_key = '%s_%s' %(param.name, 'qc_results')
                            if(particle.get(qc_results_key) is None):
                                particle[qc_results_key] = 0b0000000000000000
                            qc_results_value = particle.get(qc_results_key)
                            qc_cached_function = CachedFunction.from_qc_function(qc_function_name)
                            qc_results_mask = int(qc_cached_function.qc_flag, 2)
                            if value == 0:
                                qc_results_value = ~qc_results_mask & qc_results_value
                            elif value == 1:
                                qc_results_value = qc_results_mask ^ qc_results_value
                            particle[qc_results_key] = qc_results_value
            yield json.dumps(particle, indent=2)

    def chunks(self, r):
        count = 0
        yield '[ '
        try:
            first = True
            for chunk in self.generator.chunks(r):
                for particle in self.chunk_to_particles(r.stream_keys[0], r.parameters, chunk, r.qc_parameters):
                    count += 1
                    if first:
                        first = False
                    else:
                        yield ', '
                    yield particle
                if r.limit is not None :
                    if r.limit <= count :
                        break
            yield ']'
        except GeneratorExit as e:
            raise e
        except Exception as e:
            log.exception('An unexpected error occurred.')
            exception_output = ', ' if not first else ''
            exception_output += json.dumps(traceback.format_exc()) + ']'
            yield exception_output
        finally:
            self.generator._terminate_all()


class NetCDF_Generator(object):

    def __init__(self, generator):
        self.generator = generator

    def chunks(self, r):
        try:
            return self.create_netcdf(r)
        except GeneratorExit:
            raise
        except:
            log.exception('An unexpected error occurred.')
            raise

    def create_netcdf(self, r):
        with tempfile.NamedTemporaryFile() as tf:
            with netCDF4.Dataset(tf.name, 'w', format='NETCDF4') as ncfile:
                # set up file level attributes
                ncfile.subsite = r.stream_keys[0].subsite
                ncfile.node = r.stream_keys[0].node
                ncfile.sensor = r.stream_keys[0].sensor
                ncfile.collection_method = r.stream_keys[0].method
                ncfile.stream = r.stream_keys[0].stream.name

                variables = {}
                last_timestamp = 0.0
                # iterate through the chunks and populate the data
                for chunk in self.generator.chunks(r):
                    # sometimes we will get duplicate timestamps
                    # INITIAL solution is to remove any duplicate timestamps
                    # and the corresponding data.

                    # create a mask to match only valid INCREASING times
                    # we will insert the last valid timestamp from the previous
                    # chunk at the beginning of the array
                    chunk_times = chunk[7]['data']
                    chunk_valid = numpy.diff(numpy.insert(chunk_times, 0, last_timestamp)) != 0
                    # We will need to keep track of the last timestamp of each chunk so
                    # that we can apply this logic across chunks
                    last_timestamp = chunk_times[chunk_valid][-1]

                    deployment = str(chunk['deployment']['data'][0])
                    time_v_key = '7_%s' % (deployment,)
                    time_dim = 'time_%s' % (deployment,)
                    if time_v_key not in variables:
                        index = 0
                        ncfile.createDimension(time_dim, None)
                    else:
                        index = len(variables[time_v_key])

                    for param_id in chunk:
                        data = chunk[param_id]['data']
                        source = chunk[param_id]['source']

                        if param_id == 7:
                            group = ncfile
                            v_key = time_v_key
                        else:
                            if source in ncfile.groups:
                                group = ncfile.groups[source]
                            else:
                                group = ncfile.createGroup(source)

                            if deployment in group.groups:
                                group = group.groups[deployment]
                            else:
                                group = group.createGroup(deployment)

                            v_key = '/%s/%s/%s' % (source, deployment, param_id)

                        if v_key not in variables:
                            param = CachedParameter.from_id(param_id)
                            # param can be None if this is not a real parameter,
                            # like deployment for deployment number
                            param_name = param_id if param is None else param.name

                            dims = [time_dim]
                            if len(data.shape) > 1:
                                for index, dimension in enumerate(data.shape[1:]):
                                    name = '%s_dim_%d' % (param_name, index)
                                    group.createDimension(name, dimension)
                                    dims.append(name)

                            try:
                                variables[v_key] = group.createVariable(param_name if param_id != 7 else v_key,
                                                                            data.dtype,
                                                                            dims,
                                                                            zlib=True)
                            except TypeError:
                                log.error('Unable to create variable: %s, %s, %s, %s' % (v_key, data.dtype, data.shape, traceback.format_exc()))
                            else:
                                if param:
                                    if param.unit is not None:
                                        variables[v_key].units = param.unit
                                    if param.fill_value is not None:
                                        variables[v_key].fill_value = param.fill_value
                                    if param.description is not None:
                                        variables[v_key].long_name = param.description
                                    if param.display_name is not None:
                                        variables[v_key].display_name = param.display_name
                                    if param.data_product_identifier is not None:
                                        variables[v_key].data_product_identifier = param.data_product_identifier

                        try:
                            variables[v_key][index:] = data[chunk_valid]
                        except KeyError:
                            log.error('Variable identified with key %s doesn\'t exist' % (v_key))
                        except (IndexError, ValueError):
                            log.error('Unable to write data slice to variable: %s, %s, %s, %s' % (v_key, data.dtype, data.shape, traceback.format_exc()))

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
