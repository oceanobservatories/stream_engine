from functools import wraps
import time
from engine import app
from model.preload import Stream, Parameter, ParameterFunction
import numpy
from scipy.interpolate import griddata

FUNCTION = 'function'

stream_cache = {}
parameter_cache = {}
function_cache = {}


def with_size_and_fill(a, size):
    """
    Return array with same dtype and shape as a, but with axis 0 of length size.
    The resulting array will be filled with appropriate values for the dtype.  Currently,
    this is only numpy.NAN.
    :param a: array
    :param size: scalar
    :return: array with same dtype and shape as a, but with axis 0 of length size.
    """
    shape = a.shape
    dtype = a.dtype
    x = numpy.empty((size,) + shape[1:], dtype)
    # TODO: Fill with appropriate value for dtype
    x.fill(numpy.NAN)
    return x


def stretch(times, data, interp_times):
    if len(times) == 1:
        new_data = with_size_and_fill(numpy.array(data), len(interp_times))
        new_data[:] = data[0]
        return interp_times, new_data
    if interp_times[0] < times[0]:
        times = numpy.concatenate(([interp_times[0]], times))
        data = numpy.concatenate(([data[0]], data))
    if interp_times[-1] > times[-1]:
        times = numpy.concatenate((times, [interp_times[-1]]))
        data = numpy.concatenate((data, [data[-1]]))
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


def log_timing(func):
    if app.logger.isEnabledFor('debug'):
        @wraps(func)
        def inner(*args, **kwargs):
            app.logger.debug('Entered method: %s', func)
            start = time.time()
            results = func(*args, **kwargs)
            elapsed = time.time() - start
            app.logger.debug('Completed method: %s in %.2f', func, elapsed)
            return results
    else:
        @wraps(func)
        def inner(*args, **kwargs):
            return func(*args, **kwargs)

    return inner


def parse_pdid(pdid_string):
    try:
        return int(pdid_string.split()[0][2:])
    except ValueError:
        app.logger.warn('Unable to parse PDID: %s', pdid_string)
        return None


class TimeRange(object):
    def __init__(self, start, stop):
        self.start = start
        self.stop = stop

    def secs(self):
        return abs(self.stop - self.start)


class StreamKey(object):
    def __init__(self, subsite, node, sensor, method, stream):
        self.subsite = subsite
        self.node = node
        self.sensor = sensor
        self.method = method
        self.stream_name = stream
        if stream not in stream_cache:
            stream_cache[stream] = CachedStream.from_stream(Stream.query.filter(Stream.name == stream).first())
        self.stream = stream_cache[stream]
        # convenience property
        self.needs_cc = set().union(*[param.needs_cc for param in self.stream.parameters if param.parameter_type == FUNCTION])

    @staticmethod
    def from_dict(d):
        return StreamKey(d['subsite'], d['node'], d['sensor'], d['method'], d['stream'])

    @staticmethod
    def from_stream_key(stream_key, sensor, stream):
        return StreamKey(stream_key.subsite, stream_key.node, sensor, stream_key.method, stream)

    def __eq__(self, other):
        return all([self.subsite == other.subsite,
                    self.node == other.node,
                    self.sensor == other.sensor,
                    self.method == other.method,
                    self.stream == other.stream])

    def as_dict(self):
        return {
            'subsite': self.subsite,
            'node': self.node,
            'sensor': self.sensor,
            'method': self.method,
            'stream': self.stream.name if self.stream is not None else None
        }

    def as_refdes(self):
        return '%(subsite)s-%(node)s-%(sensor)s-%(method)s-%(stream)s' % self.as_dict()

    def __repr__(self):
        return repr(self.as_dict())

    def __str__(self):
        return str(self.as_dict())


class CachedStream(object):
    """
    Object to hold a cached version of the Stream DB object
    """
    @staticmethod
    def from_stream(stream):
        if stream.id not in stream_cache:
            s = CachedStream()
            s.id = stream.id
            s.name = stream.name
            s.parameters = []
            for p in stream.parameters:
                s.parameters.append(CachedParameter.from_parameter(p))
            stream_cache[stream.id] = s
        return stream_cache[stream.id]

    @staticmethod
    def from_id(stream_id):
        if stream_id not in stream_cache:
            stream_cache[stream_id] = CachedStream.from_stream(Stream.query.get(stream_id))
        return stream_cache[stream_id]


class CachedParameter(object):
    @staticmethod
    def from_parameter(parameter):
        if parameter is None:
            return None
        if parameter.id not in parameter_cache:
            cp = CachedParameter()
            cp.id = parameter.id
            cp.name = parameter.name
            cp.parameter_type = parameter.parameter_type.value if parameter.parameter_type is not None else None
            cp.value_encoding = parameter.value_encoding.value if parameter.value_encoding is not None else None
            cp.code_set = parameter.code_set.value if parameter.code_set is not None else None
            cp.unit = parameter.unit.value if parameter.unit is not None else None
            cp.fill_value = parameter.fill_value.value if parameter.fill_value is not None else None
            cp.display_name = parameter.display_name
            cp.precision = parameter.precision
            cp.parameter_function_map = parameter.parameter_function_map
            cp.data_product_identifier = parameter.data_product_identifier
            cp.description = parameter.description
            cp.parameter_function = CachedFunction.from_function(parameter.parameter_function)
            cp.streams = [stream.id for stream in parameter.streams]
            cp.needs = [p.id for p in parameter.needs()]
            cp.needs_cc = parameter.needs_cc()
            parameter_cache[parameter.id] = cp
        return parameter_cache[parameter.id]

    @staticmethod
    def from_id(pdid):
        if pdid not in parameter_cache:
            parameter_cache[pdid] = CachedParameter.from_parameter(Parameter.query.get(pdid))
        return parameter_cache[pdid]


class CachedFunction(object):
    @staticmethod
    def from_function(function):
        if function is None:
            return None
        if function.id not in function_cache:
            f = CachedFunction()
            f.id = function.id
            f.function_type = function.function_type.value if function.function_type is not None else None
            f.function = function.function
            f.owner = function.owner
            f.description = function.description
            f.qc_flag = function.qc_flag
            function_cache[function.id] = f
        return function_cache[function.id]

    @staticmethod
    def from_qc_function(qc_function_name):
        for function_id in function_cache:
            if function_cache.get(function_id).function.encode('ascii', 'ignore') == qc_function_name:
                return function_cache.get(function_id)
        ret = CachedFunction.from_function(ParameterFunction.query.filter_by(function = qc_function_name).first())
        if ret is None:
            app.logger.warn('Unable to find QC function: %s', qc_function_name)
        return ret


class StreamEngineException(Exception):
    status_code = 500

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv


class StreamUnavailableException(StreamEngineException):
    """
    Stream is not in cassandra
    """
    status_code = 404


class InvalidStreamException(StreamEngineException):
    """
    Stream does not exist in preload
    """
    status_code = 400


class InvalidParameterException(StreamEngineException):
    """
    Parameter does not exist in preload or the specified stream
    """
    status_code = 400


class MalformedRequestException(StreamEngineException):
    """
    Structural problem in this request such as missing mandatory data
    """
    status_code = 400


class CoefficientUnavailableException(StreamEngineException):
    """
    Missing a required calibration coefficient
    """
    status_code = 400


class UnknownEncodingException(StreamEngineException):
    """
    Internal error. A parameter specified an unknown encoding type
    """
    status_code = 500


class UnknownFunctionTypeException(StreamEngineException):
    """
    Internal error. A function specified an unknown function type
    """
    status_code = 500


class AlgorithmException(StreamEngineException):
    """
    Internal error. Exception while executing a DPA
    """
    status_code = 500