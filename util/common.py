from functools import wraps
import time
from engine import app
from model.preload import Stream, Parameter

FUNCTION = 'function'

stream_cache = {}
parameter_cache = {}
function_cache = {}


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


class TimeRange(object):
    def __init__(self, start, stop):
        self.start = start
        self.stop = stop


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
            function_cache[function.id] = f
        return function_cache[function.id]


class DataUnavailableException(Exception):
    pass


class DataNotReadyException(Exception):
    pass


class CoefficientUnavailableException(Exception):
    pass


class UnknownEncodingException(Exception):
    pass


class StreamNotFoundException(Exception):
    pass


class UnknownFunctionTypeException(Exception):
    pass