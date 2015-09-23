from functools import wraps
import time
import datetime
import msgpack
import ntplib
import xray
import pandas as pd
from engine import app
from preload_database.model.preload import Stream, Parameter, ParameterFunction
import numpy
from scipy.interpolate import griddata
import parameter_util
import dateutil.parser

FUNCTION = 'function'

stream_cache = {}
parameter_cache = {}
function_cache = {}


def isfillvalue(a):
    """
    Test element-wise for fill values and return result as a boolean array.
    :param a: array_like
    :return: ndarray
    """
    a = numpy.asarray(a)
    if a.dtype.kind == 'i':
        mask = a == -999999999
    elif a.dtype.kind == 'f':
        mask = numpy.isnan(a)
    elif a.dtype.kind == 'S':
        mask = a == ''
    else:
        raise ValueError('Fill value not known for dtype %s' % (a.dtype))
    return mask


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
        #data = data.astype('f64')
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

def ntp_to_datestring(ntp_time):
    try:
        ntp_time = float(ntp_time)
        return datetime.datetime.utcfromtimestamp( ntplib.ntp_to_system_time(ntp_time) ).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return str(ntp_time)


def ISO_to_ntp(ds):
    """"
    Given a datestring give back the iso time.
    Assumes that it is UTC time also
    Assuming it is in the form YYYY-MM-DDTHH-MM-SS.sssZ NO VALIDATION
    """
    dt_time = dateutil.parser.parse(ds)
    diff = (dt_time - datetime.datetime(1900, 1,1)).total_seconds()
    return diff


def ntp_to_ISO_date(ntp_time):
    try:
        ntp_time = float(ntp_time)
        dt = datetime.datetime.utcfromtimestamp(ntplib.ntp_to_system_time(ntp_time))
        return dt.isoformat()
    except Exception as e:
        return str(ntp_time)

class TimeRange(object):
    def __init__(self, start, stop):
        self.start = start
        self.stop = stop

    def secs(self):
        return abs(self.stop - self.start)

    def __str__(self):
        return "{} - {}".format(self.start, self.stop)

class Annotation(object):
    def __init__(self, refdes, start, end, parameters, provenance, annotation, method, deployment, ident):
        self.referenceDesignator = refdes
        self.beginDT = start
        self.endDT = end
        self.parameters = parameters
        self.provenance = provenance
        self.annotation = annotation
        self.method = method
        self.deployment = deployment
        self.ident = ident

    def as_dict(self):
        return {
        'referenceDesignator' : self.referenceDesignator,
        'beginDT' : self.beginDT,
        'endDT' : self.endDT,
        'parameters' : self.parameters,
        'provenance' : self.provenance,
        'annotation' : self.annotation,
        'method' : self.method,
        'deployment' : self.deployment,
        'id' : self.ident,
        }

    def __eq__(self, other):
        if not isinstance(other, Annotation):
            return False
        return self.ident == other.ident

    def __hash__(self):
        return hash(self.ident)

    @staticmethod
    def from_dict(d):
        return Annotation(d["referenceDesignator"], d["beginDT"], d["endDT"], d["parameters"], d["provenance"],
                            d["annotation"], d["method"], d["deployment"], d["id"])

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
    def from_refdes(refdes):
        return StreamKey(*refdes.split('|'))

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
        return '%(subsite)s|%(node)s|%(sensor)s|%(method)s|%(stream)s' % self.as_dict()

    def as_dashed_refdes(self):
        return self.as_refdes().replace('|', '-')

    def as_three_part_refdes(self):
        return "{:s}-{:s}-{:s}".format(self.subsite, self.node, self.sensor)

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
            s.time_parameter = stream.time_parameter
            s.parameters = []
            for p in stream.parameters:
                s.parameters.append(CachedParameter.from_parameter(p))

            s.source_streams = stream.source_streams
            s.product_streams = stream.product_streams
            s.is_virtual = len(stream.source_streams) > 0

            stream_cache[stream.id] = s
        return stream_cache[stream.id]

    @staticmethod
    def from_id(stream_id):
        if stream_id not in stream_cache:
            stream_cache[stream_id] = CachedStream.from_stream(Stream.query.get(stream_id))
        return stream_cache[stream_id]

    def as_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'parameters': self.parameters
        }

    def __eq__(self, other):
        return self.id == other.id

    def __str__(self):
        return str(self.as_dict())


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
            cp.standard_name = parameter.standard_name
            cp.precision = parameter.precision
            cp.parameter_function_map = parameter.parameter_function_map
            cp.data_product_identifier = parameter.data_product_identifier
            cp.description = parameter.description
            cp.parameter_function = CachedFunction.from_function(parameter.parameter_function)
            cp.streams = [stream.id for stream in parameter.streams]
            cp.needs = parameter_util.needs(parameter)
            cp.needs_cc = parameter_util.needs_cc(parameter)
            parameter_cache[parameter.id] = cp
        return parameter_cache[parameter.id]

    @property
    def is_array(self):
        return self.parameter_type == 'array<quantity>'

    @staticmethod
    def from_id(pdid):
        if pdid not in parameter_cache:
            parameter_cache[pdid] = CachedParameter.from_parameter(Parameter.query.get(pdid))
        return parameter_cache[pdid]

    def as_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'parameter_type': self.parameter_type,
            'value_encoding': self.value_encoding,
            'code_set': self.code_set,
            'unit': self.unit,
            'fill_value': self.fill_value,
            'display_name': self.display_name,
            'standard_name' : self.standard_name,
            'precision': self.precision,
            'parameter_function_map': self.parameter_function_map,
            'data_product_identifier': self.data_product_identifier,
            'description': self.description,
            'parameter_function': self.parameter_function,
            'streams': self.streams,
            'needs': self.needs,
            'needs_cc': self.needs_cc
        }

    def __str__(self):
        return str(self.as_dict())


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

class MissingTimeException(StreamEngineException):
    """
    Internal error. A stream is missing its time parameter
    """
    status_code = 500

class MissingDataException(StreamEngineException):
    """
    Internal error. Cassandra returned no data for this time range
    """
    status_code = 400

class MissingStreamMetadataException(StreamEngineException):
    """
    Internal error. Cassandra contains no metadata for the requested stream
    """
    status_code = 400

class InvalidInterpolationException(StreamEngineException):
    """
    Internal error. Invalid interpolation was attempted.
    """
    status_code = 500

class UIHardLimitExceededException(StreamEngineException):
    """
    The limit on UI queries size was exceeded
    """
    status_code = 413


def arb(d):
    """ Returns an arbitrary value from the given dictionary """
    return next(d.itervalues())


def get_stream_key_with_param(pd_data, stream, parameter):
    """Looks for a stream_key matching *stream* in pd_data that provides *parameter*"""
    for refdes in pd_data[parameter]:
        key = StreamKey.from_refdes(refdes)
        if key.stream == stream:
            return key

    return None


def fix_data_arrays(data, unpacked):
    if unpacked is None:
        return
    if len(unpacked) != data.shape[0]:
        app.logger.warn("Mismatched dimensions could not fill array")
        return
    if len(data.shape) == 1:
        for idx, val in enumerate(unpacked):
            if idx < len(data):
                data[idx] = unpacked[idx]
    else:
        if isinstance(unpacked, list):
            for data_sub, unpacked_sub in zip(data, unpacked):
                fix_data_arrays(data_sub, unpacked_sub)

def to_xray_dataset(cols, data, stream_key, san=False):
    """
    Make an xray dataset from the raw cassandra data
    """
    if len(data) == 0:
        return None
    arrays = set([p.name for p in stream_key.stream.parameters if p.parameter_type != FUNCTION and p.is_array])
    params = {p.name : p for p in stream_key.stream.parameters if p.parameter_type != FUNCTION }
    attrs = {
        'subsite': stream_key.subsite,
        'node': stream_key.node,
        'sensor': stream_key.sensor,
        'collection_method': stream_key.method,
        'stream': stream_key.stream.name,
        'institution' : '{:s}'.format(app.config['NETCDF_INSTITUTION']),
        'source' : '{:s}'.format(stream_key.as_dashed_refdes()),
        'references' : '{:s}'.format(app.config['NETCDF_REFERENCE']),
        'comment' : '{:s}'.format(app.config['NETCDF_COMMENT']),
    }
    if san:
        attrs['title'] = '{:s} for {:s}'.format("SAN offloaded netCDF", stream_key.as_dashed_refdes())
        attrs['history'] = '{:s} {:s}'.format(datetime.datetime.utcnow().isoformat(), 'generated netcdf for SAN')
    else:
        attrs['title'] = '{:s} for {:s}'.format(app.config['NETCDF_TITLE'], stream_key.as_dashed_refdes())
        attrs['history'] = '{:s} {:s}'.format(datetime.datetime.utcnow().isoformat(), app.config['NETCDF_HISTORY_COMMENT'])
    dataset = xray.Dataset(attrs=attrs)
    dataframe = pd.DataFrame(data=data, columns=cols)
    for column in dataframe.columns:
        # unpack any arrays
        if column in arrays:
            unpacked = [msgpack.unpackb(x) for x in dataframe[column].values]
            no_nones = filter(None, unpacked)
            # Get the maximum sized array using numpy
            if len(no_nones) > 0:
                shapes = [numpy.array(x).shape for x in no_nones]
                max_len = max((len(x) for x in shapes))
                shapes = filter(lambda x: len(x) == max_len, shapes)
                max_shape = max(shapes)
                shp = tuple([len(unpacked)] + list(max_shape))
                data = numpy.empty(shp)
                data.fill(param.fill_value)
                fix_data_arrays(data, unpacked)
            else:
                data = numpy.array([[] for _ in unpacked])
        else:
            data = dataframe[column].values
        # No objects. They should be strings if going to SAN
        if san and data.dtype  == 'object':
            data = data.astype(str)

        # Fix up the dimensions for possible multi-d objects
        dims = ['index']
        coords = {'index' : dataframe.index}
        if len(data.shape) > 1:
            for index, dim in enumerate(data.shape[1:]):
                name = "{:s}_dim_{:d}".format(column, index)
                dims.append(name)

        # update attributes for each variable
        array_attrs = {}
        if column in params:
            param = params[column]
            if param.unit is not None:
                array_attrs['units'] = param.unit
            if param.fill_value is not None:
                array_attrs['_FillValue'] = param.fill_value
            if param.display_name is not None:
                array_attrs['long_name'] = param.display_name
            elif param.name is not None:
                array_attrs['long_name'] = param.name
            else:
                array_attrs['long_name'] = column
            if param.standard_name is not None:
                array_attrs['standard_name'] = param.standard_name
            if param.description is not None:
                array_attrs['comment'] = param.description
            if param.data_product_identifier is not None:
                array_attrs['data_product_identifier'] = param.data_product_identifier
        else:
            array_attrs['long_name'] = column

        dataset.update({column : xray.DataArray(data, dims=dims, attrs=array_attrs)})

    return dataset

def compile_datasets(datasets):
    """
    Given a list of datasets. Possibly containing None. Return a single
    dataset with unique indexes and sorted by the 'time' parameter
    :param datasets: :return:
    """
    # filter out the Nones
    datasets = filter(None, datasets)
    if len(datasets) == 0:
        return None
    datasets.sort(key=lambda val: val['time'].values[0])
    # now determine if they are in order or not..
    start = 0
    end = 0
    idx = 0
    in_order = True
    for ds in datasets:
        ns = ds['time'].min()
        ne = ds['time'].max()
        # Determine if the max and the min are all in order
        within_range =  ns < end and ne > start
        in_order = in_order and not within_range
        start = ns
        end = ne
        new_index = [i for i in range(idx, idx + len(ds['index']))]
        ds['index'] = new_index
        idx = new_index[-1] + 1
    dataset = xray.concat(datasets, dim='index')
    if not in_order:
        sorted_idx = dataset.time.argsort()
        dataset = dataset.reindex({'index' : sorted_idx})
    return dataset
