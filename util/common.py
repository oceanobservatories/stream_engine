import datetime
import logging
import time
import msgpack
import ntplib
import numpy
import pandas as pd
import xray
from functools import wraps

from engine import app
from preload_database.model.preload import Stream

FUNCTION = 'function'

log = logging.getLogger(__name__)

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
        raise ValueError('Fill value not known for dtype %s' % a.dtype)
    return mask


def log_timing(logger):
    def _log_timing(func):
        if logger.isEnabledFor('debug'):
            @wraps(func)
            def inner(*args, **kwargs):
                reqid = None
                if args:
                    reqid = getattr(args[0], 'request_id', 'None')
                logger.debug('<%s> Entered method: %s', reqid, func)
                start = time.time()
                results = func(*args, **kwargs)
                elapsed = time.time() - start
                logger.debug('<%s> Completed method: %s in %.2f', reqid, func, elapsed)
                return results
        else:
            @wraps(func)
            def inner(*args, **kwargs):
                return func(*args, **kwargs)

        return inner

    return _log_timing


def ntp_to_datetime(ntp_time):
    try:
        ntp_time = float(ntp_time)
        unix_time = ntplib.ntp_to_system_time(ntp_time)
        dt = datetime.datetime.utcfromtimestamp(unix_time)
        return dt
    except ValueError:
        return None


def ntp_to_datestring(ntp_time):
    dt = ntp_to_datetime(ntp_time)
    if dt is None:
        return str(ntp_time)
    return dt.isoformat()


class TimeRange(object):
    def __init__(self, start, stop):
        self.start = start
        self.stop = stop

    def secs(self):
        return abs(self.stop - self.start)

    def __str__(self):
        return "{} - {}".format(self.start, self.stop)

    def collapse(self, other):
        start = max(self.start, other.start)
        stop = min(self.stop, other.stop)
        if start != self.start or stop != self.stop:
            return TimeRange(start, stop)

    def copy(self):
        return TimeRange(self.start, self.stop)

    def __eq__(self, other):
        return self.stop == other.stop and self.start == other.start


class StreamKey(object):
    glider_prefixes = ['GL', 'PG']
    mobile_prefixes = ['GL', 'PG', 'SF', 'WFP', 'SP']

    def __init__(self, subsite, node, sensor, method, stream):
        self.subsite = subsite
        self.node = node
        self.sensor = sensor
        self.method = method
        self.stream_name = stream
        self.stream = Stream.query.filter(Stream.name == stream).first()

    def _check_node(self, prefixes):
        for prefix in prefixes:
            if self.node.startswith(prefix):
                return True
        return False

    @property
    def is_virtual(self):
        return bool(self.stream.source_streams)

    @property
    def is_mobile(self):
        return self._check_node(self.mobile_prefixes)

    @property
    def is_glider(self):
        return self._check_node(self.glider_prefixes)

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
        return self.as_tuple() == other.as_tuple()

    def __hash__(self):
        return hash((self.subsite, self.node, self.sensor, self.method, self.stream_name))

    def as_dict(self):
        return {
            'subsite': self.subsite,
            'node': self.node,
            'sensor': self.sensor,
            'method': self.method,
            'stream': self.stream.name if self.stream is not None else None
        }

    def as_tuple(self):
        return self.subsite, self.node, self.sensor, self.method, self.stream_name

    def as_refdes(self):
        return '|'.join(self.as_tuple())

    def as_dashed_refdes(self):
        return '-'.join(self.as_tuple())

    def as_three_part_refdes(self):
        return '-'.join(self.as_tuple()[:3])

    def __repr__(self):
        return repr(self.as_dict())

    def __str__(self):
        return str(self.as_dict())


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


class ParamUnavailableException(StreamEngineException):
    """
    Missing a required parameter
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


class TimedOutException(Exception):
    pass


def fix_data_arrays(data, unpacked):
    if unpacked is None:
        return
    if len(unpacked) != data.shape[0]:
        app.logger.warn("Mismatched dimensions could not fill array")
        return
    if len(data.shape) == 1:
        for idx, val in enumerate(unpacked):
            if idx < len(data):
                """ Don't overwrite the fill value when data is None """
                if unpacked[idx] is not None:
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
    params = {p.name: p for p in stream_key.stream.parameters if p.parameter_type != FUNCTION}
    attrs = {
        'subsite': stream_key.subsite,
        'node': stream_key.node,
        'sensor': stream_key.sensor,
        'collection_method': stream_key.method,
        'stream': stream_key.stream.name,
        'institution': '{:s}'.format(app.config['NETCDF_INSTITUTION']),
        'source': '{:s}'.format(stream_key.as_dashed_refdes()),
        'references': '{:s}'.format(app.config['NETCDF_REFERENCE']),
        'comment': '{:s}'.format(app.config['NETCDF_COMMENT']),
    }
    if san:
        attrs['title'] = '{:s} for {:s}'.format("SAN offloaded netCDF", stream_key.as_dashed_refdes())
        attrs['history'] = '{:s} {:s}'.format(datetime.datetime.utcnow().isoformat(), 'generated netcdf for SAN')
    else:
        attrs['title'] = '{:s} for {:s}'.format(app.config['NETCDF_TITLE'], stream_key.as_dashed_refdes())
        attrs['history'] = '{:s} {:s}'.format(datetime.datetime.utcnow().isoformat(),
                                              app.config['NETCDF_HISTORY_COMMENT'])
    dataset = xray.Dataset(attrs=attrs)
    dataframe = pd.DataFrame(data=data, columns=cols)
    for column in dataframe.columns:
        # unpack any arrays
        if column in params:
            data = replace_values(dataframe[column].values,
                                  params[column].value_encoding,
                                  get_fill_value(params[column]),
                                  params[column].parameter_type == 'array<quantity>',
                                  params[column].name)
        else:
            data = replace_values(dataframe[column].values, str, '', False, column)

        # Fix up the dimensions for possible multi-d objects
        dims = ['index']
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
            if get_fill_value(param) is not None:
                array_attrs['_FillValue'] = get_fill_value(param)
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

        dataset.update({column: xray.DataArray(data, dims=dims, attrs=array_attrs)})

    return dataset


def replace_values(data_slice, value_encoding, fill_value, is_array, name):
    """
    Replace any missing values in the parameter
    :param data_slice: pandas series to replace missing values in
    :param value_encoding: Type information about the parameter
    :param fill_value: Fill value for the parameter
    :param is_array: Flag indicating if this is a msgpack array
    :return: data_slice with missing values filled with fill value
    """
    # Nones can only be in ndarrays with dtype == object.  NetCDF
    # doesn't like objects.  First replace Nones with the
    # appropriate fill value.
    #
    # pandas does some funny things to missing values if the whole column is missing it becomes a None filled object
    # Otherwise pandas will replace floats with Not A Number correctly.
    # Integers are cast as floats and missing values replaced with Not A Number
    # The below case will take care of instances where the whole series is missing or if it is an array or
    # some other object we don't know how to fill.
    if is_array:
        unpacked = [msgpack.unpackb(x) for x in data_slice]
        no_nones = filter(None, unpacked)
        # Get the maximum sized array using numpy
        if len(no_nones) > 0:
            shapes = [numpy.array(x).shape for x in no_nones]
            max_len = max((len(x) for x in shapes))
            shapes = filter(lambda x: len(x) == max_len, shapes)
            max_shape = max(shapes)
            shp = tuple([len(unpacked)] + list(max_shape))
            data_slice = numpy.empty(shp, dtype=value_encoding)
            data_slice.fill(fill_value)
            try:
                fix_data_arrays(data_slice, unpacked)
            except Exception:
                log.exception("Error filling arrays with data for parameter %s replacing with fill values", name)
                data_slice.fill(fill_value)
        else:
            data_slice = numpy.array([[] for _ in unpacked], dtype=value_encoding)
    if data_slice.dtype == 'object' and not is_array:
        nones = numpy.equal(data_slice, None)
        if numpy.any(nones):
            if fill_value is not None:
                data_slice[nones] = fill_value
                data_slice = data_slice.astype(value_encoding)
            else:
                log.warn("No fill value for param %s", name)
                # If there are nones either fill with specific value for ints, floats, string, or throw an error
                if value_encoding in ['int', 'uint8', 'uint16', 'uint32', 'uint64', 'int8', 'int16', 'int32', 'int64']:
                    data_slice[nones] = -999999999
                    data_slice = data_slice.astype('int64')
                elif value_encoding in ['float16', 'float32', 'float64', 'float96']:
                    data_slice[nones] = numpy.nan
                    data_slice = data_slice.astype('float64')
                elif value_encoding == 'string':
                    data_slice[nones] = ''
                    data_slice = data_slice.astype('str')
                else:
                    raise StreamEngineException('Do not know how to fill for data type %s', value_encoding)

    # otherwise if the returned data is a float we need to check and make sure it is not supposed to be an int
    elif data_slice.dtype == 'float64':
        # Int's are upcast to floats if there is a missing value.
        if value_encoding in ['int', 'uint8', 'uint16', 'uint32', 'uint64', 'int8', 'int16', 'int32', 'int64']:
            # We had a missing value because it was upcast
            indexes = numpy.where(numpy.isnan(data_slice))
            if len(indexes) > 0:
                if fill_value is not None:
                    data_slice[indexes] = fill_value
                    data_slice = data_slice.astype(value_encoding)
                else:
                    log.warn("No fill value for param %s", name)
                    data_slice[indexes] = -999999999
                    data_slice = data_slice.astype('int64')

    # Pandas also treats strings as objects.  NetCDF doesn't
    # like objects.  So convert objects to strings.
    if data_slice.dtype == object:
        try:
            data_slice = data_slice.astype(value_encoding)
        except ValueError as e:
            log.error('Unable to convert %s to value type (%s) (may be caused by jagged arrays): %s',
                      name, value_encoding, e)
    return data_slice


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
    idx = 0
    for ds in datasets:
        # Determine if the max and the min are all in order
        new_index = [i for i in range(idx, idx + len(ds['index']))]
        ds['index'] = new_index
        idx = new_index[-1] + 1
    dataset = xray.concat(datasets, dim='index')
    sorted_idx = dataset.time.argsort()
    dataset = dataset.reindex({'index': sorted_idx})
    return dataset


def timed_cache(expire_seconds):
    """
    Simple time-based cache. Only valid for functions which have no arguments
    :param expire_seconds: time in seconds before cached result expires
    :return:
    """
    cache = {'cache_time': 0, 'cache_value': None}

    def expired():
        return cache['cache_time'] + expire_seconds < time.time()

    def wrapper(func):
        @wraps(func)
        def inner():
            if expired():
                cache['cache_value'] = func()
                cache['cache_time'] = time.time()
            return cache['cache_value']

        return inner

    return wrapper


def get_fill_value(param):
    if param.fill_value is not None:
        return param.fill_value
    elif param.value_encoding in app.config['FILL_VALUES']:
        return app.config['FILL_VALUES'][param.value_encoding]
    else:
        return None
