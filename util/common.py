import os
import csv
import datetime
import logging
import time
from functools import wraps
from collections import OrderedDict

import ntplib
import numpy

from engine import app
from ooi_data.postgres.model import Stream
#from util.qc_executor import QC_EXECUTED, QC_RESULTS
#from util.qartod_qc_executor import QARTOD_PRIMARY, QARTOD_SECONDARY

log = logging.getLogger(__name__)

stream_cache = {}
parameter_cache = {}
function_cache = {}

PROVENANCE_KEYORDER = ["eventId", "editPhase", "eventName", "eventType", "referenceDesignator", "deploymentNumber",
                       "versionNumber", "inductiveId", "assetUid", "dataSource", "lastModifiedTimestamp", "tense", 
                       "ingestInfo", "notes", "mooring", "mooring.location.location", "node", "node.location.location", 
                       "eventStartTime", "eventStopTime", "waterDepth", "location", "location.location", "deployedBy", 
                       "deployCruiseInfo", "recoveredBy", "recoverCruiseInfo", "sensor", "sensor.location.location", 
                       "sensor.calibration"]

ANNOTATION_FILE_FORMAT = '%s_annotations_%s.json'

# QC Parameter identification patterns
QC_EXECUTED = 'qc_executed'
QC_RESULTS = 'qc_results'
# QARTOD Parameter identification patterns
QARTOD_PRIMARY = 'qartod_flag_primary'
QARTOD_SECONDARY = 'qartod_flag_secondary'


class QartodFlags:
    """Primary flags for QARTOD."""
    # Don't subclass Enum since values don't fit nicely into a numpy array.
    PASS = 1
    NOT_EVALUATED = 2
    SUSPECT = 3
    FAIL = 4
    MISSING = 9

    @classmethod
    def getFlagOrder(cls):
        return [cls.NOT_EVALUATED, cls.PASS, cls.MISSING, cls.SUSPECT, cls.FAIL]

    @classmethod
    def getValidQCFlags(cls):
        return [cls.PASS, cls.NOT_EVALUATED, cls.SUSPECT, cls.FAIL, cls.MISSING]

    @classmethod
    def getQCFlagMeanings(cls):
        return ["PASS", "NOT_EVALUATED", "SUSPECT", "FAIL", "MISSING"]

    @classmethod
    def isValidQCFlag(cls, flag):
        return flag in cls.getValidQCFlags()

def is_qc_parameter(param):
    #return any(quality_variable in param for quality_variable in [QC_EXECUTED, QC_RESULTS, QARTOD_PRIMARY, QARTOD_SECONDARY])
    return True

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


def get_annotation_filename(stream_request):
    time_range = str(stream_request.time_range).replace(" ", "")
    stream_key = stream_request.stream_key.as_dashed_refdes()
    return ANNOTATION_FILE_FORMAT % (stream_key, time_range)


def log_timing(logger):
    def _log_timing(func):
        request_id = 'request_id'
        if logger.isEnabledFor('debug'):
            @wraps(func)
            def inner(*args, **kwargs):
                reqid = kwargs.get(request_id)
                if reqid is None and args:
                    reqid = getattr(args[0], request_id, 'None')
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
    except (ValueError, TypeError):
        return None


def ntp_to_datestring(ntp_time):
    dt = ntp_to_datetime(ntp_time)
    if dt is None:
        return str(ntp_time)
    return dt.isoformat()


def ntp_to_short_iso_datestring(ntp_time):
    dt = ntp_to_datetime(ntp_time)
    if dt is None:
        return str(ntp_time)
    return dt.strftime("%Y%m%dT%H%M%S")


class TimeRange(object):
    def __init__(self, start, stop):
        self.start = start
        self.stop = stop

    def secs(self):
        return abs(self.stop - self.start)

    def __str__(self):
        return "{} - {}".format(ntp_to_datestring(self.start), ntp_to_datestring(self.stop))

    def collapse(self, other):
        start = max(self.start, other.start)
        stop = min(self.stop, other.stop)
        if start != self.start or stop != self.stop:
            return TimeRange(start, stop)
        return self

    def copy(self):
        return TimeRange(self.start, self.stop)

    def as_millis(self):
        """
        Return the start/stop times in milliseconds since 1-1-1970
        :return: (start, stop)
        """
        return int(ntplib.ntp_to_system_time(self.start) * 1000), int(ntplib.ntp_to_system_time(self.stop) * 1000)

    def __eq__(self, other):
        return self.stop == other.stop and self.start == other.start

    def __ne__(self, other):
        return self.stop != other.stop or self.start != other.start


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

    def __ne__(self, other):
        return self.as_tuple() != other.as_tuple()

    def __hash__(self):
        return hash((self.subsite, self.node, self.sensor, self.method, self.stream_name))

    def as_dict(self):
        return {
            'subsite': self.subsite,
            'node': self.node,
            'sensor': self.sensor,
            'method': self.method,
            'stream': self.stream_name
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
    status_code = 404


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


class TimedOutException(StreamEngineException):
    """
    The limit on processing time was exceeded
    """
    status_code = 408


class WriteErrorException(StreamEngineException):
    """
    Error writing one or more files
    """
    status_code = 500


class InvalidPathException(StreamEngineException):
    """
    Invalid path was supplied.
    """
    status_code = 400


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


def read_size_config(config):
    """
    :param config:  file containing size estimates for each stream
    :return:  dictionary with size estimates
    """
    sizes = {}
    with open(config, 'rb') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            stream_name = row[0]
            psize = row[1]
            sizes[stream_name] = float(psize)
    return sizes


def find_root():
    here = os.path.dirname(__file__)
    return os.path.dirname(here)


def dict_equal(d1, d2):
    """Function to recursively check if two dicts are equal"""
    if isinstance(d1, dict) and isinstance(d2, dict):
        # check keysets
        if set(d1) != set(d2):
            return False

        # otherwise loop through all the keys and check if the dicts and items are equal
        return all((dict_equal(d1[key], d2[key]) for key in d1))

    # check equality on other objects
    else:
        return d1 == d2


def _sort_dicts_in_list(data_list, key_order, sorted_first, alphabetize, key_stack):
    """
    Helper function for sorting dictionaries; takes a list and if it contains any dictionaries, sorts those according
    to the key_order passed in and returns the updated list.
    
    :param data_list: the list potentially containing dictionaries to be sorted
    :param key_order: a list with dictionary keys, listed in desired order
    :param sorted_first: whether key-value pairs whose keys are not found in key_order should come before or after the
                         pairs that are represented - True indicates before (i.e. sorted pairs come before unsorted
                         ones)
    :param alphabetize: if True, key-value pairs without their keys in key_order will be sorted alphabetically
    :param key_stack: a list acting as a stack to track what level of the dictionary is currently being sorted; used
                      for recursive calls
    :return: the list updated so that any embedded dictionaries are sorted
    """
    for index in range(len(data_list)):
        item = data_list[index]
        if isinstance(item, dict):
            data_list[index] = sort_dict(item, key_order, sorted_first, alphabetize, key_stack)
        elif isinstance(item, list):
            # recursive call to keep looking for embedded dictionaries
            data_list[index] = _sort_dicts_in_list(item, key_order, sorted_first, alphabetize, key_stack)
    return data_list
        

def sort_dict(data_dict, key_order, sorted_first=False, alphabetize=True, key_stack=[]):
    """
    Create an OrderedDict from an unsorted dictionary using specified key ordering. The original dictionary is not
    changed. Note: key-value pairs whose keys are not specified in key_order will still appear, but will not be sorted.
    Nested dictionaries and nested lists containing dictionaries are handled.
    
    :param data_dict: the dictionary to be sorted
    :param key_order: a list with keys from the dictionary, listed in desired order. in the case of nested dictionaries,
                      the parent key(s) should be included as a prefix followed by a period (e.g. 'key1.key2.key3' to
                      represent 'key3' in the dict {'key1': {'key2': {'key3' : 'foo'}}}). No prefix is added for a
                      dictionary in a list (e.g. 'key1.key2.key3' to represent 'key3' in
                      {'key1': [1, {'key2': {'key3'}, 'a']}.
    :param sorted_first: whether key-value pairs whose keys are not found in key_order should come before or after the
                         pairs that are represented - True indicates before (i.e. sorted pairs come before unsorted
                         ones)
    :param alphabetize: if True, key-value pairs without their keys in key_order will be sorted alphabetically
    :param key_stack: a list acting as a stack to track what level of the dictionary is currently being sorted; used for
                      recursive calls only - should be allowed to default to empty
    :return: an OrderedDict representing the original dictionary after sorting
    """
    if alphabetize:
        # sort data_dict by keys alphabetically
        result = OrderedDict(sorted(data_dict.items()))
    else:
        result = OrderedDict(data_dict)

    # recursively sort lower levels (nested dictionaries) first
    for key, value in result.items():
        # lower level detected - sort it and add back to result
        if isinstance(value, dict):
            # add key to a stack to track what level we are at - used to determine what elements in key_order are
            # applicable at a given level of the dictionary
            key_stack.append(key)
            result[key] = sort_dict(value, key_order, sorted_first, alphabetize, key_stack)
            # we are back up a level after the recursive call, so remove the key from the stack
            key_stack.pop()
        elif isinstance(value, list):
            # recursively sort any dictionaries in the list
            key_stack.append(key)
            _sort_dicts_in_list(value, key_order, sorted_first, alphabetize, key_stack)
            key_stack.pop()
    
    # sort current level
    
    # get the keys relevant to this "level" of the dictionary by looking at their prefix - prefix will show parent keys
    # (i.e. parent objects in JSON)
    prefix = ".".join(key_stack) + "."
    # at the top level, there is no prefix
    if prefix == ".":
        new_order = [x for x in key_order if "." not in x]
    else:
        new_order = [x.replace(prefix, "") for x in key_order if prefix in x]
    order_dict = {k: v for v, k in enumerate(new_order)}
    
    # use the default in order_dict.get() to control whether unsorted key-value pairs come before or after the sorted
    # pairs
    default = len(order_dict) if sorted_first else None
    result = OrderedDict(sorted(result.items(), key=lambda j: order_dict.get(j[0], default)))
    return result
