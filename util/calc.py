import logging
import os
from collections import namedtuple
from functools import wraps

import ion_functions
import time

import ntplib

import util.stream_request
from jsonresponse import JsonResponse
from preload_database.model.preload import Stream, Parameter
from util.common import (StreamKey, TimeRange, MalformedRequestException, InvalidStreamException,
                         InvalidParameterException, UIHardLimitExceededException)
from util.csvresponse import CsvGenerator
from util.netcdf_generator import NetcdfGenerator
from engine import app

if hasattr(ion_functions, '__version__'):
    ION_VERSION = ion_functions.__version__
else:
    ION_VERSION = 'unversioned'

log = logging.getLogger(__name__)

RequestParameters = namedtuple('RequestParameters', ['id', 'streams', 'coefficients', 'uflags', 'start', 'stop',
                                                     'limit', 'include_provenance', 'include_annotations',
                                                     'qc_parameters', 'strict_range', 'location_information'])


def execute_stream_request(request_parameters, needs_only=False):
    stream_key = StreamKey.from_dict(request_parameters.streams[0])
    parameters = request_parameters.streams[0].get('parameters', [])
    time_range = TimeRange(request_parameters.start, request_parameters.stop)
    collapse_times = not needs_only
    stream_request = util.stream_request.StreamRequest(stream_key, parameters, request_parameters.coefficients,
                                                       time_range, request_parameters.uflags,
                                                       qc_parameters=request_parameters.qc_parameters,
                                                       limit=request_parameters.limit,
                                                       include_provenance=request_parameters.include_provenance,
                                                       include_annotations=request_parameters.include_annotations,
                                                       strict_range=request_parameters.strict_range,
                                                       location_information=request_parameters.location_information,
                                                       request_id=request_parameters.id,
                                                       collapse_times=collapse_times)
    if not needs_only:
        stream_request.fetch_raw_data()
        stream_request.calculate_derived_products()
        stream_request.import_extra_externals()
    return stream_request


def time_request(func):
    @wraps(func)
    def inner(*args, **kwargs):
        start = time.time()
        request_id = None
        if len(args) > 0 and isinstance(args[0], dict):
            request_id = args[0].get('requestUUID', None)
        response = func(*args, **kwargs)
        log.info("<%s> Request took %.2fs to complete", request_id, time.time() - start)
        return response
    return inner


@time_request
def get_particles(input_data, url):
    stream_request = execute_stream_request(validate(input_data))
    return JsonResponse(stream_request).json()


@time_request
def get_netcdf(input_data, url):
    disk_path = input_data.get('directory', 'unknown')
    classic = input_data.get('classic', False)
    stream_request = execute_stream_request(validate(input_data))
    return NetcdfGenerator(stream_request, classic, disk_path).write()


@time_request
def get_csv(input_data, url, delimiter=','):
    stream_request = execute_stream_request(validate(input_data))
    return CsvGenerator(stream_request, delimiter).to_csv()


@time_request
def get_csv_fs(input_data, url, base_path, delimiter=','):
    stream_request = execute_stream_request(validate(input_data))
    return CsvGenerator(stream_request, delimiter).to_csv_files(base_path)


@time_request
def get_needs(input_data, url):
    stream_request = execute_stream_request(validate(input_data), needs_only=True)
    return stream_request.needs_cc


def validate(input_data):
    if input_data is None:
        raise MalformedRequestException('Received NULL input data')

    request_id = input_data.get('requestUUID', None)
    limit = input_data.get('limit', 0)

    if limit <= 0:
        limit = None
    if limit >= app.config['UI_HARD_LIMIT']:
        message = '<{:s}> Requested number of particles ({:,d}) larger than maximum allowed limit ({:,d})'
        message = message.format(request_id, limit, app.config['UI_HARD_LIMIT'])
        raise UIHardLimitExceededException(message=message)

    streams = _validate_streams(input_data)
    coefficients = _validate_coefficients(input_data)
    user_flags = _get_userflags(input_data)

    start = input_data.get('start', app.config["UNBOUND_QUERY_START"])
    stop = input_data.get('stop', ntplib.system_to_ntp_time(time.time()))
    prov = input_data.get('include_provenance', False)
    annotate = input_data.get('include_annotations', False)
    qc = input_data.get('qcParameters', {})
    strict = input_data.get('strict_range', False)
    locs = input_data.get('locations', {})

    return RequestParameters(request_id, streams, coefficients, user_flags, start,
                             stop, limit, prov, annotate, qc, strict, locs)


def _validate_coefficients(input_data):
    coeffs = input_data.get('coefficients', {})
    if not isinstance(coeffs, dict):
        raise MalformedRequestException('Received invalid coefficient data, must be a map',
                                        payload={'coefficients': input_data.get('coefficients')})
    return coeffs


def _validate_streams(input_data):
    streams = input_data.get('streams')
    if streams is None or not isinstance(streams, list):
        raise MalformedRequestException('Received invalid request', payload={'request': input_data})

    for each in streams:
        _validate_stream(each)
    return streams


def _validate_stream(stream):
    if not isinstance(stream, dict):
        raise MalformedRequestException('Received invalid request, stream is not dictionary',
                                        payload={'request': stream})
    required = {'subsite', 'node', 'sensor', 'method', 'stream'}
    missing = required.difference(stream)
    if missing:
        raise MalformedRequestException('Missing stream information from request',
                                        payload={'request': stream})

    preload_stream = Stream.query.filter(Stream.name == stream['stream']).first()
    if preload_stream is None:
        raise InvalidStreamException('The requested stream does not exist in preload', payload={'stream': stream})

    parameters = stream.get('parameters', [])
    stream_parameters = [p.id for p in preload_stream.parameters]
    for pid in parameters:
        p = Parameter.query.get(pid)
        if p is None:
            raise InvalidParameterException('The requested parameter does not exist in preload',
                                            payload={'id': pid})

        if pid not in stream_parameters:
            raise InvalidParameterException('The requested parameter does not exist in this stream',
                                            payload={'id': pid, 'stream': stream})


def _get_userflags(input_data):
    keys = ['userName', 'advancedStreamEngineLogging']
    user_flags = {k: input_data.get(k) for k in keys}
    return user_flags
