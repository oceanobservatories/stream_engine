import logging
import os
from collections import namedtuple
from functools import wraps

import ion_functions
import time

import ntplib

import util.stream_request
from jsonresponse import JsonResponse
from ooi_data.postgres.model import Stream, Parameter
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
                                                     'qc_parameters', 'strict_range', 'location_information',
                                                     'execute_dpa', 'require_deployment'])


def execute_stream_request(request_parameters, needs_only=False):
    stream_request = []

    for index, stream in enumerate(request_parameters.streams):
        stream_key = StreamKey.from_dict(request_parameters.streams[index])
        parameters = request_parameters.streams[index].get('parameters', [])
        time_range = TimeRange(request_parameters.start, request_parameters.stop)
        collapse_times = not needs_only

        stream_request.append(util.stream_request.StreamRequest(
            stream_key, parameters, time_range, request_parameters.uflags,
            qc_parameters=request_parameters.qc_parameters,
            limit=request_parameters.limit,
            include_provenance=request_parameters.include_provenance,
            include_annotations=request_parameters.include_annotations,
            strict_range=request_parameters.strict_range,
            request_id=request_parameters.id,
            collapse_times=collapse_times,
            execute_dpa=request_parameters.execute_dpa,
            require_deployment=request_parameters.require_deployment))

        if not needs_only:
            stream_request[index].fetch_raw_data()
            if request_parameters.execute_dpa:
                stream_request[index].calculate_derived_products()
                stream_request[index].import_extra_externals()
            stream_request[index].execute_qc()
            stream_request[index].insert_provenance()
        else:
            # If needs_only is true we only want to process the first stream, for now
            break

        if index > 0:
            stream_request[0].interpolate_from_stream_request(stream_request[index])

    return stream_request[0]


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
def get_estimate(input_data, url):
    stream_request = execute_stream_request(validate(input_data), needs_only=True)
    filesize = stream_request.compute_request_size()
    return {
        'size': filesize,
        'time': stream_request.compute_request_time(filesize)
    }

@time_request
def get_particles(input_data, url):
    stream_request = execute_stream_request(validate(input_data))
    return JsonResponse(stream_request).json()


@time_request
def get_netcdf(input_data, url):
    disk_path = input_data.get('directory', 'unknown')
    classic = input_data.get('classic', False)
    stream_request = execute_stream_request(validate(input_data))
    return stream_request.stream_key.stream.name, NetcdfGenerator(stream_request, classic, disk_path).write()


@time_request
def get_csv(input_data, url, delimiter=','):
    stream_request = execute_stream_request(validate(input_data))
    return stream_request.stream_key.stream.name, CsvGenerator(stream_request, delimiter).to_csv()


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
    if limit > app.config['UI_HARD_LIMIT']:
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
    execute_dpa = input_data.get('execute_dpa', True)
    require_deployment = input_data.get('require_deployment', app.config["REQUIRE_DEPLOYMENT"])

    return RequestParameters(request_id, streams, coefficients, user_flags, start,
                             stop, limit, prov, annotate, qc, strict, locs, execute_dpa, require_deployment)


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

    for index, each in enumerate(streams):
        _validate_stream(each, index > 0)

    return streams


def _validate_stream(stream, empty_param_check=False):
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

    if empty_param_check and len(parameters) == 0:
        raise InvalidParameterException('The parameter list for the secondary stream is empty',
                                            payload={'stream': stream})

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
    keys = ['userName', 'advancedStreamEngineLogging', 'requestTime']
    user_flags = {k: input_data.get(k) for k in keys}

    return user_flags
