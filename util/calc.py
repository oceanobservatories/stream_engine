import logging
import os
from collections import namedtuple

import ion_functions
import time

import ntplib

import util.stream_request
from jsonresponse import JsonResponse
from preload_database.model.preload import Stream, Parameter
from util.common import (log_timing, StreamKey, TimeRange, MalformedRequestException, InvalidStreamException,
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
    stream_request = util.stream_request.StreamRequest(stream_key, parameters, request_parameters.coefficients,
                                                       time_range, request_parameters.uflags,
                                                       qc_parameters=request_parameters.qc_parameters,
                                                       limit=request_parameters.limit,
                                                       include_provenance=request_parameters.include_provenance,
                                                       include_annotations=request_parameters.include_annotations,
                                                       strict_range=request_parameters.strict_range,
                                                       location_information=request_parameters.location_information,
                                                       request_id=request_parameters.id)
    if not needs_only:
        stream_request.fetch_raw_data()
        stream_request.calculate_derived_products()
    return stream_request


def get_particles(input_data, url):
    rp = validate(input_data)
    request_start_time = time.time()

    log.info("<%s> Handling request to %s - %s", rp.id, url, rp.streams)
    stream_request = execute_stream_request(rp)
    response = JsonResponse(stream_request).json()

    log.info("<%s> Request took %.2fs to complete", rp.id, time.time() - request_start_time)
    return response


def get_netcdf(input_data, url):
    rp = validate(input_data)
    disk_path = input_data.get('directory', 'unknown')
    classic = input_data.get('classic', False)
    request_start_time = time.time()

    log.info("<%s> Handling request to %s - %s", rp.id, url, rp.streams)
    stream_request = execute_stream_request(rp)
    response = NetcdfGenerator(stream_request, classic, disk_path).write()

    log.info("<%s> Request took %.2fs to complete", rp.id, time.time() - request_start_time)
    return response


def get_csv(input_data, url, delimiter=','):
    rp = validate(input_data)

    request_start_time = time.time()
    log.info("<%s> Handling request to %s - %s", rp.id, url, rp.streams)
    stream_request = execute_stream_request(rp)
    response = CsvGenerator(stream_request, delimiter).to_csv()
    log.info("Request took {:.2f}s to complete".format(time.time() - request_start_time))
    return response


def get_csv_fs(input_data, url, base_path, delimiter=','):
    rp = validate(input_data)

    request_start_time = time.time()
    log.info("<%s> Handling request to %s - %s", rp.id, url, rp.streams)
    stream_request = execute_stream_request(rp)
    response = CsvGenerator(stream_request, delimiter).to_csv_files(base_path)
    log.info("<%s> Request took %.2fs to complete", rp.id, time.time() - request_start_time)
    return response


def get_needs(input_data, url):
    rp = validate(input_data)
    request_start_time = time.time()

    log.info("<%s> Handling request to %s - %s", rp.id, url, rp.streams)
    stream_request = execute_stream_request(rp, needs_only=True)

    log.info("<%s> Request took %.2fs to complete", rp.id, time.time() - request_start_time)
    return stream_request.needs_cc


def validate(input_data):
    if input_data is None:
        raise MalformedRequestException('Received NULL input data')

    request_id = input_data.get('requestUUID', '')
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
    if len(missing) > 0:
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
