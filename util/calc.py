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
from util.common import (get_annotation_filename, StreamKey, TimeRange, MalformedRequestException, 
                         InvalidStreamException, InvalidParameterException, UIHardLimitExceededException, 
                         MissingDataException)
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
                                                     'execute_dpa', 'require_deployment', 'raw_data_only'])


def execute_stream_request(request_parameters, needs_only=False, base_path=None):
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
            require_deployment=request_parameters.require_deployment,
            raw_data_only=request_parameters.raw_data_only))

        if not needs_only:
            # compute annotations before fetching data so they are available if a MissingDataException is thrown
            stream_request[index].insert_annotations()
            try:
                stream_request[index].fetch_raw_data()
                if request_parameters.raw_data_only:
                    continue
            except MissingDataException:
                _write_annotations(stream_request[index], base_path)
                # reraise the MissingDataException for handling elsewhere (e.g. reporting to user)
                raise
            if request_parameters.execute_dpa:
                stream_request[index].calculate_derived_products()
                stream_request[index].import_extra_externals()
            stream_request[index].execute_qc()
            stream_request[index].execute_qartod_qc()
            stream_request[index].insert_provenance()
        else:
            # If needs_only is true we only want to process the first stream, for now
            break

        if index > 0:
            stream_request[0].interpolate_from_stream_request(stream_request[index])

    stream_request[0].rename_parameters()
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


def get_stats(async_job_dir):
    # sum the size of the files in the output directory
    files = [os.path.join(async_job_dir, f) for f in os.listdir(async_job_dir)]
    download_size = sum(os.path.getsize(f) for f in files if os.path.isfile(f))

    # the status.txt file is written at the very end of aggregation and is the canonical way to determine that an async
    # job is done, so its last modified timestamp indicates request completion time for successful requests
    status_file = os.path.join(async_job_dir, "status.txt")
    if os.path.isfile(status_file):
        completion_time = os.path.getmtime(status_file)
    else:
        # for some reason status.txt was not written - search instead for the most recently modified file time
        completion_time = max(os.path.getmtime(f) for f in files if os.path.isfile(f))

    return {
        'size': download_size,
        'time': completion_time * 1000  # milliseconds since epoch timestamp, not an elapsed time since request start
    }


@time_request
def get_particles(input_data, url):
    stream_request = execute_stream_request(validate(input_data))
    return JsonResponse(stream_request).json()


@time_request
def get_particles_fs(input_data, url, base_path=None):
    stream_request = execute_stream_request(validate(input_data), base_path=base_path)
    return JsonResponse(stream_request).write_json(base_path)


@time_request
def get_netcdf(input_data, url, base_path=None):
    disk_path = input_data.get('directory', 'unknown')
    classic = input_data.get('classic', False)
    stream_request = execute_stream_request(validate(input_data), base_path=base_path)
    return stream_request.stream_key.stream.name, NetcdfGenerator(stream_request, classic, disk_path).write()


@time_request
def get_csv(input_data, url, delimiter=','):
    stream_request = execute_stream_request(validate(input_data))
    return stream_request.stream_key.stream.name, CsvGenerator(stream_request, delimiter).to_csv()


@time_request
def get_csv_fs(input_data, url, base_path, delimiter=','):
    stream_request = execute_stream_request(validate(input_data), base_path=base_path)
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
    raw_data_only = input_data.get('raw_data_only', False)
    if raw_data_only:
        execute_dpa = False

    return RequestParameters(request_id, streams, coefficients, user_flags, start,
                             stop, limit, prov, annotate, qc, strict, locs, execute_dpa,
                             require_deployment, raw_data_only)


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


def _write_annotations(stream_request, base_path):
    """
    Write stream request annotation data to a JSON file. This function will do nothing for synchronous requests 
    (i.e. if 'limit' is not None).
    
    WARNING: This function should only be used when a MissingDataException prevents the execution of a normal 
    file writing class (netcdf_generator, csvresponse, or jsonresponse).
    """
    # don't process synchronous requests or those with unspecified output paths
    if stream_request.limit is not None or base_path is None:
        return
    
    if stream_request.include_annotations:
        anno_fname = get_annotation_filename(stream_request)
        anno_json = os.path.join(base_path, anno_fname)
        stream_request.annotation_store.dump_json(anno_json)


def _get_userflags(input_data):
    keys = ['userName', 'advancedStreamEngineLogging', 'requestTime']
    user_flags = {k: input_data.get(k) for k in keys}

    return user_flags
