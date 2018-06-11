import json
import logging
import os
import signal
import time
from functools import wraps
from io import BytesIO

from flask import request, Response, jsonify, send_file

import util.aggregation
import util.calc
from engine import app
from util.common import (StreamEngineException, TimedOutException, MissingDataException,
                         MissingTimeException, ntp_to_datestring, StreamKey, InvalidPathException)
from util.san import onload_netCDF, SAN_netcdf
from util.releasenotes import ReleaseNotes

log = logging.getLogger(__name__)

release = ReleaseNotes.instance()
log.info('Starting {} v{} {} ({})'.format(
    release.component_name(),
    release.latest_version(),
    release.latest_descriptor(),
    release.latest_date()))

@app.errorhandler(Exception)
def handle_exception(error):
    request_id = request.get_json().get('requestUUID')
    if isinstance(error, StreamEngineException):
        error_dict = error.to_dict()
        error_dict['requestUUID'] = request_id
        status_code = error.status_code
    else:
        log.exception('error during request')
        msg = "Unexpected internal error during request"
        error_dict = {'message': msg, 'requestUUID': request_id}
        status_code = 500
    response = jsonify(error_dict)
    response.status_code = status_code
    log.info("Returning exception: %s", error_dict)
    return response, status_code


@app.before_request
def log_request():
    data = request.get_json()
    request_id = data.get('requestUUID')
    streams = data.get('streams')
    if log.isEnabledFor(logging.DEBUG):
        log.debug('<%s> Incoming request url=%r data=%r', request_id, request.url, data)
    else:
        log.info('<%s> Handling request to %r - %r', request_id, request.url, streams)


# noinspection PyUnusedLocal
def signal_handler(signum, frame):
    raise TimedOutException("Data processing timed out after %s seconds")


def set_timeout(timeout=None):
    # If timeout is a supplied argument to the wrapped function
    # use it, otherwise use the default timeout
    if timeout is None or not isinstance(timeout, int):
        timeout = app.config['REQUEST_TIMEOUT_SECONDS']

    def inner(func):
        @wraps(func)
        def decorated_function(*args, **kwargs):
            signal.signal(signal.SIGALRM, signal_handler)
            signal.alarm(timeout)
            try:
                result = func(*args, **kwargs)
            except TimedOutException:
                raise StreamEngineException("Data processing timed out after %s seconds" %
                                            timeout, status_code=408)
            finally:
                signal.alarm(0)

            return result

        return decorated_function
    return inner


def write_file_with_content(base_path, file_path, content):
    # check directory exists - if not create
    if not os.path.exists(base_path):
        try:
            os.makedirs(base_path)
        except OSError:
            pass

    # if base_path is dir, then write file, otherwise error out
    if os.path.isdir(base_path):
        with open(file_path, 'w') as f:
            f.write(content)
            return True
    else:
        return False


def write_status(path, filename="status.txt", status="complete"):
    if not os.path.exists(path):
        os.makedirs(path)
    with open(os.path.join(path, filename), 'w') as s:
        s.write(status + os.linesep)


def time_prefix_filename(ntp_start, ntp_stop, suffix):
    sdate = ntp_to_datestring(ntp_start)
    edate = ntp_to_datestring(ntp_stop)
    s = "-"
    status_filename = s.join((sdate.translate(None, '-:'), edate.translate(None, '-:'), suffix))
    return status_filename


def fix_directory(input_data):
    # TODO
    # TEMP FIX UNTIL UFRAME IS UPDATED TO PASS RELATIVE PATH
    directory = input_data.pop('directory', None)
    if directory:
        input_data['directory'] = '/'.join(directory.split('/')[-2:])
    return input_data


def raise_invalid_path(base_dir, path):
    """
    Given a base directory and a path, return true if path resolves under this directory.
    Return false if the path escapes the base directory
    :param base_dir:
    :param path:
    :return:
    """
    base_real = os.path.realpath(base_dir)
    path_real = os.path.realpath(os.path.join(base_dir, path))
    if not path_real.startswith(base_real + os.sep):
        raise InvalidPathException('Supplied path: %r does not resolve under base path: %r' % (path, base_dir))


def get_local_dir(input_data):
    input_data = fix_directory(input_data)
    base_dir = app.config['LOCAL_ASYNC_DIR']
    request_dir = input_data.get('directory')
    if request_dir is None:
        raise InvalidPathException('Supplied path: %r is not valid' % request_dir)
    raise_invalid_path(base_dir, request_dir)
    return os.path.join(base_dir, request_dir)


##########################
# ROUTES
##########################

# All requests to stream engine should be in the following format (any exceptions noted in the route)
#
# {
#     'streams': [
#         {
#             'subsite': subsite,
#             'node': node,
#             'sensor': sensor,
#             'method': method,
#             'stream': stream,
#             'parameters': [...],
#         },
#         ...
#     ],
#     'coefficients': {
#         'CC_a0': [
#             { 'start': ntptime, 'stop': ntptime, 'value': 1.0 },
#             ...
#         ],
#         ...
#     },
#     'start': ntptime,
#     'stop': ntptime
# }


@app.route('/estimate', methods=['POST'])
@set_timeout()
def estimate():
    """
    Return an estimate for how big the request would be if processed
    """
    input_data = request.get_json()
    return jsonify(util.calc.get_estimate(input_data, request.url))


@app.route('/particles', methods=['POST'])
@set_timeout()
def particles():
    """
    Return the results of this query as JSON
    """
    input_data = request.get_json()
    return Response(util.calc.get_particles(input_data, request.url), mimetype='application/json')


@app.route('/netcdf', methods=['POST'])
@set_timeout()
def netcdf():
    """
    Return the results of this query as netCDF
    """
    input_data = request.get_json()
    input_data['directory'] = None  # Set to synchronous mode
    stream_name, zip_data = util.calc.get_netcdf(input_data, request.url)
    return send_file(BytesIO(zip_data),
                     attachment_filename='%s.zip' % stream_name,
                     mimetype='application/octet-stream')


@app.route('/csv', methods=['POST'])
@set_timeout()
def csv():
    """
    Return the results of this query as CSV
    """
    input_data = request.get_json()
    stream_name, csv_data = util.calc.get_csv(input_data, request.url, delimiter=',')
    return send_file(BytesIO(csv_data),
                     attachment_filename='%s.zip' % stream_name,
                     mimetype='application/octet-stream')


@app.route('/tab', methods=['POST'])
@set_timeout()
def tab():
    """
    Return the results of this query as TSV
    """
    input_data = request.get_json()
    stream_name, csv_data = util.calc.get_csv(input_data, request.url, delimiter='\t')
    return send_file(BytesIO(csv_data),
                     attachment_filename='%s.zip' % stream_name,
                     mimetype='application/octet-stream')


@app.route('/netcdf-fs', methods=['POST'])
@set_timeout()
def netcdf_save_to_filesystem():
    """
    Save the requested data to the filesystem as netCDF, return the result of the query as JSON
    """
    input_data = request.get_json()
    base_path = get_local_dir(input_data)

    try:
        _, json_str = util.calc.get_netcdf(input_data, request.url, base_path)
    except Exception as e:
        json_efile = time_prefix_filename(input_data.get('start'), input_data.get('stop'), "failure.json")
        json_str = output_async_error(input_data, e, filename=json_efile)

    status_filename = time_prefix_filename(input_data.get('start'), input_data.get('stop'), "status.txt")
    write_status(base_path, filename=status_filename)
    return Response(json_str, mimetype='application/json')


@app.route('/particles-fs', methods=['POST'])
@set_timeout()
def particles_save_to_filesystem():
    """
    Save the requested data to the filesystem as JSON, return the result of the query as JSON
    :return: JSON object:
    """
    input_data = request.get_json()
    base_path = get_local_dir(input_data)

    try:
        json_str = util.calc.get_particles_fs(input_data, request.url, base_path)
    except Exception as e:
        json_efile = time_prefix_filename(input_data.get('start'), input_data.get('stop'), "failure.json")
        json_str = output_async_error(input_data, e, filename=json_efile)

    status_filename = time_prefix_filename(input_data.get('start'), input_data.get('stop'), "status.txt")
    write_status(base_path, filename=status_filename)
    return Response(json_str, mimetype='application/json')


@app.route('/csv-fs', methods=['POST'])
@set_timeout()
def csv_save_to_filesystem():
    """
    Save the query results to the filesystem as CSV, return the overall status as JSON
    :return: JSON object:
    """
    return _delimited_fs(',')


@app.route('/tab-fs', methods=['POST'])
@set_timeout()
def tab_save_to_filesystem():
    """
    Save the query results to the filesystem as TSV, return the overall status as JSON
    :return: JSON object:
    """
    return _delimited_fs('\t')


def _delimited_fs(delimiter):
    """
    Given a delimiter X, write the XSV output to the filesystem, return the overall status as JSON
    :param delimiter:
    :return: JSON response
    """
    input_data = request.get_json()
    base_path = get_local_dir(input_data)

    try:
        json_str = util.calc.get_csv_fs(input_data, request.url, base_path, delimiter=delimiter)
    except Exception as e:
        json_efile = time_prefix_filename(input_data.get('start'), input_data.get('stop'), "failure.json")
        json_str = output_async_error(input_data, e, filename=json_efile)

    status_filename = time_prefix_filename(input_data.get('start'), input_data.get('stop'), "status.txt")
    write_status(base_path, filename=status_filename)
    return Response(json_str, mimetype='application/json')


@app.route('/aggregate', methods=['POST'])
@set_timeout(timeout=app.config.get('AGGREGATION_TIMEOUT_SECONDS'))
def aggregate_async():
    if app.config['AGGREGATE']:
        input_data = request.get_json()
        async_job = input_data.get("async_job")
        request_id = input_data.get("requestUUID")

        # Verify the supplied path is valid before proceeding
        raise_invalid_path(app.config['FINAL_ASYNC_DIR'], async_job)

        log.info("<%s> Performing aggregation on asynchronous job %s", request_id, async_job)
        st = time.time()
        util.aggregation.aggregate(async_job, request_id=request_id)
        et = time.time() - st
        log.info("<%s> Done performing aggregation on asynchronous job %s took %s seconds", request_id, async_job, et)
        message = "aggregation complete"
    else:
        message = "aggregation disabled"

    return jsonify({'code': 200, 'message': message})


@app.route('/san_offload', methods=['POST'])
@set_timeout()
def full_netcdf():
    """
    POST should contain a dictionary of the following format:
    {
        'streams': [
            {
                'subsite': subsite,
                'node': node,
                'sensor': sensor,
                'method': method,
                'stream': stream,
                'parameters': [...],
            },
            ...
        ],
        'bins' : [
            11212,
            11213,
            11315,
        ],

    }

    :return: Object which contains results for the data SAN:
             {
                message: "A message"
                results : An array of booleans with the status of each offload
             }
    """
    input_data = request.get_json()
    rp = util.calc.validate(input_data)
    bins = input_data.get('bins', [])
    log.info("Handling request to offload stream: %s bins: %s", input_data.get('streams', ""), bins)
    results, message = SAN_netcdf(input_data.get('streams'), bins, rp.id)
    resp = {'results': results, 'message': message}
    response = Response(json.dumps(resp), mimetype='application/json')
    return response


@app.route('/san_onload', methods=['POST'])
@set_timeout()
def onload_netcdf():
    """
    Post should contain the fileName : string file name to onload
    :return:
    """
    input_data = request.get_json()
    file_name = input_data.get('fileName')
    if file_name is None:
        return Response('"Error no file provided"', mimetype='text/plain')
    else:
        log.info("Onloading netCDF file: %s from SAN to Cassandra", file_name)
        resp = onload_netCDF(file_name)
        return Response('"{:s}"'.format(resp), mimetype='text/plain')


@app.route('/needs', methods=['POST'])
@set_timeout()
def needs():
    """
    Given a list of reference designators, streams and parameters, return the
    needed calibration constants for each reference designator
    and the data products which can be computed. Data products which
    are missing L0 data shall not be returned.

    Currently no validation on time frames is provided. If the necessary L0
    data from any time frame is available then this method will return that
    product in the list of parameters. When the algorithm is run, a determination
    if the needed data is present will be made.

    Note, this method may return more reference designators than specified
    in the request, should more L0 data be required.

    POST should contain a dictionary of the following format:
    {
        'streams': [
            {
                'subsite': subsite,
                'node': node,
                'sensor': sensor,
                'method': method,
                'stream': stream,
                'parameters': [...],
            },
            ...
            ]
    }

    :return: JSON object:
    {
        'streams': [
            {
                'subsite': subsite,
                'node': node,
                'sensor': sensor,
                'method': method,
                'stream': stream,
                'coefficients': [...],
                'parameters': [...],
            },
            ...
            ]
    }
    """
    input_data = request.get_json()
    stream_needs = input_data['streams']
    stream_needs[0]['parameters'] = []
    return jsonify({'streams': stream_needs})


def output_async_error(input_data, e, filename="failure.json"):
    output = {
        "code": 500,
        "message": "Request for particles failed for the following reason: %s" % e.message,
        'requestUUID': input_data.get('requestUUID', '')
    }
    base_path = get_local_dir(input_data)
    # try to write file, if it does not succeed then return an additional error
    json_str = json.dumps(output, indent=2, separators=(',', ': '))
    if not write_file_with_content(base_path, os.path.join(base_path, filename), json_str):
        msg = "%s. Supplied directory '%s' is invalid. Path specified exists but is not a directory." % (
            output['message'], base_path)
        output['message'] = msg
    json_str = json.dumps(output, indent=2, separators=(',', ': '))
    log.error(json_str)
    return json_str
