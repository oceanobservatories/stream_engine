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
                         MissingTimeException, ntp_to_datestring, StreamKey)
from util.san import onload_netCDF, SAN_netcdf

log = logging.getLogger(__name__)


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
    return response, 500


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


def set_timeout(func):
    @wraps(func)
    def decorated_function(*args, **kwargs):
        signal.signal(signal.SIGALRM, signal_handler)
        signal.alarm(int(app.config['REQUEST_TIMEOUT_SECONDS']))
        try:
            result = func(*args, **kwargs)
        finally:
            signal.alarm(0)

        return result
    return decorated_function


def write_file_with_content(base_path, file_path, content):
    # check directory exists - if not create
    if not os.path.exists(base_path):
        os.makedirs(base_path)
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
    status_filename = s.join((sdate.translate(None, '-:'),edate.translate(None, '-:'), suffix))
    return status_filename


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
@set_timeout
def estimate():
    """
    Return an estimate for how big the request would be if processed
    """
    input_data = request.get_json()
    return jsonify(util.calc.get_estimate(input_data, request.url))


@app.route('/particles', methods=['POST'])
@set_timeout
def particles():
    """
    Return the results of this query as JSON
    """
    input_data = request.get_json()
    return Response(util.calc.get_particles(input_data, request.url), mimetype='application/json')


@app.route('/netcdf', methods=['POST'])
@set_timeout
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
@set_timeout
def csv():
    """
    Return the results of this query as CSV
    """
    input_data = request.get_json()
    stream_name, csv_data = util.calc.get_csv(input_data, request.url, delimiter=',')
    return send_file(BytesIO(csv_data),
                     attachment_filename='%s.csv' % stream_name,
                     mimetype='application/csv')


@app.route('/tab', methods=['POST'])
@set_timeout
def tab():
    """
    Return the results of this query as TSV
    """
    input_data = request.get_json()
    stream_name, csv_data = util.calc.get_csv(input_data, request.url, delimiter='\t')
    return send_file(BytesIO(csv_data),
                     attachment_filename='%s.tsv' % stream_name,
                     mimetype='application/tab-separated-values')


@app.route('/netcdf-fs', methods=['POST'])
@set_timeout
def netcdf_save_to_filesystem():
    """
    Save the requested data to the filesystem as netCDF, return the result of the query as JSON
    """
    input_data = request.get_json()
    base_path = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'], input_data.get('directory', 'unknown'))

    try:
        _, json_str = util.calc.get_netcdf(input_data, request.url)
    except Exception as e:
        json_efile = time_prefix_filename(input_data.get('start'), input_data.get('stop'), "failure.json")
        json_str = output_async_error(input_data, e, filename=json_efile)

    status_filename = time_prefix_filename(input_data.get('start'), input_data.get('stop'), "status.txt")

    write_status(base_path, filename=status_filename)
    return Response(json_str, mimetype='application/json')


@app.route('/particles-fs', methods=['POST'])
@set_timeout
def particles_save_to_filesystem():
    """
    Save the requested data to the filesystem as JSON, return the result of the query as JSON
    :return: JSON object:
    """
    input_data = request.get_json()
    base_path = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'], input_data.get('directory', 'unknown'))
    filename = '{:s}.json'.format(StreamKey.from_dict(input_data.get('streams')[0]).as_dashed_refdes())
    file_path = os.path.join(base_path, filename)

    code = 200
    message = str([file_path])

    try:
        json_output = util.calc.get_particles(input_data, request.url)
    except (MissingDataException, MissingTimeException) as e:
        # treat as empty
        log.warning(e)
        # set contents of stream.json to empty
        json_output = json.dumps({})
    except Exception as e:
        # set code to error
        code = 500
        # make message be the error code
        message = "Request for particles failed for the following reason: " + e.message
        # set the contents of failure.json
        json_output = json.dumps({'code': 500, 'message': message, 'requestUUID': input_data.get('requestUUID', '')})
        filename = time_prefix_filename(input_data.get('start'), input_data.get('stop'), "failure.json")
        file_path = os.path.join(base_path, filename)
        log.exception(json_output)

    # try to write file, if it does not succeed then return an error
    if not write_file_with_content(base_path, file_path, json_output):
        # if the code is 500, append message, otherwise replace it with this error
        message = "%sSupplied directory '%s' is invalid. Path specified exists but is not a directory." \
                  % (message + ". " if code == 500 else "", base_path)
        code = 500

    write_status(base_path)

    return Response(json.dumps({'code': code, 'message': message}, indent=2), mimetype='application/json')


@app.route('/csv-fs', methods=['POST'])
@set_timeout
def csv_save_to_filesystem():
    """
    Save the query results to the filesystem as CSV, return the overall status as JSON
    :return: JSON object:
    """
    return _delimited_fs(',')


@app.route('/tab-fs', methods=['POST'])
@set_timeout
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
    base_path = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'], input_data.get('directory', 'unknown'))
    try:
        json_str = util.calc.get_csv_fs(input_data, request.url, base_path, delimiter=delimiter)
    except Exception as e:
        json_str = output_async_error(input_data, e)

    write_status(base_path)
    return Response(json_str, mimetype='application/json')


@app.route('/aggregate', methods=['POST'])
@set_timeout
def aggregate_async():
    input_data = request.get_json()
    async_job = input_data.get("async_job")
    log.info("Performing aggregation on asynchronous job %s", async_job)
    st = time.time()
    util.aggregation.aggregate(async_job)
    et = time.time()
    log.info("Done performing aggregation on asynchronous job %s took %s seconds", async_job, et - st)
    return "done"


@app.route('/san_offload', methods=['POST'])
@set_timeout
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
@set_timeout
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
@set_timeout
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
    needs = input_data['streams']
    needs[0]['parameters'] = []
    return jsonify({'streams': needs})


def output_async_error(input_data, e, filename="failure.json"):
    output = {
        "code": 500,
        "message": "Request for particles failed for the following reason: %s" % e.message,
        'requestUUID': input_data.get('requestUUID', '')
    }
    base_path = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'], input_data.get('directory', 'unknown'))
    # try to write file, if it does not succeed then return an additional error
    json_str = json.dumps(output, indent=2, separators=(',', ': '))
    if not write_file_with_content(base_path, os.path.join(base_path, filename), json_str):
        msg = "%s. Supplied directory '%s' is invalid. Path specified exists but is not a directory." % (
            output['message'], base_path)
        output['message'] = msg
    json_str = json.dumps(output, indent=2, separators=(',', ': '))
    log.exception(json_str)
    return json_str
