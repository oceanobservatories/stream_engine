import json
import time
import logging
import os

import ntplib
from flask import request, Response, jsonify

from engine import app
from preload_database.model.preload import Stream
import util.calc
from util.cass import stream_exists, time_to_bin, bin_to_time
from util.common import CachedParameter, StreamEngineException, MalformedRequestException, \
    InvalidStreamException, StreamUnavailableException, InvalidParameterException, ISO_to_ntp, ntp_to_ISO_date, \
    StreamKey, MissingDataException, MissingTimeException
from util.san import onload_netCDF, SAN_netcdf


log = logging.getLogger(__name__)


@app.errorhandler(StreamEngineException)
def handle_stream_not_found(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code

    log.info("Returning exception: {}".format(error.to_dict()))
    return response

@app.errorhandler(Exception)
def handle_stream_not_found(error):
    msg = "Unexpected internal error during request"
    log.exception(msg)
    return '{{\n  "message": "{}"\n}}'.format(msg)


@app.before_request
def log_request():
    log.info('Incoming request url=%s data=%s', request.url, request.data)


@app.route('/particles', methods=['POST'])
def particles():
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
        'coefficients': {
            'CC_a0': [
                { 'start': ntptime, 'stop': ntptime, 'value': 1.0 },
                ...
            ],
            ...
        },
        'start': ntptime,
        'stop': ntptime
    }

    :return: JSON object:
    """
    input_data = request.get_json()
    validate(input_data)

    request_start_time = time.time()
    log.info("Handling request to {} - {}".format(request.url, input_data.get('streams', "")))

    start = input_data.get('start', app.config["UNBOUND_QUERY_START"])
    stop = input_data.get('stop', ntplib.system_to_ntp_time(time.time()))
    limit = input_data.get('limit', 0)
    if limit <= 0:
        limit = None

    prov = input_data.get('include_provenance', False)
    annotate = input_data.get('include_annotations', False)
    resp = Response(util.calc.get_particles(input_data.get('streams'), start, stop, input_data.get('coefficients', {}),
                    input_data.get('qcParameters', {}), limit=limit,
                    custom_type=input_data.get('custom_type'), include_provenance=prov, include_annotations=annotate ,
                    strict_range=input_data.get('strict_range', False), request_uuid=input_data.get('requestUUID','')),
                mimetype='application/json')

    log.info("Request took {:.2f}s to complete".format(time.time() - request_start_time))
    return resp


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


@app.route('/particles-fs', methods=['POST'])
def particles_save_to_filesystem():
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
        'coefficients': {
            'CC_a0': [
                { 'start': ntptime, 'stop': ntptime, 'value': 1.0 },
                ...
            ],
            ...
        },
        'start': ntptime,
        'stop': ntptime
    }

    :return: JSON object:
    """
    input_data = request.get_json()
    validate(input_data)

    request_start_time = time.time()
    log.info("Handling request to {} - {}".format(request.url, input_data.get('streams', "")))

    start = input_data.get('start', app.config["UNBOUND_QUERY_START"])
    stop = input_data.get('stop', ntplib.system_to_ntp_time(time.time()))
    limit = input_data.get('limit', 0)
    if limit <= 0:
        limit = None

    prov = input_data.get('include_provenance', False)
    annotate = input_data.get('include_annotations', False)
    # output is sent to filesystem, the directory will be supplied via endpoint, in case it is not, use a backup
    # NOTE(uFrame): If a backup is being used, there's likely a bug in uFrame, so best to track that down as soon as possible
    base_path = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'],
                             input_data.get('directory','unknown/%0f-%s' % (start, input_data.get('requestUUID', 'unknown'))))
    streams = input_data.get('streams')
    fn = '%s.json' % (StreamKey.from_dict(streams[0]).stream_name)
    code = 200
    file_path = os.path.join(base_path, fn)
    message = str([file_path])
    try:
        # query for the contents of stream.json
        json_output = util.calc.get_particles(streams, start, stop, input_data.get('coefficients', {}),
                        input_data.get('qcParameters', {}), limit=limit, custom_times=input_data.get('custom_times'),
                        custom_type=input_data.get('custom_type'), include_provenance=prov, include_annotations=annotate ,
                        strict_range=input_data.get('strict_range', False), request_uuid=input_data.get('requestUUID',''))
    except (MissingDataException,MissingTimeException) as e:
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
        json_output = json.dumps({ 'code': 500, 'message' : message})
        file_path = os.path.join(base_path, 'failure.json')
        log.exception(json_output)

    # try to write file, if it does not succeed then return an error
    if not write_file_with_content(base_path=base_path, file_path=file_path, content=json_output):
        # if the code is 500, append message, otherwise replace it with this error as returning a non-existent filename
        # no longer makes sense
        message = "%sSupplied directory '%s' is invalid. Path specified exists but is not a directory." \
                  % (message + ". " if code == 500 else "", base_path)
        code = 500

    log.info("Request took {:.2f}s to complete".format(time.time() - request_start_time))
    return Response(json.dumps({'code': code, 'message' : message},indent=2, separators=(',',': ')),
                    mimetype='application/json')

@app.route('/san_offload', methods=['POST'])
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
    validate(input_data)
    bins = input_data.get('bins', [])
    log.info("Handling request to offload stream: %s bins: %s", input_data.get('streams', ""), bins)
    results, message = SAN_netcdf(input_data.get('streams'), bins)
    resp = {'results': results, 'message': message}
    response = Response(json.dumps(resp),  mimetype='application/json')
    return response


@app.route('/san_onload', methods=['POST'])
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


@app.route('/netcdf', methods=['POST'])
def netcdf():
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
        'coefficients': {
            'CC_a0': [
                { 'start': ntptime, 'stop': ntptime, 'value': 1.0 },
                ...
            ],
            ...
        },
        'start': ntptime,
        'stop': ntptime
    }

    :return: JSON object:
    """
    input_data = request.get_json()
    validate(input_data)

    request_start_time = time.time()
    log.info("Handling request to {} - {}".format(request.url, input_data.get('streams', "")))

    start = input_data.get('start', app.config["UNBOUND_QUERY_START"])
    stop = input_data.get('stop', ntplib.system_to_ntp_time(time.time()))
    limit = input_data.get('limit', 0)
    if limit <= 0:
        limit = None

    prov = input_data.get('include_provenance', False)
    annotate = input_data.get('include_annotations', False)
    resp = Response(util.calc.get_netcdf(input_data.get('streams'), start, stop, input_data.get('coefficients', {}),
                                         limit=limit,
                                         custom_type=input_data.get('custom_type'), include_provenance=prov,
                                         include_annotations=annotate, request_uuid=input_data.get('requestUUID', '')),
                    mimetype='application/netcdf')

    log.info("Request took {:.2f}s to complete".format(time.time() - request_start_time))
    return resp

@app.route('/netcdf-fs', methods=['POST'])
def netcdf_save_to_filesystem():
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
        'coefficients': {
            'CC_a0': [
                { 'start': ntptime, 'stop': ntptime, 'value': 1.0 },
                ...
            ],
            ...
        },
        'start': ntptime,
        'stop': ntptime,
        'directory': directory
    }

    :return: JSON object:
    """
    input_data = request.get_json()
    validate(input_data)

    request_start_time = time.time()
    log.info("Handling request to {} - {}".format(request.url, input_data.get('streams', "")))

    start = input_data.get('start', app.config["UNBOUND_QUERY_START"])
    stop = input_data.get('stop', ntplib.system_to_ntp_time(time.time()))
    limit = input_data.get('limit', 0)
    if limit <= 0:
        limit = None

    prov = input_data.get('include_provenance', False)
    annotate = input_data.get('include_annotations', False)
    try:
        json_str = util.calc.get_netcdf(input_data.get('streams'), start, stop, input_data.get('coefficients', {}),
                                         limit=limit,
                                         custom_type=input_data.get('custom_type'), include_provenance=prov,
                                         include_annotations=annotate, request_uuid=input_data.get('requestUUID', ''),
                                         disk_path=input_data.get('directory','unknown'))
    except Exception as e:
        output = { "code" : 500, "message": "Request for particles failed for the following reason: %s" % (e.message) }
        base_path = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'],input_data.get('directory','unknown'))
         # try to write file, if it does not succeed then return an additional error
        json_str = json.dumps(output, indent=2, separators=(',',': '))
        if not write_file_with_content(base_path=base_path, file_path=os.path.join(base_path, "failure.json"), content=json_str):
            output['message'] = "%s. Supplied directory '%s' is invalid. Path specified exists but is not a directory." % (output['message'],base_path)
        json_str = json.dumps(output, indent=2, separators=(',',': '))
        log.exception(json_str)


    log.info("Request took {:.2f}s to complete".format(time.time() - request_start_time))
    return Response(json_str, mimetype='application/json')

@app.route('/needs', methods=['POST'])
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
    validate(input_data)

    request_start_time = time.time()
    log.info("Handling request to {} - {}".format(request.url, input_data.get('streams', "")))

    output_data = {'streams': util.calc.get_needs(input_data.get('streams'))}
    resp = Response(json.dumps(output_data), mimetype='application/json')

    log.info("Request took {:.2f}s to complete".format(time.time() - request_start_time))
    return resp


@app.route("/get_bins", methods=['GET'])
def get_bins():
    """Given ntp time return bins"""
    st = request.args.get('start')
    et = request.args.get('stop')
    if st is None or et is None:
        return "Need start and end time to compute bin range"
    try:
        stf = float(st)
        etf = float(et)
        start_bin = time_to_bin(stf)
        end_bin = time_to_bin(etf)
    except:
        # assuming it is in the form YY-MM-DDTHH-MM-SS.sssZ
        st = ISO_to_ntp(st)
        et = ISO_to_ntp(et)
        start_bin = time_to_bin(st)
        end_bin = time_to_bin(et)
    return Response(json.dumps({'start_bin' : start_bin, 'stop_bin' : end_bin}), mimetype='application/json')

@app.route("/get_times", methods=['GET'])
def get_times():
    """Given bins return start time of first and start time of first + 1 bins"""
    sb = request.args.get('start')
    eb = request.args.get('stop')
    if sb is None or eb is None:
        return "Need bin and end to compute bin range"
    sb = int(sb)
    eb = int(eb)
    ret = {
        'startDT' : ntp_to_ISO_date(bin_to_time(sb)),
        'start' : bin_to_time(sb),
        'stopDT' : ntp_to_ISO_date(bin_to_time(eb + 1)),
        'stop' : bin_to_time(eb + 1)
    }
    return Response(json.dumps(ret), mimetype='application/json')


@app.route('/')
def index():
    return "You are trying to access <strong>stream engine</strong> directly. Please access through uframe instead."


def validate(input_data):
    if input_data is None:
        raise MalformedRequestException('Received NULL input data')

    streams = input_data.get('streams')
    if streams is None or not isinstance(streams, list):
        raise MalformedRequestException('Received invalid request', payload={'request': input_data})

    for each in streams:
        if not isinstance(each, dict):
            raise MalformedRequestException('Received invalid request, stream is not dictionary',
                                            payload={'request': input_data})
        keys = each.keys()
        required = {'subsite', 'node', 'sensor', 'method', 'stream'}
        missing = required.difference(keys)
        if len(missing) > 0:
            raise MalformedRequestException('Missing stream information from request',
                                            payload={'request': input_data})

        stream = Stream.query.filter(Stream.name == each['stream']).first()
        if stream is None:
            raise InvalidStreamException('The requested stream does not exist in preload', payload={'stream': each})

        # this check will disallow virtual streams to be accessed, so it's commented out for now
        #if not stream_exists(each['subsite'],
                             #each['node'],
                             #each['sensor'],
                             #each['method'],
                             #each['stream']):
            #raise StreamUnavailableException('The requested stream does not exist in cassandra',
                                             #payload={'stream' :each})

        parameters = each.get('parameters', [])
        stream_parameters = [p.id for p in stream.parameters]
        for pid in parameters:
            p = CachedParameter.from_id(pid)
            if p is None:
                raise InvalidParameterException('The requested parameter does not exist in preload',
                                                payload={'id': pid})

            if pid not in stream_parameters:
                raise InvalidParameterException('The requested parameter does not exist in this stream',
                                                payload={'id': pid, 'stream': each})

    if not isinstance(input_data.get('coefficients', {}), dict):
        raise MalformedRequestException('Received invalid coefficient data, must be a map',
                                        payload={'coefficients': input_data.get('coefficients')})
