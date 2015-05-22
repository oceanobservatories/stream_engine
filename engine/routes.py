import engine

import json
import time
import logging

import ntplib
from flask import request, Response, jsonify
from functools import wraps

from engine import app
from preload_database.model.preload import Stream
import util.calc
from util.cass import stream_exists
from util.common import CachedParameter, StreamEngineException, MalformedRequestException, \
    InvalidStreamException, StreamUnavailableException, InvalidParameterException

log = logging.getLogger(__name__)

@app.errorhandler(StreamEngineException)
def handle_stream_not_found(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code

    log.info("Returning exception: {}".format(error.to_dict()))
    return response

#def log_request(f):
    #@wraps(f)
    #def inner(*args, **kwargs):
        #start_time = time.time()
        #log.info("Handling request for {} - {}".format(request.url, request.get('streams', "")))
        #result = f(*args, **kwargs)
        #log.info("Request took {:.2f}s to complete".format(time.time()-start_time))
        #return result
    #return inner


@app.route('/particles', methods=['POST'])
#@log_request
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
    log.info("Handling request for {} - {}".format(request.url, input_data.get('streams', "")))

    start = input_data.get('start', 1)
    stop = input_data.get('stop', ntplib.system_to_ntp_time(time.time()))
    limit = input_data.get('limit', 0)
    if limit <= 0:
        limit = None
    
    resp = Response(util.calc.get_particles(input_data.get('streams'), start, stop, input_data.get('coefficients', {}),
                    input_data.get('qcParameters', {}), limit=limit, custom_times=input_data.get('custom_times'),
                    custom_type=input_data.get('custom_type')), mimetype='application/json')

    log.info("Request took {:.2f}s to complete".format(time.time()-request_start_time))
    return resp


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
    start = input_data.get('start', 1)
    stop = input_data.get('stop', ntplib.system_to_ntp_time(time.time()))
    limit = input_data.get('limit', 0)
    if limit <= 0:
        limit = None
    return Response(util.calc.get_netcdf(input_data.get('streams'), start, stop, input_data.get('coefficients', {}), limit=limit, custom_times=input_data.get('custom_times'), custom_type=input_data.get('custom_type')),
                    mimetype='application/netcdf')


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
    output_data = {'streams': util.calc.get_needs(input_data.get('streams'))}
    return Response(json.dumps(output_data), mimetype='application/json')


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
