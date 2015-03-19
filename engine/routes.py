import ntplib
import time
from engine import app
from flask import request, Response
from werkzeug.exceptions import abort
import util.calc
import util.streams


@app.before_first_request
def setup():
    """
    Any code in this method will be executed AFTER forking but before the first request
    :return:
    """
    print 'HELLO WORLD'


@app.route('/calculate', methods=['POST'])
def calculate():
    """
    First DRAFT, only supports 1 stream

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
            'CC_a0': 1.0,
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

    return Response(util.calc.calculate(input_data.get('streams')[0], start, stop, input_data.get('coefficients', [])),
                    mimetype='application/json')


@app.route('/particles', methods=['POST'])
def particles():
    input_data = request.get_json()
    validate(input_data)

    start = input_data.get('start', 1)
    stop = input_data.get('stop', ntplib.system_to_ntp_time(time.time()))
    return Response(util.calc.get_particles(input_data.get('streams'), start, stop, input_data.get('coefficients', [])),
                    mimetype='application/json')


def validate(input_data):
    input_data = request.get_json()
    if input_data is None:
        app.logger.warn('Received null request')
        abort(400)

    streams = input_data.get('streams')
    if streams is None or not isinstance(streams, list):
        app.logger.warn('Received invalid request: %r', streams)
        abort(400)

    for each in streams:
        if not isinstance(each, dict):
            abort(400)