import json
from flask import Flask, request, Response
from flask.ext.sqlalchemy import SQLAlchemy
import ntplib
import time
from werkzeug.exceptions import abort


app = Flask(__name__)
app.config.from_object('config')

# preload database handle
db = SQLAlchemy(app)

@app.route('/')
def hello():
    return 'hello world!'


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
    import util.calc
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

    start = input_data.get('start', 1)
    stop = input_data.get('stop', ntplib.system_to_ntp_time(time.time()))

    return Response(util.calc.calculate(streams[0], start, stop, input_data.get('coefficients', [])),
                    mimetype='application/json')


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
    import util.streams
    output_data = {'streams': []}
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

        needed = util.streams.NeededStream(each)
        output_data['streams'].append(needed.as_dict())

    return Response(json.dumps(output_data), mimetype='application/json')
