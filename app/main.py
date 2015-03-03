import json
from flask import request
from werkzeug.exceptions import abort
from model.preload import Stream, Parameter
from app import db
from app import app
from util import cassandra_query


@app.route('/')
def index():
    return 'Hello world!'


@app.route('/parameter/<refdes>/<pdid>')
def parameter(refdes, pdid):
    p = Parameter.query.get(pdid)
    if p is None:
        abort(404)

    if p.parameter_type.value == 'function':
        return json.dumps(p.parameter_function_map)

    return cassandra_query.calculate(refdes, p)

@app.route('/stream/<subsite>/<node>/<sensor>/<stream>')
def get_stream(subsite, node, sensor, stream):
    s = Stream.query.filter(Stream.name == stream).first()
    if s is None:
        abort(404)
    return cassandra_query.get_stream(subsite, node, sensor, s)