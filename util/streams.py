from werkzeug.exceptions import abort
from engine import app
from model.preload import Stream, Parameter
from util import cassandra_query

session = cassandra_query.session

STREAM_EXISTS_PS = session.prepare(
    '''
    select *
    from stream_metadata
    where subsite=? and node=? and sensor=? and method=? and stream=?
    '''
)


class NeededStream(object):
    def __init__(self, input_dict, satisfied_parameters=None):
        self.subsite = input_dict.get('subsite')
        self.node = input_dict.get('node')
        self.sensor = input_dict.get('sensor')
        self.method = input_dict.get('method')
        self.params = input_dict.get('parameters', [])
        self.parameters = []
        self.stream = input_dict.get('stream')
        self.stream_db = Stream.query.filter(Stream.name == self.stream).first()
        self.coefficients = []
        self.needed_streams = []
        self._validate()
        self._compute()

    def _validate(self):
        if any([self.subsite is None,
                self.node is None,
                self.method is None,
                self.stream_db is None,
                not isinstance(self.params, list)]):
            app.logger.warn('Bad request for NeededStream: %r', self.__dict__)
            abort(400)
        if not self._available():
            abort(404)

    def _available(self):
        rows = session.execute(STREAM_EXISTS_PS, (self.subsite, self.node, self.sensor, self.method, self.stream))
        return len(rows) == 1

    def _compute(self):
        if len(self.params) == 0:
            self.params = self.stream_db.parameters
        else:
            self.params = [Parameter.query.get(p) for p in self.params]
            self.params = [p for p in self.params if p in self.stream.parameters]

        derived_parameters = []
        coeffs = []

        # pass one, identify derived parameters
        for p in self.params:
            if p.parameter_type.value == 'function':
                derived_parameters.append(p)

        # to avoid getting this data multiple times, we will obtain and reuse
        # the list of distinct sensors from cassandra
        distinct_sensors = cassandra_query.get_distinct_sensors()

        # pass two, determine needed external values for derived parameters
        needs_map = {}
        for p in derived_parameters:
            coeffs.extend(p.needs_cc())
            needs_map[p] = p.needs()

        # eliminate duplicates
        needed_params = []
        for p in needs_map:
            for each in needs_map[p]:
                if each not in needed_params:
                    needed_params.append(each)

        # eliminate items present in this stream
        needed_params = [p for p in needed_params if p not in self.stream_db.parameters]
        app.logger.debug('need params: %s', ', '.join(['%d %s' % (p.id, p.name) for p in needed_params]))

        missing = []
        for need in needed_params:

            # see if we have any streams available from this sensor or node
            # which provide this parameter
            app.logger.debug('NEED PARAMETER: %d %s STREAMS: %s', need.id, need.name, [s.name for s in need.streams])
            sensor, stream = cassandra_query.find_stream(self.subsite, self.node, self.sensor,
                                                         need.streams, distinct_sensors)
            if stream is not None:
                self.needed_streams.append((self.subsite, self.node, sensor, stream.name))
            else:
                # missing data for this parameter
                app.logger.warn('Unable to find needed parameter: %d %s from streams: %s',
                                need.id, need.name, [s.name for s in need.streams])
                missing.append(need)

        # determine which parameters are unable to be fulfilled if we are missing any parameters
        for each in missing:
            for p in needs_map.keys():
                if each in needs_map[p]:
                    app.logger.warn('Unable to fulfill derived product: %d %s', p.id, p.name)
                    del(needs_map[p])
                    self.params.remove(p)

        self.coefficients = list(set(coeffs))
        self.parameters = [p.id for p in self.params]

    def as_dict(self):
        fields = ['subsite', 'node', 'sensor', 'method', 'stream', 'coefficients', 'parameters']
        return {f: self.__dict__[f] for f in fields}
