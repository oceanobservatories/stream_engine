import json
import engine
import logging
import requests
from copy import deepcopy

log = logging.getLogger(__name__)


class QartodTestServiceAPI(object):
    def __init__(self, qartod_url):
        self.base_url = qartod_url

    def find_qartod_tests(self, subsite, node, sensor, stream, parameter):
        result = []

        params = {
            'subsite': subsite,
            'node': node,
            'sensor': sensor,
            'stream': stream,
            'parameter': parameter
        }

        url = '/'.join((self.base_url, 'find'))
        response = requests.get(url, params=params)
        try:
            payload = response.json()
        except ValueError:
            log.error('Error fetching QARTOD tests: %s', ValueError.message)
            return result

        if response.status_code == requests.codes.ok:
            for record in payload:
                if record.pop('@class', None) == '.QartodTestRecord':
                    qartod_test_record = QartodTestRecord(**record)
                    expanded_test_records = self.expand_qartod_record(qartod_test_record, subsite, node, sensor,
                                                                      stream, parameter)
                    result.extend(expanded_test_records)
        else:
            log.error('Error fetching QARTOD tests: <%r> %r', response.status_code, payload)
        return result

    def find_qartod_tests_bulk(self, subsite, node, sensor, stream, parameters):
        result = []

        params = {
            'subsite': subsite,
            'node': node,
            'sensor': sensor,
            'stream': stream,
            'parameters': parameters
        }

        url = '/'.join((self.base_url, 'bulk'))
        response = requests.get(url, params=params)
        try:
            payload = response.json()
        except ValueError:
            log.error('Error fetching QARTOD tests: %s', ValueError.message)
            return result

        if response.status_code == requests.codes.ok:
            for record in payload:
                if record.pop('@class', None) == '.QartodTestRecord':
                    qartod_test_record = QartodTestRecord(**record)
                    expanded_test_records = self.expand_qartod_record(qartod_test_record, subsite, node, sensor,
                                                                      stream, parameters)
                    result.extend(expanded_test_records)
        else:
            log.error('Error fetching QARTOD tests: <%r> %r', response.status_code, payload)
        return result

    def expand_qartod_record(self, record, subsite, node, sensor, stream, parameters):
        # Nulls are used as wildcards for subsite, node, sensor, stream, and parameter to match all possible values.
        # Expand these records into the multiple test records they represent for clarity and later reference.
        tests = []
        # record the reference designator we are currently processing
        if record.refDes is None:
            record.refDes = {"@class": "ReferenceDesignator", "subsite": subsite, "node": node, "sensor": sensor}
        # record the stream we are currently processing
        if record.stream is None:
            record.stream = stream
        # expand the null wildcard into all of the test records it represents
        if record.parameter is None:
            for parameter in parameters:
                new_record = deepcopy(record)
                new_record.parameter = parameter
                tests.append(new_record)
        else:
            tests.append(record)
        return tests


class QartodTestRecord(object):
    def __init__(self, id, refDes, stream, parameter, test, inputs):
        self.id = id
        self.refDes = refDes
        self.stream = stream
        self.parameter = parameter
        self.test = test
        self.inputs = inputs

    def __key(self):
        return (self.id, self.refDes['subsite'], self.refDes['node'], self.refDes['sensor'], self.stream,
                self.parameter, self.test, self.inputs)

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, QartodTestRecord):
            return self.__key() == other.__key()
        return NotImplemented

    def __str__(self):
        json_str = json.dumps(self, default=lambda x: x.__dict__, separators=(',', ':'), indent=2)
        return json_str[:4] + '"@class": ".QartodTestRecord",\n  ' + json_str[4:]

    def __repr__(self):
        return self.__str__()

qartodTestServiceAPI = QartodTestServiceAPI(engine.app.config['QARTOD_TEST_SERVICE_URL'])
