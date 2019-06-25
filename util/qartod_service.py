import engine
import logging
import requests

log = logging.getLogger(__name__)


class QartodTestServiceAPI(object):
    def __init__(self, qartod_url):
        self.base_url = qartod_url

    def find_qartod_tests(self, subsite, node, sensor, stream, parameter):
        params = {
            'subsite': subsite,
            'node': node,
            'sensor': sensor,
            'stream': stream,
            'parameter': parameter
        }

        url = '/'.join((self.base_url, 'find'))
        response = requests.get(url, params=params)
        payload = response.json()

        if response.status_code == requests.codes.ok:
            result = []
            for record in payload:
                if record.pop('@class', None) == '.QartodTestRecord':
                    qartod_test_record = QartodTestRecord(**record)
                    result.append(qartod_test_record)
            return result
        else:
            log.error('Error fetching QARTOD tests: <%r> %r', response.status_code, payload)
            return None

    def find_qartod_tests_bulk(self, subsite, node, sensor, stream, parameters):
        params = {
            'subsite': subsite,
            'node': node,
            'sensor': sensor,
            'stream': stream,
            'parameters': parameters
        }

        url = '/'.join((self.base_url, 'bulk'))
        response = requests.get(url, params=params)
        payload = response.json()

        if response.status_code == requests.codes.ok:
            result = []
            for record in payload:
                if record.pop('@class', None) == '.QartodTestRecord':
                    qartod_test_record = QartodTestRecord(**record)
                    result.append(qartod_test_record)
            return result
        else:
            log.error('Error fetching QARTOD tests: <%r> %r', response.status_code, payload)
            return None


class QartodTestRecord(object):
    def __init__(self, id, refDes, stream, parameter, test, inputs):
        self.id = id
        self.refDes = refDes
        self.stream = stream
        self.parameter = parameter
        self.test = test
        self.inputs = inputs

    def __eq__(self, item):
        return isinstance(item, QartodTestRecord) and item.id == self.id


qartodTestServiceAPI = QartodTestServiceAPI(engine.app.config['QARTOD_TEST_SERVICE_URL'])
