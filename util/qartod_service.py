import re
import json
import engine
import logging
import requests
from copy import deepcopy

log = logging.getLogger(__name__)


class QartodTestServiceAPI(object):
    def __init__(self, qartod_url):
        self.base_url = qartod_url

    def find_qartod_tests(self, subsite, node, sensor, stream, parameters):
        """
        Lookup QARTOD test records via the EDEX webservice.
        :param subsite: name of subsite to match
        :param node: name of node to match
        :param sensor: name of sensor to match
        :param stream: name of stream to match
        :param parameters: list of parameter names, one of which must match
        :return: list of QartodTestRecord objects
        """
        params = {
            'subsite': subsite,
            'node': node,
            'sensor': sensor,
            'stream': stream,
            'parameters': parameters
        }

        url = '/'.join((self.base_url, 'find'))
        response = requests.get(url, params=params)
        try:
            payload = response.json()
        except ValueError:
            log.error('Error fetching QARTOD tests: %s', ValueError.message)
            return []

        if response.status_code != requests.codes.ok:
            log.error('Error fetching QARTOD tests: <%r> %r', response.status_code, payload)
            return []

        result = []
        for record in payload:
            if record.pop('@class', None) == '.QartodTestRecord':
                qartod_test_record = QartodTestRecord(**record)
                expanded_test_records = self.expand_qartod_record(qartod_test_record, subsite, node, sensor,
                                                                  stream, parameters)
                result.extend(expanded_test_records)
        return result

    @staticmethod
    def expand_qartod_record(record, subsite, node, sensor, stream, parameters):
        """
        Check for null fields indicating wildcards in a QartodTestRecord and replace these nulls with applicable values
        for the current data under test and return the result. In the case of parameters, check for a null "inp"
        parameter in the parameters JSON and then return one QartodTestRecord per applicable parameter, setting the
        "inp" in the parameters JSON accordingly.
        :param record: QartodTestRecord with potentially null fields to process
        :param subsite: name of subsite of StreamDataset against which QARTOD tests will run
        :param node: name of node of StreamDataset against which QARTOD tests will run
        :param sensor: name of sensor of StreamDataset against which QARTOD tests will run
        :param stream: name of stream of StreamDataset against which QARTOD tests will run
        :param parameters: list of parameter names applicable for StreamDataset against which QARTOD tests will run
        :return: list of processed QartodTestRecords with no null fields
        """
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
        new_params = None
        if record.parameters is None:
            # parameters was null - add parameters to new records
            new_params = {parameter: '{"inp": "' + parameter + '"}' for parameter in parameters}
        elif re.compile('[\'"]inp[\'"]: [\'"]?(\w*)[\'"]?').search(record.parameters).group(1) == 'null':
            # parameters existed, but the "inp" attribute was null - replace "inp" in each new record
            new_params = {parameter: '{"inp": "' + parameter + '"}' for parameter in parameters}

        if new_params:
            # expand the null parameters into every applicable parameter
            for parameter in new_params:
                new_record = deepcopy(record)
                new_record.parameters = new_params[parameter]
                tests.append(new_record)
        else:
            # no record expansion necessary - return the record
            tests.append(record)
        return tests


class QartodTestRecord(object):
    def __init__(self, id, refDes, stream, parameters, qcConfig, source=""):
        self.id = id
        self.refDes = refDes
        self.stream = stream
        self.parameters = parameters
        self.qcConfig = qcConfig
        self.source = source

    def __key(self):
        return (self.id, self.refDes['subsite'], self.refDes['node'], self.refDes['sensor'], self.stream,
                self.parameters, self.qcConfig, self.source)

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, QartodTestRecord):
            return self.__key() == other.__key()
        return NotImplemented

    def __str__(self):
        # serialize the class as JSON
        json_str = json.dumps(self, default=lambda x: x.__dict__, separators=(',', ':'), indent=2)
        # insert the missing "@class" attribute
        return json_str[:4] + '"@class": ".QartodTestRecord",\n  ' + json_str[4:]

    def __repr__(self):
        return self.__str__()

qartodTestServiceAPI = QartodTestServiceAPI(engine.app.config['QARTOD_TEST_SERVICE_URL'])
