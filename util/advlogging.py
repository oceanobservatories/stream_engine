
import json
import os
from datetime import datetime
import logging

from engine import app

DEFAULT_LOG_DIR = app.config.get('PARAMETER_LOGGING', '.')

log = logging.getLogger(__name__)


class QueryInfo(object):

    def __init__(self, name):
        self.user_name = name
        self.log_time = datetime.now().isoformat()


class CalculatedParameter(object):

    def __init__(self, parameter, name, algorithm):
        self.calculated_parameter_id = parameter
        self.calculated_parameter_name = name
        self.algorithm = algorithm
        # list of QueryParameter
        self.arg_summary = []
        self.algorithm_arguments = []

    def add_argument(self, query_parameter):
        self.arg_summary.append(query_parameter.argument)
        self.algorithm_arguments.append(query_parameter)


class QueryData(object):

    def __init__(self, name):
        self.calculated_parameters = {}
        self.user_info = QueryInfo(name)

    def add_algorithm_argument(self, calc_param_id, arg):
        calc_param = self.calculated_parameters[calc_param_id]
        calc_param.add_argument(arg)

    def add_calculated_parameter(self, param_id, param_name, algorithm):
        dparam = CalculatedParameter(param_id, param_name, algorithm)
        self.calculated_parameters[param_id] = dparam


class QueryParameter(object):

    def __init__(self, param, value):
        self.argument = param
        self.value = value


class ParameterReport(object):

    def __init__(self, name, request_id, query_name, log_dir=DEFAULT_LOG_DIR):
        self.m_qdata = QueryData(name)
        self.m_path = os.path.join(log_dir, name, request_id, query_name + '.log')

    def add_parameter_argument(self, calc_param_id, param, value):
        arg = QueryParameter(param, value)
        self.m_qdata.add_algorithm_argument(calc_param_id, arg)

    def set_calculated_parameter(self, param_id, param_name, algorithm):
        self.m_qdata.add_calculated_parameter(param_id, param_name, algorithm)

    def write(self):
        try:
            if not os.path.exists(os.path.dirname(self.m_path)):
                os.makedirs(os.path.dirname(self.m_path))
            with open(self.m_path, 'w') as fh:
                json.dump(self.m_qdata, fh, default=jdefault, indent=2, separators=(',', ': '))
        except EnvironmentError as e:
            log.error('Failed to write advanced logfile: %s', e)


def jdefault(o):
    if isinstance(o, set):
        return list(o)
    return o.__dict__
