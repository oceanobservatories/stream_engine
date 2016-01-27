
import json
import os
from datetime import datetime
import logging

from engine.routes import app

log = logging.getLogger(__name__)

class QueryInfo(object):

    def __init__(self):
        self.user_name = None
        self.log_time = None

    def set_id(self, name):
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

    def __init__(self):
        self.calculated_parameters = {}
        self.user_info = QueryInfo()

    def set_id(self, name):
        self.user_info.set_id(name)

    def add_algorithm_argument(self, calc_param_id, arg):
        calc_param = self.calculated_parameters[calc_param_id]  
        calc_param.add_argument(arg)


    def add_calculated_parameter(self, param_id, param_name, algorithm):
        dparam = CalculatedParameter(param_id, param_name, algorithm)
        self.calculated_parameters[param_id] = dparam


class QueryParameter(object):

    def __init__(self, param):
        self.argument = param 
        self.value = []


DEFAULT_LOG_DIR = app.config.get('PARAMETER_LOGGING', '.')

class ParameterReport:

    def __init__(self, log_dir=DEFAULT_LOG_DIR):
        self.m_qdata = QueryData()
        self.m_path = log_dir
        self.m_jsonfile = None

    def __del__(self):
        self.m_jsonfile.close()

    def set_user_info(self, name, query):
        self.m_qdata.set_id(name)
        directory = self.m_path + "/" + name
        outfile = "/" + query + ".log"
        self.m_path = directory + outfile
        if not os.path.exists(directory):
            try:
                os.makedirs(directory)
            except Exception, e:
                errmsg = "Failed to create logging directory, " + directory + " :"
                errmsg += e
                log.error(errmsg)

    def add_parameter_argument(self, calc_param_id, param, value):
        arg = QueryParameter(param)
        arg.value = value
        self.m_qdata.add_algorithm_argument(calc_param_id, arg)

    def set_calculated_parameter(self, param_id, param_name, algorithm):
        self.m_qdata.add_calculated_parameter(param_id, param_name, algorithm)

    def write (self):
        log.info("writing file")
        try:
            self.m_jsonfile = open(self.m_path, "a")
            json.dump(self.m_qdata, self.m_jsonfile, sort_keys=True,
                      default=jdefault, indent=2, separators=(',', ': '))
            self.m_jsonfile.close()
        except Exception, e:
            errmsg = "Failed to open Advanced logfile, " + self.m_path + " :"
            errmsg += e
            log.error(errmsg)


def jdefault(o):
    if isinstance(o, set):
        return list(o)
    return o.__dict__


