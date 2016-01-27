
import json
import numpy
import os
from datetime import datetime

from engine.routes import app

class QueryInfo(object):

    def setID(self, name, query):
        self.user_name = name
        self.log_time = datetime.now().isoformat()

#------------------------------------------------
class CalculatedParameter(object):

    def __init__(self, parameter, name, functionID):
        self.calculated_parameter = parameter
        self.parameter_name = name
        self.functionID = functionID
        # list of QueryParameter
        self.arg_summary = []
        self.function_arguments = []

    def addArgument(self, query_parameter):
        self.arg_summary.append(query_parameter.parameter)
        self.function_arguments.append(query_parameter)

#------------------------------------------------

class QueryData(object):

    def __init__(self):
        self.calculated_parameters = {}
        self.user_info = QueryInfo()

    def setID(self, name, query):
        self.user_info.setID(name, query)

    def addFunctionArgument(self, calc_paramID, arg):
        calcParam = self.calculated_parameters[calc_paramID]  
        calcParam.addArgument(arg)


    def addCalculatedParameter(self, paramID, param_name, function):
        dparam = CalculatedParameter(paramID, param_name, function)
        self.calculated_parameters[paramID] = dparam



#------------------------------------------------
class QueryParameter(object):

    def __init__(self, paramID):
        self.parameter = paramID 
        self.value = []

    def setParamName(self, name):
        self.parameter = name

    def setValue(self, value):
        self.value = value

#------------------------------------------------

class ParameterReport:

    def __init__(self):
        self.m_qdata = QueryData()
        log_dir = app.config['PARAMETER_LOGGING']
        if log_dir is None:
            log_dir = '.'
        self.m_path = log_dir

    def __del__(self):
        self.m_jsonfile.close()

    def setUserInfo(self, name, query):
        self.m_qdata.setID(name, query)
        directory = self.m_path + "/" + name
        if not os.path.exists(directory):
            os.makedirs(directory)
        file = "/" + query + ".log" 
        self.m_path = directory + file

    def addParameterWithName(self, paramID, paramName, value):
        qparam = QueryParameter(paramID)
        qparam.setParamName(paramName)
        qparam.setValue(value)
        self.m_qdata.addParameter(qparam)

    def addParameter(self, paramID, value):
        qparam = QueryParameter(paramID)
        qparam.setValue(value)
        self.m_qdata.addParameter(qparam)

    def addParameterArgument(self, calcParamID, paramID, value):
        arg = QueryParameter(paramID)
        arg.setValue(value)
        self.m_qdata.addFunctionArgument(calcParamID, arg)

    def setCalculatedParameter(self, paramID, param_name, function):
        self.m_qdata.addCalculatedParameter(paramID, param_name, function)

    def write (self):   
        self.m_jsonfile = open(self.m_path, "a")
        json.dump(self.m_qdata, self.m_jsonfile, sort_keys=True, 
                   default=jdefault, indent=2, separators=(',', ': '))
        self.m_jsonfile.close()

def jdefault(o):
    if isinstance(o, set):
        return list(o)
    return o.__dict__


