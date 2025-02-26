import os
import json
import engine
import logging
from collections import OrderedDict
from util.qartod_service import qartodTestServiceAPI
from ioos_qc.config import QcConfig

import numpy as np

from util.common import QartodFlags, NumpyEncoder, QARTOD_PRIMARY, QARTOD_SECONDARY

log = logging.getLogger(__name__)

EXCEPTION_MESSAGE = 'Logged Exception'
DEFAULT_QARTOD_TEST_MODULE = engine.app.config['DEFAULT_QARTOD_TEST_MODULE']


class QartodQcExecutor(object):
    """
    Class to manage the execution of all necessary QC for a request
    """
    def __init__(self, stream_request):
        self.stream_request = stream_request
        self.request_id = stream_request.request_id

    def execute_qartod_tests(self):
        external_includes = self.stream_request.external_includes
        #only run QARTOD on requested parameters, no need to run on parameters that won't be in returned data
        
        parameters = [parameter.name for parameter in self.stream_request.requested_parameters] 
        for stream_key, stream_dataset in self.stream_request.datasets.iteritems():
            subsite, node, sensor, _, stream = stream_key.as_tuple()
            qartod_tests = qartodTestServiceAPI.find_qartod_tests(subsite, node, sensor, stream, parameters)
            for dataset in stream_dataset.datasets.itervalues():
                for qartod_test_record in qartod_tests:
                    self.execute_qartod_test(qartod_test_record, dataset, external_includes)

    def execute_qartod_test(self, qartod_test_record, dataset, external_includes):
        """
        Run a single QARTOD test against the given dataset and record the results in the dataset.
        :param qartod_test_record: QartodTestRecord indicating a test to execute
        :param dataset: xarray.Dataset holding the science data the QARTOD test evaluates
        :return:
        """
        is_2d_array = False
        # Extract configuration details for test inputs referring to dataset variables
        params = qartod_test_record.parameters
        # single quoted strings in parameters (i.e. from the database field) will mess up the json.loads call
        params = params.replace("'", "\"")
      
        try:
            param_dict = json.loads(params)
        except ValueError:
            log.error('<%s> Failure deserializing QC parameter configuration %r', self.request_id, params)
            return

        parameter_under_test = param_dict['inp']
        # can't run test on data that's not there
        if parameter_under_test not in dataset:
            return

        # need to account for external parameters (i.e. depth) for some qartod tests
        external_params = {}
        for external_stream_key in external_includes:
            for parameter in external_includes[external_stream_key]:
                long_parameter_name = external_stream_key.stream_name + "-" + parameter.name
                external_params[parameter.name] = long_parameter_name
        # replace parameter names with the actual numpy arrays from the dataset for each entry in param_dict
        # cast keys to list instead of iterating dict directly because we may delete keys in this loop 
        try:
            for input_name in list(param_dict.keys()):
                param_name = param_dict[input_name]
                if param_name and param_name != 'null' and param_name != 'None':
                    #use param name from external parameters if qartod parameter not found in dataset
                    if param_name not in list(dataset.variables):
                        if param_name in external_params:
                            param_name = external_params[param_name]
                    
                    param_dict[input_name] = dataset[param_name].values
                else:
                    # optional parameter set to None/null - remove it
                    del param_dict[input_name]
        except KeyError:
            log.error('<%s> Failure reading QARTOD test parameter %r. Skipping test.', self.request_id,
                      param_name)
            return
       
        # Extract configuration details for remaining test inputs
        config = qartod_test_record.qcConfig
        # single quoted strings in qcConfig (i.e. from the database field) will mess up the json.loads call
        config = config.replace("'", "\"")
        secondary_dim_len = None
        if len(param_dict['inp'].shape) > 1:
            secondary_dim_len = param_dict['inp'].shape[1]
            is_2d_array=True
       
        qc_configs = [] 
        try:
            json_config = json.loads(config)
            results = OrderedDict()
            suspect_values = []
            fail_values = []
            if is_2d_array:      
                for modu, tests in json_config.items():
                    results[modu] = OrderedDict()
                    for testname, kwargs in tests.items():
                        if testname == 'gross_range_test':
                            suspect_values = kwargs.get('suspect_span', [])
                            fail_values = kwargs.get('fail_span', [])

                            if suspect_values and fail_values:
                                if not isinstance(suspect_values[0], list) and not isinstance(fail_values[0], list):
                                    fail_values = [fail_values] * secondary_dim_len
                                    suspect_values = [suspect_values] * secondary_dim_len
                                
                                qc_configs.extend(
                                    QcConfig({modu: {testname: {'suspect_span': suspect, 'fail_span': fail}}})
                                    for suspect, fail in zip(suspect_values, fail_values)
                                )

                            elif fail_values:
                                fail_values = [fail_values] * secondary_dim_len
                                qc_configs.extend(
                                    QcConfig({modu: {testname: {'fail_span': fail}}})
                                    for fail in fail_values
                                )

                        elif testname == 'climatology_test':
                            qc_config = QcConfig(json_config)                      
            else:
                qc_config = QcConfig(json_config)
        except ValueError:
            log.error('<%s> Failure deserializing QC test configuration %r for parameter %r', self.request_id,
                    config, parameter_under_test)
            return                

        if 'tinp' in param_dict.keys():
            # Seconds from NTP epoch to UNIX epoch           
            NTP_OFFSET_SECS = 2208988800
            #convert NTP times to UNIX time before sending to Qartod for Climatology test
            if np.all(param_dict['tinp'] > NTP_OFFSET_SECS):
                param_dict['tinp'] = param_dict['tinp'] - NTP_OFFSET_SECS
   
        # call QARTOD test in a separate process to deal with crashes, e.g. segfaults
        read_fd, write_fd = os.pipe()
        processid = os.fork()
        if processid == 0:
            results = []
            # child process
            with os.fdopen(write_fd, 'w') as w:
                os.close(read_fd)
                # run the qc function
                try:
                    # all arguments except the data under test come from the configuration object
                    # results is a nested dictionary
                    if is_2d_array:
                        sub_param_dict = {}
                        #gross range 2D contains multiple qc_configs, need to run them each individually
                        if qc_configs:
                            for counter, qc_config in enumerate(qc_configs):
                                sub_param_dict['inp'] = np.array([value[counter] for value in param_dict['inp']])
                                sub_results = qc_config.run(**sub_param_dict)
                                results.append(sub_results)
                        #climatology uses zinp parameter         
                        elif 'zinp' in param_dict:
                            for counter, zinp in enumerate(param_dict['zinp']):
                                sub_param_dict['inp'] = np.array([value[counter] for value in param_dict['inp']])
                                sub_param_dict['zinp'] = zinp
                                sub_param_dict['tinp'] = param_dict['tinp']
                                sub_results = qc_config.run(**sub_param_dict)
                                results.append(sub_results)
                    else:
                        results.append(qc_config.run(**param_dict))
                    # convert results into a string for sending over pipe
                    # NOTE: this converts numpy arrays to lists! Use np.asarray() to restore them.
                    results_string = json.dumps(results, cls=NumpyEncoder)
                    w.write(results_string)
                except (TypeError, ValueError) as e:
                    log.exception('<%s> Failure executing QC with configuration %r %r', self.request_id, config, e)
                    w.write(EXCEPTION_MESSAGE)
            # child process is done, don't let it stick around
            os._exit(0)

        # parent process
        os.close(write_fd)
        with os.fdopen(read_fd) as r:
            results_string = r.read()
        # wait for the child process to prevent zombies - second argument of 0 means default behavior of waitpid
        os.waitpid(processid, 0)
        # check for failure to produce results
        if not results_string:
            # an error, e.g. segfault, prevented proper qc execution, proceed with trying the next qc function
            log.error('<%s> Failed to execute QC with configuration %r: QC process failed to return any data',
                      self.request_id, config)
            return

        if results_string == EXCEPTION_MESSAGE:
            # an exception has already been logged, proceed with trying the next qc function
            return

        # load the results dict from the results string
        results = json.loads(results_string)

        # results is a nested dictionary with the outer keys being module names, the inner keys being test
        # names, and the inner values being the results for the given test
        # e.g. {'qartod': {'gross_range_test': [0, 0, 3, 4, 0], 'location_test': [2, 2, 2, 2, 2]}}
        all_test_results = []
        test = None
        if results:
            for result_item in results:
                for module, test_set in result_item.items():
                    for test, test_results in test_set.items():
                        # test_results was converted from an np.array to a list during serialization, so convert it back
                        test_results = np.asarray(test_results)
                        
                        # Verify all QC results are valid QARTOD Primary Level Flags
                        mask = np.array([item not in QartodFlags.getValidQCFlags() for item in test_results])

                        if mask.any():
                            log.error('Received QC result with invalid QARTOD Primary Flag from %s. Invalid flags: %r',
                                    test, np.unique(test_results[mask]))
                            # Use the "high interest" (SUSPECT) flag to draw attention to the failure
                            test_results[mask] = QartodFlags.SUSPECT
                        all_test_results.append(test_results)
    
                        # add results to dataset
                        #QartodQcExecutor.insert_qc_results(parameter_under_test, test, all_test_results, dataset)
                    
            all_test_results = np.array(all_test_results)
            all_test_results = np.squeeze(all_test_results)
            test_results = all_test_results.T   
            test_results = np.asarray(test_results)
            if test:
                try:
                    QartodQcExecutor.insert_qc_results(parameter_under_test, test, test_results, dataset)
                except ValueError as ve:
                    log.error('<%s> %s', self.request_id, ve.message)


    @staticmethod
    def insert_qc_results(parameter, test, results, dataset):
        """
        Convert QARTOD test results into xarray.DataArrays and attributes and add them to the xarray.Dataset oject
        they describe.
        :param parameter: name of stream parameter the QARTOD test was run against to get results
        :param test: name of function which implements and executed the specific QARTOD test
        :param results: numpy.ndarray of QARTOD flags indicating test results for each observation of parameter
        :param dataset: xarray.Dataset into which the QARTOD results should be inserted (the same one which had the
        QARTOD test run against it to get results)
        :return:
        """
        # make sure the array data type is correct - failure to do this can cause issues writing the NetCDF file later
        # specifically, without this the type is assumed int64 and recasting in netcdf_utils clears the attrs
        results = results.astype(np.uint8)

        qartod_primary_flag_name = parameter + QARTOD_PRIMARY
        qartod_secondary_flag_name = parameter + QARTOD_SECONDARY
        # In rare cases, a parameter may have a dimension other than 'obs'. Normally, dataset[parameter].dims would be
        # a tuple potentially containing multiple dimensions, but we previously checked the data is 1-dimensional,
        # so the tuple must contain only 1 dimension
        qc_obs_dimension = dataset[parameter].dims

        # try to get the standard_name if its set, but default to the parameter name otherwise
        parameter_standard_name = dataset[parameter].attrs.get('standard_name', parameter)
        # try to get the long_name if its set, but default to the parameter name otherwise
        parameter_long_name = dataset[parameter].attrs.get('long_name', parameter)

        # UPDATE PRIMARY FLAGS
        if qartod_primary_flag_name not in dataset:
            # add this variable to 'ancillary_variables' attribute for the parameter it describes
            if dataset[parameter].attrs.get('ancillary_variables', None):
                dataset[parameter].attrs['ancillary_variables'] += ' ' + qartod_primary_flag_name
            else:
                dataset[parameter].attrs['ancillary_variables'] = qartod_primary_flag_name

            dataset[qartod_primary_flag_name] = (qc_obs_dimension, results, {})
            # add attribute info for QC flag interpretation
            dataset[qartod_primary_flag_name].attrs['flag_values'] = np.array(
                QartodFlags.getValidQCFlags()).astype(np.uint8)
            dataset[qartod_primary_flag_name].attrs['flag_meanings'] = ' '.join(QartodFlags.getQCFlagMeanings())
            dataset[qartod_primary_flag_name].attrs['standard_name'] = parameter_standard_name + ' status_flag'
            dataset[qartod_primary_flag_name].attrs['long_name'] = parameter_long_name + ' QARTOD Summary Flag'
            dataset[qartod_primary_flag_name].attrs['references'] = ('https://ioos.noaa.gov/project/qartod '
                                                                     'https://github.com/ioos/ioos_qc')
            dataset[qartod_primary_flag_name].attrs['comment'] = ('Summary QARTOD test flags. For each datum, the flag '
                                                                  'is set to the most significant result of all QARTOD '
                                                                  'tests run for that datum.')
        else:
            # combine qc results by keeping the most adverse flag for each observation
            current_qartod_flag_primary = dataset[qartod_primary_flag_name].values
            temp_qartod_flag_primary = np.maximum(current_qartod_flag_primary, results)
            dataset[qartod_primary_flag_name].values = temp_qartod_flag_primary

        # Work with results as an array of strings for the secondary flag
        results_string = results.astype('U1')

        # UPDATE SECONDARY FLAGS
        # represent QARTOD test results as a string of space separated test result integers (flags) in the order the
        # tests were run, as indicated by the 'tests_executed' attribute
        if qartod_secondary_flag_name not in dataset:
            # add this variable to 'ancillary_variables' attribute for the parameter it describes
            if dataset[parameter].attrs.get('ancillary_variables', None):
                dataset[parameter].attrs['ancillary_variables'] += ' ' + qartod_secondary_flag_name
            else:
                dataset[parameter].attrs['ancillary_variable'] = qartod_secondary_flag_name

            dataset[qartod_secondary_flag_name] = (qc_obs_dimension, results_string, {})
            # add attribute info for QC flag interpretation
            dataset[qartod_secondary_flag_name].attrs['tests_executed'] = test
            dataset[qartod_secondary_flag_name].attrs['standard_name'] = parameter_standard_name + ' status_flag'
            dataset[qartod_secondary_flag_name].attrs['long_name'] = parameter_long_name + ' Individual QARTOD Flags'
            dataset[qartod_secondary_flag_name].attrs['references'] = ('https://ioos.noaa.gov/project/qartod '
                                                                       'https://github.com/ioos/ioos_qc')
            flag_mapping = dict(zip(QartodFlags.getValidQCFlags(), QartodFlags.getQCFlagMeanings()))
            flag_mapping_string = ', '.join('{}: {}'.format(k, v) for k, v in flag_mapping.items())
            dataset[qartod_secondary_flag_name].attrs['comment'] = ('Individual QARTOD test flags. For each datum, '
                                                                    'flags are listed in a string matching the order '
                                                                    'of the tests_executed attribute. Flags should be '
                                                                    'interpreted using the standard QARTOD mapping: ['
                                                                    + flag_mapping_string + '].')
        else:
            # combine qc results by appending results to the relevant string for each observation (i.e. all tests for
            # a given observation occur in the same string)
            current_qartod_flag_secondary = dataset[qartod_secondary_flag_name].values
            temp_qartod_flag_secondary = np.core.defchararray.add(current_qartod_flag_secondary, results_string)
            dataset[qartod_secondary_flag_name].values = temp_qartod_flag_secondary

            # update the attributes to detail which tests were run (and in what order)
            dataset[qartod_secondary_flag_name].attrs['tests_executed'] += ', ' + test

