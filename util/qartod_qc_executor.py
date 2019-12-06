import os
import io
import json
import engine
import logging
from util.qartod_service import qartodTestServiceAPI
from ioos_qc.config import QcConfig

import numpy as np

from util.common import QartodFlags, NumpyEncoder

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
        for stream_key, stream_dataset in self.stream_request.datasets.iteritems():
            subsite, node, sensor, _, stream = stream_key.as_tuple()
            parameters = [parameter.name for parameter in stream_key.stream.parameters]
            qartod_tests = qartodTestServiceAPI.find_qartod_tests(subsite, node, sensor, stream, parameters)

            for dataset in stream_dataset.datasets.itervalues():
                for qartod_test_record in qartod_tests:
                    self.execute_qartod_test(qartod_test_record, dataset)

    def execute_qartod_test(self, qartod_test_record, dataset):
        """
        Run a single QARTOD test against the given dataset and record the results in the dataset.
        :param qartod_test_record: QartodTestRecord indicating a test to execute
        :param dataset: xarray.Dataset holding the science data the QARTOD test evaluates
        :return:
        """
        parameter = qartod_test_record.parameter

        if parameter not in dataset:
            return

        config = qartod_test_record.qcConfig
        # single quoted strings in qcConfig (i.e. from the database field) will mess up the json.loads call
        config = config.replace("'", "\"")
        try:
            qc_config = QcConfig(json.loads(config))
        except ValueError:
            log.error('<%s> Failure deserializing QC test configuration %r for parameter %r', self.request_id,
                      config, parameter)
            return

        array_under_test = dataset[parameter].values

        # call QARTOD test in a separate process to deal with crashes, e.g. segfaults
        read_fd, write_fd = os.pipe()
        processid = os.fork()
        if processid == 0:
            # child process
            with os.fdopen(write_fd, 'w') as w:
                os.close(read_fd)
                # run the qc function
                try:
                    # all arguments except the data under test come from the configuration object
                    # results is a nested dictionary
                    results = qc_config.run(inp=array_under_test)
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
        for module, test_set in results.items():
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

                # add results to dataset
                QartodQcExecutor.insert_qc_results(parameter, test, test_results, dataset)

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

        qartod_primary_flag_name = '_'.join([parameter, 'qartod_flag_primary'])
        qartod_secondary_flag_name = '_'.join([parameter, 'qartod_flag_secondary'])
        # In rare cases, a parameter may have a dimension other than 'obs'. Normally, dataset[parameter].dims would be
        # a tuple potentially containing multiple dimensions, but we previously checked the data is 1-dimensional,
        # so the tuple must contain only 1 dimension
        qc_obs_dimension = dataset[parameter].dims[0]

        # UPDATE PRIMARY FLAGS
        if qartod_primary_flag_name not in dataset:
            dataset[qartod_primary_flag_name] = (qc_obs_dimension, results, {})
            # add attribute info for QC flag interpretation
            dataset[qartod_primary_flag_name].attrs['flag_values'] = np.array(
                QartodFlags.getValidQCFlags()).astype(np.uint8)
            dataset[qartod_primary_flag_name].attrs['flag_meanings'] = ' '.join(QartodFlags.getQCFlagMeanings())
            dataset[qartod_primary_flag_name].attrs['long_name'] = qartod_primary_flag_name
        else:
            # combine qc results by keeping the most adverse flag for each observation
            current_qartod_flag_primary = dataset[qartod_primary_flag_name].values
            temp_qartod_flag_primary = np.maximum(current_qartod_flag_primary, results)
            dataset[qartod_primary_flag_name].values = temp_qartod_flag_primary

        # Work with results as an array of strings for the secondary flag
        results_string = results.astype('S1')

        # UPDATE SECONDARY FLAGS
        # represent QARTOD test results as a string of space separated test result integers (flags) in the order the
        # tests were run, as indicated by the 'tests_executed' attribute
        if qartod_secondary_flag_name not in dataset:
            dataset[qartod_secondary_flag_name] = (qc_obs_dimension, results_string, {})
            # add attribute info for QC flag interpretation
            dataset[qartod_secondary_flag_name].attrs['flag_values'] = np.array(
                QartodFlags.getValidQCFlags()).astype(np.uint8)
            dataset[qartod_secondary_flag_name].attrs['flag_meanings'] = ' '.join(QartodFlags.getQCFlagMeanings())
            dataset[qartod_secondary_flag_name].attrs['tests_executed'] = test
            dataset[qartod_secondary_flag_name].attrs['long_name'] = qartod_secondary_flag_name
        else:
            # combine qc results by appending results to the relevant string for each observation (i.e. all tests for
            # a given observation occur in the same string)
            current_qartod_flag_secondary = dataset[qartod_secondary_flag_name].values
            temp_qartod_flag_secondary = np.core.defchararray.add(current_qartod_flag_secondary, results_string)
            dataset[qartod_secondary_flag_name].values = temp_qartod_flag_secondary

            # update the attributes to detail which tests were run (and in what order)
            dataset[qartod_secondary_flag_name].attrs['tests_executed'] += ', ' + test
