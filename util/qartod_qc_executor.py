import os
import io
import json
import engine
import importlib
import logging
from util.qartod_service import qartodTestServiceAPI

import numpy as np

from util.common import QartodFlags

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
        test_identifier = qartod_test_record.test

        if parameter not in dataset:
            return

        # if the full function path is specified, parse the module from the path; otherwise, assume the function is from
        # the default module and the test identifier is simply the function name
        if test_identifier.count('.'):
            module, test = test_identifier.rsplit('.', 1)
        else:
            module = DEFAULT_QARTOD_TEST_MODULE
            test = test_identifier

        # do some magical input processing here... assume that qartod_test_record.inputs is JSON
        inputs = json.loads(qartod_test_record.inputs)
        # only inputs that should require translation are ndarray references which are referenced by strings
        for key, value in inputs.iteritems():
            if isinstance(value, basestring):
                if value not in dataset:
                    log.error('<%s> Unable to find QC test input %r for function %r', self.request_id, key, test)
                    return

                if len(dataset[value].values.shape) > 1:
                    log.error('<%s> Attempted to run QC against >1d data %r %r',
                              self.request_id, value, dataset[value].values.shape)
                    return

                inputs[key] = dataset[value].values

        module = importlib.import_module(module)

        # call QARTOD test in a separate process to deal with crashes, e.g. segfaults
        read_fd, write_fd = os.pipe()
        processid = os.fork()
        if processid == 0:
            # child process
            with os.fdopen(write_fd, 'w') as w:
                os.close(read_fd)
                # run the qc function
                try:
                    results = getattr(module, test)(**inputs)
                    # convert the np.ndarray into a string for sending over pipe
                    rbytes = io.BytesIO()
                    np.savez(rbytes, x=results)
                    results_string = rbytes.getvalue()
                    w.write(results_string)
                except (TypeError, ValueError) as e:
                    log.exception('<%s> Failed to execute QC %s %r', self.request_id, test, e)
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
            log.error('<%s> Failed to execute QC %s: QC process failed to return any data', self.request_id, test)
            return

        if results_string == EXCEPTION_MESSAGE:
            # an exception has already been logged, proceed with trying the next qc function
            return

        # load the np.ndarray from the sent results string
        data = np.load(io.BytesIO(results_string))
        results = data['x']

        # Verify all QC results are valid QARTOD Primary Level Flags
        mask = np.array([item not in QartodFlags.getValidQCFlags() for item in results])
        if mask.any():
            log.error('Received QC result with invalid QARTOD Primary Flag from %s. Invalid flags: %r',
                      test, np.unique(results[mask]))
            # Use the "high interest" (SUSPECT) flag to draw attention to the failure
            results[mask] = QartodFlags.SUSPECT

        QartodQcExecutor.insert_qc_results(parameter, test, results, dataset)

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
            print(temp_qartod_flag_secondary)
            dataset[qartod_secondary_flag_name].values = temp_qartod_flag_secondary

            # update the attributes to detail which tests were run (and in what order)
            dataset[qartod_secondary_flag_name].attrs['tests_executed'] += ', ' + test
