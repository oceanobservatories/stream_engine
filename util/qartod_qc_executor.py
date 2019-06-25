import os
import io
import json
import importlib
import logging
from util.qartod_service import qartodTestServiceAPI

import numpy as np

from util.common import QartodFlags

log = logging.getLogger(__name__)

EXCEPTION_MESSAGE = "Logged Exception"

# QARTOD Parameter identification patterns
QARTOD_PRIMARY = 'qartod_flag_primary'
QARTOD_SECONDARY = 'qartod_flag_secondary'


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
            qartod_tests = qartodTestServiceAPI.find_qartod_tests_bulk(subsite, node, sensor, stream, parameters)

            for dataset in stream_dataset.datasets.itervalues():
                for qartod_test_record in qartod_tests:
                    self.execute_qartod_test(qartod_test_record, dataset)

    def execute_qartod_test(self, qartod_test_record, dataset):
        parameter = qartod_test_record.parameter
        test_identifier = qartod_test_record.test

        # if the full function path is specified, parse the module from the path; otherwise, assume the function is from
        # the default module and the test identifier is simply the function name
        if test_identifier.count(".") > 0:
            module, test = test_identifier.rsplit('.', 1)
        else:
            module = "ion_functions.qc.qartod_functions"
            test = test_identifier

        if parameter not in dataset:
            return

        # do some magical input processing here... assume that qartod_test_record.inputs is JSON
        inputs = json.loads(qartod_test_record.inputs)
        # only inputs that should require translation are ndarray references which are referenced by strings
        for key in inputs:
            value = inputs[key]
            if isinstance(value, basestring):
                if value in dataset:
                    if len(dataset[value].values.shape) > 1:
                        log.error('<%s> Attempted to run QC against >1d data %r %r',
                                  self.request_id, value, dataset[value].values.shape)
                        return
                    inputs[key] = dataset[value].values
                else:
                    log.error('<%s> Unable to find QC test input %r for function %r', self.request_id, key, test)
                    return

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
        else:
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
            elif results_string == EXCEPTION_MESSAGE:
                # an exception has already been logged, proceed with trying the next qc function
                return
            else:
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
        # make sure the array data type is correct - failure to do this can cause issues writing the NetCDF file later
        # specifically, without this the type is assumed int64 and recasting in netcdf_utils clears the attrs
        results = results.astype(np.uint8)

        qartod_primary_flag_name = '_'.join([parameter, "qartod_flag_primary"])
        qartod_secondary_flag_name = '_'.join([parameter, "qartod_flag_secondary"])
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
            # combine qc results by keeping the most adverse flag for each  observation
            current_qartod_flag_primary = dataset[qartod_primary_flag_name].values
            temp_qartod_flag_primary = np.maximum(current_qartod_flag_primary, results)
            dataset[qartod_primary_flag_name].values = temp_qartod_flag_primary

        # UPDATE SECONDARY FLAGS
        # represent QARTOD test results as a bit mask with bit position indicating which test, 0 indicating either a
        # good value or not evaluated (1 or 2), and 1 indicating suspect, bad, or missing (3, 4, or 9)
        if qartod_secondary_flag_name not in dataset:
            # convert results into bit mask with 0 for flags < 3 and 1 otherwise (i.e. 1 for tests indicating failures)
            # bit shift to the most significant bit
            results_mask = np.where(results < 3, 0, 1) << 7
            # make sure the array data type is correct - failure to do this can cause issues writing the NetCDF file
            # specifically, without this the type is assumed int64 and recasting in netcdf_utils clears the attrs
            results_mask = results_mask.astype(np.uint8)
            dataset[qartod_secondary_flag_name] = (qc_obs_dimension, results_mask, {})
            # add attribute info for QC flag interpretation
            # 1 << 7 = 128 the most significant bit for the first test result
            dataset[qartod_secondary_flag_name].attrs['flag_values'] = np.array([128]).astype(np.uint8)
            dataset[qartod_secondary_flag_name].attrs['flag_meanings'] = '_'.join([test.upper(), 'FAILED'])
            dataset[qartod_secondary_flag_name].attrs['tests_executed'] = test
            dataset[qartod_secondary_flag_name].attrs['long_name'] = qartod_secondary_flag_name
        else:
            # convert results into bit mask with 0 for flags < 3 and 1 otherwise (i.e. 1 for tests indicating failures)
            # bit shift to the correct position based on test order
            shift_value = 7 - len(dataset[qartod_secondary_flag_name].attrs['tests_executed'].split(','))
            results_mask = np.where(results < 3, 0, 1) << shift_value
            # make sure the array data type is correct - failure to do this can cause issues writing the NetCDF file
            # specifically, without this the type is assumed int64 and recasting in netcdf_utils clears the attrs
            results_mask = results_mask.astype(np.uint8)

            current_qartod_flag_secondary = dataset[qartod_secondary_flag_name].values
            dataset[qartod_secondary_flag_name].values = np.bitwise_or(current_qartod_flag_secondary, results_mask)

            # update the attributes to detail which tests were run (and in what order)
            dataset[qartod_secondary_flag_name].attrs['tests_executed'] += ', ' + test

            current_flag_meanings = dataset[qartod_secondary_flag_name].attrs['flag_values']
            dataset[qartod_secondary_flag_name].attrs['flag_values'] = np.append(
                current_flag_meanings, 1 << shift_value).astype(np.uint8)

            dataset[qartod_secondary_flag_name].attrs['flag_meanings'] += ' ' + '_'.join([test.upper(), 'FAILED'])
