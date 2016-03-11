import importlib
import logging
import ion_functions
import numexpr
import xray as xr
import numpy as np

from engine import app
from preload_database.model.preload import Parameter, Stream, NominalDepth
import util.annotation
import util.provenance_metadata_store
from util.advlogging import ParameterReport

from util.calibration_coefficient_store import CalibrationCoefficientStore

from util.cass import (build_stream_dictionary, get_available_time_range, fetch_l0_provenance,
                       get_streaming_provenance, get_location_metadata, fetch_nth_data, get_full_cass_dataset,
                       get_first_before_metadata, CASS_LOCATION_NAME, get_cass_lookback_dataset, SAN_LOCATION_NAME)

from util.common import (log_timing, StreamEngineException, StreamKey, MissingDataException,
                         UnknownFunctionTypeException, TimeRange, ntp_to_datestring)
from util.datamodel import compile_datasets, create_empty_dataset, add_location_data

from util.qc_executor import QcExecutor
from util.san import fetch_nsan_data, fetch_full_san_data, get_san_lookback_dataset
from util.xray_interpolation import interp1d_data_array

log = logging.getLogger()

ION_VERSION = getattr(ion_functions, '__version__', 'unversioned')
PRESSURE_DPI = app.config.get('PRESSURE_DPI')
GPS_STREAM_ID = app.config.get('GPS_STREAM_ID')
LATITUDE_PARAM_ID = app.config.get('LATITUDE_PARAM_ID')
LONGITUDE_PARAM_ID = app.config.get('LONGITUDE_PARAM_ID')
INT_PRESSURE_NAME = app.config.get('INT_PRESSURE_NAME')
MAX_DEPTH_VARIANCE = app.config.get('MAX_DEPTH_VARIANCE')


class StreamRequest(object):
    """
    Stores the information from a request, and calculates the required
    parameters and their streams
    """

    def __init__(self, stream_key, parameters, coefficients, time_range, uflags, qc_parameters=None,
                 limit=None, include_provenance=False, include_annotations=False, strict_range=False,
                 location_information=None, request_id='', collapse_times=False):

        if not isinstance(stream_key, StreamKey):
            raise StreamEngineException('Received no stream key', status_code=400)

        # Inputs
        self.request_id = request_id
        self.stream_key = stream_key
        self.requested_parameters = parameters
        self.coefficients = CalibrationCoefficientStore(coefficients, request_id)
        self.time_range = time_range
        self.uflags = uflags
        self.qc_executor = QcExecutor(qc_parameters, self)
        self.limit = limit
        self.include_provenance = include_provenance
        self.include_annotations = include_annotations
        self.strict_range = strict_range
        self.location_information = location_information if location_information is not None else {}

        # Internals
        self.stream_parameters = {}
        self.unfulfilled = set()
        self.provenance_metadata = util.provenance_metadata_store.ProvenanceMetadataStore(request_id)
        self.annotation_store = util.annotation.AnnotationStore()
        self.datasets = {}
        self.external_includes = {}

        self._initialize()

        if collapse_times:
            self._collapse_times()

    def __repr__(self):
        return str(self.__dict__)

    @property
    def needs_cc(self):
        """
        Return the list of calibration coefficients necessary to compute all data products for this request
        :return:
        """
        stream_list = []
        for sk in self.stream_parameters:
            needs = list(sk.stream.needs_cc)
            d = sk.as_dict()
            d['coefficients'] = needs
            stream_list.append(d)
        return stream_list

    @log_timing(log)
    def fetch_raw_data(self):
        """
        Fetch the source data for this request
        :return:
        """
        for stream_key, stream_parameters in self.stream_parameters.iteritems():
            should_pad = stream_key != self.stream_key
            if not stream_key.is_virtual:
                log.debug('<%s> Fetching raw data for %s', self.request_id, stream_key.as_refdes())
                ds = get_dataset(stream_key, self.time_range, self.limit,
                                 self.provenance_metadata, should_pad, [], self.request_id)
                if ds is not None:
                    self.datasets[stream_key] = ds

                else:
                    if stream_key == self.stream_key:
                        raise MissingDataException("Query returned no results for primary stream")
                    elif stream_key.stream in self.stream_key.stream.source_streams:
                        raise MissingDataException("Query returned no results for source stream")
            else:
                log.debug('<%s> Creating empty dataset for virtual stream: %s',
                          self.request_id, stream_key.as_refdes())
                self.datasets[stream_key] = create_empty_dataset(stream_key, self.request_id)

    # noinspection PyTypeChecker
    def _insert_provenance(self):
        """
        Insert all source provenance for this request. This is dependent on the data already having been fetched.
        :return:
        """
        if self.include_provenance:
            self.provenance_metadata.add_query_metadata(self, self.request_id, 'JSON')
            for stream_key in self.stream_parameters:
                self.provenance_metadata.add_instrument_provenance(stream_key, self.time_range.start,
                                                                   self.time_range.stop)

                if stream_key.method not in ['streamed', ]:
                    provenance = self.datasets[stream_key].provenance.values.astype('str')
                    for deployment in np.unique(self.datasets[stream_key].deployment.values):
                        prov = fetch_l0_provenance(stream_key, provenance, deployment)
                        self.provenance_metadata.update_provenance(prov)
                else:
                    # Get the ids for times and get the provenance information
                    times = self.datasets[stream_key].time.values
                    prov_ids, prov_dict = get_streaming_provenance(stream_key, times)
                    self.provenance_metadata.update_streaming_provenance(prov_dict)

    def _insert_annotations(self):
        """
        Insert all annotations for this request. This is dependent on the data already having been fetched.
        :return:
        """
        if self.include_annotations:
            for stream_key in self.stream_parameters:
                self.annotation_store.add_annotations(
                        util.annotation.query_annotations(stream_key, self.time_range))

    def _log_algorithm_inputs(self, parameter, kwargs):
        flag = self.uflags.get('advancedStreamEngineLogging', False)
        user = self.uflags.get('userName', '_nouser')

        if flag:
            log.debug('<%s> _log_algorithm_inputs (%r)', self.request_id, parameter)
            log_name = '{:s}-{:s}'.format(self.stream_key.as_dashed_refdes(), parameter.name)
            report = ParameterReport(user, self.request_id, log_name)
            report.set_calculated_parameter(parameter.id, parameter.name, parameter.parameter_function.function)
            for key, value in kwargs.iteritems():
                report.add_parameter_argument(parameter.id, key, value.tolist())
            report.write()

    @log_timing(log)
    def _execute_algorithm(self, parameter, kwargs):
        """
        Executes a single derived product algorithm
        """
        func = parameter.parameter_function
        log.debug('<%s> _execute_algorithm Parameter: %r', self.request_id, parameter)
        log.debug('<%s> _execute_algorithm Function %r', self.request_id, func)
        log.debug('<%s> _execute_algorithm Keyword Args %r', self.request_id, sorted(kwargs))

        try:
            if func.function_type == 'PythonFunction':
                module = importlib.import_module(func.owner)
                version = ION_VERSION
                result = getattr(module, func.function)(**kwargs)

            elif func.function_type == 'NumexprFunction':
                version = 'unversioned'
                result = numexpr.evaluate(func.function, kwargs)

            else:
                to_attach = {'type': 'UnknownFunctionError',
                             "parameter": parameter,
                             'function': str(func.function_type)}
                raise UnknownFunctionTypeException(func.function_type.value, payload=to_attach)

        except UnknownFunctionTypeException:
            raise
        except Exception as e:
            log.exception('Except!')
            to_attach = {'type': 'FunctionError', "parameter": parameter,
                         'function': str(func.id) + " " + str(func.description)}
            raise StreamEngineException('%s threw exception: %s' % (func.function_type, e), payload=to_attach)

        return result, version

    def _create_parameter_metadata(self, stream, param, interpolated=False):
        """
        Given a source stream and parameter, generate the corresponding parameter metadata
        :param stream: Source stream
        :param param: Parameter
        :param interpolated: Boolean indicating if this data was interpolated
        :return: Dictionary containing metadata describing this Stream/Parameter
        """
        stream_keys = {sk.stream: sk for sk in self.datasets}
        stream_key = stream_keys.get(stream)
        if stream_key:
            dataset = self.datasets[stream_key]
            times = dataset.time.values
            t1, t2 = times[0], times[-1]
            t1_dt, t2_dt = ntp_to_datestring(t1), ntp_to_datestring(t2)
            deployments = list(np.unique(dataset.deployment.values))
            return {'type': "parameter",
                    'source': stream_key.as_refdes(),
                    'parameter_id': param.id,
                    'name': param.name,
                    'data_product_identifier': param.data_product_identifier,
                    'interpolated': interpolated,
                    'time_start': t1,
                    'time_startDT': t1_dt,
                    'time_end': t2,
                    'time_endDT': t2_dt,
                    'deployments': deployments}

    def _build_function_arguments(self, dataset, stream_key, funcmap, deployment, source_dataset=None):
        """
        Build the arguments needed to execute a data product algorithm
        :param dataset: Dataset containing the data
        :param stream_key: StreamKey corresponding to dataset
        :param funcmap: The computed function map {name: (source, value)}
        :param deployment: Deployment number being processed
        :param source_dataset: Optional parameter. If supplied, stream is virtual and depends on
                               un-interpolated values from this dataset.
        :return:
        """
        kwargs = {}
        if source_dataset:
            times = source_dataset.time.values
        else:
            times = dataset.time.values

        t1 = times[0]
        t2 = times[-1]
        begin_dt, end_dt = ntp_to_datestring(t1), ntp_to_datestring(t2)
        arg_metadata = {
            'time_source': {
                'begin': t1,
                'end': t2,
                'beginDT': begin_dt,
                'endDT': end_dt,
            }}

        # Step through each item in the function map
        for name, (source, value) in funcmap.iteritems():
            param_meta = None
            # Calibration Value
            if source == 'CAL':
                cal, param_meta = self.coefficients.get(value, deployment, times)
                if cal is not None:
                    kwargs[name] = cal
                    if np.any(np.isnan(cal)):
                        msg = '<{:s}> There was not coefficient data for {:s} for all times in deployment ' \
                              '{:d} in range ({:s} {:s})'.format(self.request_id, name, deployment, begin_dt, end_dt)
                        log.warn(msg)

            # Internal Parameter
            elif source == stream_key.stream and value.name in dataset:
                kwargs[name] = dataset[value.name].values
                param_meta = self._create_parameter_metadata(source, value)

            # Virtual stream parameter
            elif source_dataset and value.name in source_dataset:
                kwargs[name] = source_dataset[value.name].values
                param_meta = self._create_parameter_metadata(source, value)

            # External Parameter
            else:
                new_name = '-'.join((source.name, value.name))
                if new_name in dataset:
                    kwargs[name] = dataset[new_name].values
                    param_meta = self._create_parameter_metadata(source, value, True)

            if param_meta is not None:
                arg_metadata[name] = param_meta

        return kwargs, arg_metadata

    @staticmethod
    def _create_calculation_metadata(param, version, arg_metadata):
        calc_meta = {'function_name': param.parameter_function.function,
                     'function_type': param.parameter_function.function_type,
                     'function_version': version,
                     'function_id': param.parameter_function.id,
                     'function_owner': param.parameter_function.owner,
                     'argument_list': [arg for arg in param.parameter_function_map],
                     'arguments': arg_metadata}
        return calc_meta

    @log_timing(log)
    def _create_derived_product(self, dataset, stream_key, param, deployment, source_dataset=None):
        """
        Extract the necessary args to create the derived product <param>, call _execute_algorithm
        and insert the result back into dataset.
        :param dataset: source data
        :param stream_key: source stream
        :param param: derived parameter
        :param deployment: deployment number
        :return:
        """
        log.info('<%s> _create_derived_product %r %r', self.request_id, stream_key.as_refdes(), param)
        streams = {sk.stream for sk in self.datasets}

        function_map, missing = stream_key.stream.create_function_map(param, streams.difference({stream_key}))

        if not missing:
            kwargs, arg_metadata = self._build_function_arguments(dataset, stream_key, function_map,
                                                                  deployment, source_dataset)

            if set(kwargs) == set(function_map):
                self._log_algorithm_inputs(param, kwargs)
                result, version = self._execute_algorithm(param, kwargs)
                dims = ['obs']
                for index, _ in enumerate(result.shape[1:]):
                    name = '%s_dim_%d' % (param.name, index)
                    dims.append(name)

                if 'obs' not in dataset or len(result) == len(dataset.obs.values):
                    dataset[param.name] = (dims, result, {})
                    calc_metadata = self._create_calculation_metadata(param, version, arg_metadata)
                    self.provenance_metadata.calculated_metatdata.insert_metadata(param, calc_metadata)
                else:
                    log.error('<%s> Result from algorithm length mismatch, got: %r expected: %r',
                              self.request_id, len(result), len(dataset.time.values))

        else:
            error_info = {'derived_id': param.id, 'derived_name': param.name,
                          'derived_display_name': param.display_name, 'missing': []}
            for key in missing:
                source, value = missing[key]
                missing_dict = {
                    'source': source,
                    'value': value
                }
                error_info['missing'].append(missing_dict)

            self.provenance_metadata.calculated_metatdata.errors.append(error_info)

            log.error('<%s> Unable to create derived product: %r missing: %r',
                      self.request_id, param.name, error_info)

    @log_timing(log)
    def _interpolate(self, source_key, target_key, parameter):
        """
        Interpolate <parameter> from the dataset defined by source_key to the dataset defined by target_key
        :param source_key: StreamKey representing the data source
        :param target_key: StreamKey representing the data destination
        :param parameter: Parameter defining the data to be interpolated
        :return:
        """
        # Don't interpolate back to the source stream. Simplifies code upstream
        if source_key == target_key:
            return

        source_dataset = self.datasets[source_key]
        target_dataset = self.datasets[target_key]
        name = parameter.name
        if name in source_dataset:
            new_name = '-'.join((source_key.stream.name, name))
            if new_name not in target_dataset:
                log.debug('<%s> Interpolating %r from %s into %s as %s', self.request_id,
                          parameter, source_key.as_refdes(), target_key.as_refdes(), new_name)
                interp_values = interp1d_data_array(source_dataset.time.values,
                                                    source_dataset[name],
                                                    time=target_dataset.time)
                target_dataset[new_name] = interp_values

    @log_timing(log)
    def _import_interpolated(self, stream_key, param):
        """
        Given a StreamKey and Parameter, calculate the parameters which need to be interpolated into
        the dataset defined by StreamKey for Parameter
        :param stream_key: StreamKey defining the target dataset
        :param param: Parameter defining the L2 parameter which requires data from an external dataset
        :return:
        """
        log.debug('<%s> _import_interpolated: %r %r', self.request_id, stream_key.as_refdes(), param)
        streams = {sk.stream: sk for sk in self.datasets}
        funcmap, missing = stream_key.stream.create_function_map(param, set(streams).difference({stream_key}))
        if not missing:
            for name in funcmap:
                source, value = funcmap[name]
                if source not in ['CAL', stream_key.stream]:
                    source_key = streams.get(source)
                    self._interpolate(source_key, stream_key, value)
        else:
            log.error('<%s> Unable to interpolate data: %r from: %r error locating data',
                      self.request_id, param, stream_key.as_refdes())

    def _calculate_parameter_list(self, dataset, sk, params, deployment, name):
        if params:
            log.info('<%s> executing %s algorithms for %r deployment %d',
                     self.request_id, name, sk.as_refdes(), deployment)
            for param in params:
                self._create_derived_product(dataset, sk, param, deployment)

    @log_timing(log)
    def calculate_derived_products(self):
        """
        Calculate all derived products for this StreamRequest
        :return:
        """
        log.info('<%s> calculate_derived_products', self.request_id)
        for sk in self.datasets:
            if sk.is_virtual:
                continue

            datasets = []
            l1_params = [param for param in sk.stream.derived if param.is_l1 and not sk.stream.needs_external([param])]
            l2_params = [param for param in sk.stream.derived if param.is_l2 and not sk.stream.needs_external([param])]

            if l1_params or l2_params:
                # Calculate internal L1 and L2 parameters
                for deployment, dataset in self.datasets[sk].groupby('deployment'):
                    self._calculate_parameter_list(dataset, sk, l1_params, deployment, 'internal L1')
                    self._calculate_parameter_list(dataset, sk, l2_params, deployment, 'internal L2')
                    datasets.append(dataset)
                self.datasets[sk] = xr.concat(datasets, 'obs')

        # calculate external L2
        for sk in self.datasets:
            if sk.is_virtual:
                continue
            # create a list of external L1 and L2 parameters
            external = [param for param in sk.stream.derived if sk.stream.needs_external([param])]
            external_l1 = [param for param in external if param.is_l1]
            external_l2 = [param for param in external if param.is_l2]

            if external:
                log.info('<%s> interpolating external data for L2 algorithms for %r', self.request_id, sk.as_refdes())
                for param in external:
                    self._import_interpolated(sk, param)

                datasets = []
                # Calculate internal L1 and L2 parameters
                for deployment, dataset in self.datasets[sk].groupby('deployment'):
                    self._calculate_parameter_list(dataset, sk, external_l1, deployment, 'external L1')
                    self._calculate_parameter_list(dataset, sk, external_l2, deployment, 'external L2')
                    datasets.append(dataset)
                self.datasets[sk] = xr.concat(datasets, 'obs')

        # Calculate virtual streams
        for sk in self.datasets:
            if sk.is_virtual:
                log.info('<%s> Compute virtual stream', self.request_id)
                source_key = None
                for key in self.datasets:
                    if key.stream in sk.stream.source_streams:
                        source_key = key
                        break

                dataset = self.datasets[sk]
                datasets = []
                for deployment, source_dataset in self.datasets[source_key].groupby('deployment'):
                    # compute the time parameter
                    time_param = Parameter.query.get(sk.stream.time_parameter)
                    self._create_derived_product(self.datasets[sk], sk, time_param, deployment,
                                                 source_dataset=source_dataset)
                    dataset['time'] = dataset[time_param.name].copy()
                    for param in sk.stream.parameters:
                        if param != time_param:
                            self._create_derived_product(self.datasets[sk], sk, param, deployment,
                                                         source_dataset=source_dataset)
                    datasets.append(dataset)
                self.datasets[sk] = compile_datasets(datasets)

        self._run_qc()
        self._insert_provenance()
        self._insert_annotations()
        self._add_location()

    @log_timing(log)
    def _run_qc(self):
        # execute any QC
        for sk, dataset in self.datasets.iteritems():
            for param in sk.stream.parameters:
                self.qc_executor.qc_check(param, dataset)

    def import_extra_externals(self):
        # import any other required "externals" into all datasets
        for source_sk in self.external_includes:
            for param in self.external_includes[source_sk]:
                for target_sk in self.datasets:
                    self._interpolate(source_sk, target_sk, param)

    def _add_location(self):
        log.debug('<%s> Inserting location data for all datasets', self.request_id)
        for sk in self.datasets:
            if not sk.is_glider:
                add_location_data(self.datasets[sk], sk, self.location_information, self.request_id)

    def _locate_externals(self, parameters):
        """
        Locate external data sources for the given list of parameters
        :param parameters: list of type Parameter
        :return: found parameters as dict(StreamKey, Parameter), unfulfilled parameters as set(Parameter)
        """
        log.debug('<%s> _locate_externals: %r', self.request_id, parameters)
        external_to_process = set(parameters)
        found = {}
        external_unfulfilled = set()
        stream_parameters = {}

        def process_found_stream(stream_key, parameter):
            """
            Internal subroutine to process each found stream/parameter
            :param stream_key: StreamKey found by find_stream
            :param parameter: Parameter inside found stream
            :return: None
            """
            found.setdefault(stream_key, set()).add(parameter)
            sk_needs_internal = stream_key.stream.needs_internal([parameter])
            sk_needs_external = stream_key.stream.needs_external([parameter])
            log.debug('<%s> _locate_externals FOUND INT: %r %r', self.request_id,
                      stream_key.as_refdes(), sk_needs_internal)
            log.debug('<%s> _locate_externals FOUND EXT: %r %r', self.request_id,
                      stream_key.as_refdes(), sk_needs_external)

            # Add externals not yet processed to the to_process set
            for sub_need in sk_needs_external:
                if sub_need not in external_unfulfilled:
                    external_to_process.add(sub_need)
            # Add internal parameters to the corresponding stream set
            stream_parameters.setdefault(stream_key, set()).update(sk_needs_internal)

        while external_to_process:
            # Pop an external from the list of externals to process
            external = external_to_process.pop()
            stream, poss_params = external
            log.debug('<%s> _locate_externals: STREAM: %r POSS_PARAMS: %r', self.request_id, stream, poss_params)
            found_sk, found_param = self.find_stream(self.stream_key, poss_params, stream=stream)
            if found_sk:
                process_found_stream(found_sk, found_param)
            else:
                external_unfulfilled.add(external)

        return stream_parameters, found, external_unfulfilled

    @log_timing(log)
    def _get_mobile_externals(self):
        """
        For mobile assets, build the set of externals necessary to provide location data
        :return: set((Stream, (Parameter,)))
        """
        external_to_process = set()
        if self.stream_key.is_mobile:
            dpi = PRESSURE_DPI
            external_to_process.add((None, tuple(Parameter.query.filter(
                    Parameter.data_product_identifier == dpi).all())))

        if self.stream_key.is_glider:
            gps_stream = Stream.query.get(GPS_STREAM_ID)
            external_to_process.add((gps_stream, (Parameter.query.get(LATITUDE_PARAM_ID),)))
            external_to_process.add((gps_stream, (Parameter.query.get(LONGITUDE_PARAM_ID),)))
        return external_to_process

    @log_timing(log)
    def _initialize(self):
        """
        Initialize stream request. Computes data sources / parameters
        :return:
        """
        # Build our list of internally requested parameters
        if self.requested_parameters:
            internal_requested = [p for p in self.stream_key.stream.parameters if p.id in self.requested_parameters]
        else:
            internal_requested = self.stream_key.stream.parameters
        self.requested_parameters = internal_requested

        # Identify internal parameters needed to support this query
        primary_internals = self.stream_key.stream.needs_internal(internal_requested)
        log.debug('<%s> primary stream internal needs: %r', self.request_id, primary_internals)
        self.stream_parameters[self.stream_key] = primary_internals

        # Identify external parameters needed to support this query
        external_to_process = self.stream_key.stream.needs_external(internal_requested)
        log.debug('<%s> primary stream external needs: %r', self.request_id, external_to_process)
        if external_to_process:
            stream_parameters, found, external_unfulfilled = self._locate_externals(external_to_process)
            for sk in stream_parameters:
                self.stream_parameters.setdefault(sk, set()).update(stream_parameters[sk])
            self.unfulfilled = external_unfulfilled

        # Now identify any parameters needed for mobile assets
        external_to_process = self._get_mobile_externals()
        if external_to_process:
            stream_parameters, found, external_unfulfilled = self._locate_externals(external_to_process)
            for sk in stream_parameters:
                self.stream_parameters.setdefault(sk, set()).update(stream_parameters[sk])
            self.unfulfilled = self.unfulfilled.union(external_unfulfilled)
            self.external_includes.update(found)

    @log_timing(log)
    def _collapse_times(self):
        """
        Collapse request times to match available data
        :return:
        """
        if self.stream_key.is_virtual:
            # collapse to smallest of all source streams
            tr = self.time_range.copy()
            for sk in self.stream_parameters:
                if sk.is_virtual:
                    continue
                tr = tr.collapse(get_available_time_range(sk))
            new_time_range = self.time_range.collapse(tr)
            if new_time_range:
                log.info('Collapsing requested time range: %s to available time range: %s',
                         self.time_range, new_time_range)
                self.time_range = new_time_range

        else:
            # collapse to primary stream
            new_time_range = self.time_range.collapse(get_available_time_range(self.stream_key))
            if new_time_range:
                log.info('Collapsing requested time range: %s to available time range: %s',
                         self.time_range, new_time_range)
                self.time_range = new_time_range

    @log_timing(log)
    def find_stream(self, stream_key, poss_params, stream=None):
        subsite = stream_key.subsite
        node = stream_key.node
        sensor = stream_key.sensor
        stream_dictionary = build_stream_dictionary()

        # First, try to find the stream on the same sensor
        for p in poss_params:
            search_streams = [stream] if stream else p.streams
            sk = self._find_stream_same_sensor(stream_key, search_streams, stream_dictionary)
            if sk:
                return sk, p

        # Attempt to find an instrument at the same depth (if not mobile)
        if not stream_key.is_mobile:
            nominal_depth = NominalDepth.get_nominal_depth(subsite, node, sensor)
            if nominal_depth is not None:
                co_located = nominal_depth.get_colocated_subsite()
                for p in poss_params:
                    search_streams = [stream] if stream else p.streams
                    sk = self._find_stream_from_list(stream_key, search_streams, co_located, stream_dictionary)
                    if sk:
                        return sk, p

        # Attempt to find an instrument on the same node
        for p in poss_params:
            search_streams = [stream] if stream else p.streams
            sk = self._find_stream_same_node(stream_key, search_streams, stream_dictionary)
            if sk:
                return sk, p

        # Not found at same depth, attempt to find nearby (if not mobile)
        if not stream_key.is_mobile:
            nominal_depth = NominalDepth.get_nominal_depth(subsite, node, sensor)
            if nominal_depth is not None:
                nearby = nominal_depth.get_depth_within(MAX_DEPTH_VARIANCE)
                for p in poss_params:
                    search_streams = [stream] if stream else p.streams
                    sk = self._find_stream_from_list(stream_key, search_streams, nearby, stream_dictionary)
                    if sk:
                        return sk, p

        return None, None

    @staticmethod
    def _find_stream_same_sensor(stream_key, streams, stream_dictionary):
        """
        Given a primary source, attempt to find one of the supplied streams from the same instrument
        :param stream_key:
        :param streams:
        :return:
        """
        method = stream_key.method
        subsite = stream_key.subsite
        node = stream_key.node
        sensor = stream_key.sensor

        # Search the same reference designator
        for stream in streams:
            sensors = stream_dictionary.get(stream.name, {}).get(method, {}).get(subsite, {}).get(node, [])
            if sensor in sensors:
                return StreamKey.from_dict({
                    "subsite": subsite,
                    "node": node,
                    "sensor": sensor,
                    "method": method,
                    "stream": stream.name
                })

    @staticmethod
    def _find_stream_from_list(stream_key, streams, sensors, stream_dictionary):
        method = stream_key.method
        subsite = stream_key.subsite
        designators = [(c.subsite, c.node, c.sensor) for c in sensors]

        for stream in streams:
            subsite_dict = stream_dictionary.get(stream.name, {}).get(method, {}).get(subsite, {})
            for _node in subsite_dict:
                for _sensor in subsite_dict[_node]:
                    des = (subsite, _node, _sensor)
                    if des in designators:
                        return StreamKey.from_dict({
                            "subsite": subsite,
                            "node": _node,
                            "sensor": _sensor,
                            "method": method,
                            "stream": stream.name
                        })

    @staticmethod
    def _find_stream_same_node(stream_key, streams, stream_dictionary):
        """
        Given a primary source, attempt to find one of the supplied streams from the same instrument,
        same node or same subsite
        :param stream_key: StreamKey - defines the source of the primary stream
        :param streams: List - list of target streams
        :return: StreamKey if found, otherwise None
        """
        if not isinstance(streams, (list, tuple)):
            streams = [streams]

        method = stream_key.method
        subsite = stream_key.subsite
        node = stream_key.node

        for stream in streams:
            sensors = stream_dictionary.get(stream.name, {}).get(method, {}).get(subsite, {}).get(node, [])
            if sensors:
                return StreamKey.from_dict({
                    "subsite": subsite,
                    "node": node,
                    "sensor": sensors[0],
                    "method": method,
                    "stream": stream.name
                })


@log_timing(log)
def get_dataset(key, time_range, limit, provenance_metadata, pad_forward, deployments, request_id=None):
    """

    :param key:
    :param time_range:
    :param limit:
    :param provenance_metadata:
    :param pad_forward:
    :param deployments:
    :param request_id:
    :return:
    """
    cass_locations, san_locations, messages = get_location_metadata(key, time_range)
    provenance_metadata.add_messages(messages)
    # check for no data
    datasets = []
    total = float(san_locations.total + cass_locations.total)
    san_percent = cass_percent = 0
    if total != 0:
        san_percent = san_locations.total / total
        cass_percent = cass_locations.total / total

    if pad_forward:
        # pad forward on some datasets
        datasets.append(get_lookback_dataset(key, time_range, deployments, request_id))

    if san_locations.total > 0:
        # put the range down if we are within the time range
        t1 = max(time_range.start, san_locations.start_time)
        t2 = min(time_range.stop, san_locations.end_time)
        san_times = TimeRange(t1, t2)
        if limit:
            datasets.append(fetch_nsan_data(key, san_times, num_points=int(limit * san_percent),
                                            location_metadata=san_locations))
        else:
            datasets.append(fetch_full_san_data(key, san_times, location_metadata=san_locations))
    if cass_locations.total > 0:
        t1 = max(time_range.start, cass_locations.start_time)
        t2 = min(time_range.stop, cass_locations.end_time)
        # issues arise when sending cassandra a query with the exact time range.  Data points at the start and end will
        # be left out of the results.  This is an issue for full data queries.  To compensate for this we add .1 seconds
        # to the given start and end time
        t1 -= .1
        t2 += .1
        cass_times = TimeRange(t1, t2)
        if limit:
            datasets.append(fetch_nth_data(key, cass_times, num_points=int(limit * cass_percent),
                                           location_metadata=cass_locations, request_id=request_id))
        else:
            datasets.append(get_full_cass_dataset(key, cass_times,
                                                  location_metadata=cass_locations, request_id=request_id))
    return compile_datasets(datasets)


@log_timing(log)
def get_lookback_dataset(key, time_range, deployments, request_id=None):
    first_metadata = get_first_before_metadata(key, time_range.start)
    if CASS_LOCATION_NAME in first_metadata:
        locations = first_metadata[CASS_LOCATION_NAME]
        return get_cass_lookback_dataset(key, time_range.start, locations.bin_list[0], deployments, request_id)
    elif SAN_LOCATION_NAME in first_metadata:
        locations = first_metadata[SAN_LOCATION_NAME]
        return get_san_lookback_dataset(key, TimeRange(locations.start_time, time_range.start), locations.bin_list[0],
                                        deployments)
    else:
        return None
