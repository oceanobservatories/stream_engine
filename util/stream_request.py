import importlib
import logging
import ion_functions
import numexpr
import xray
import numpy as np

from preload_database.model.preload import Parameter, Stream
import util.annotation
import util.provenance_metadata_store

from util.calibration_coefficient_store import CalibrationCoefficientStore
from util.cass import (build_stream_dictionary, get_available_time_range, fetch_l0_provenance,
                       get_streaming_provenance, get_location_metadata, fetch_nth_data, get_full_cass_dataset,
                       get_first_before_metadata, CASS_LOCATION_NAME, get_cass_lookback_dataset, SAN_LOCATION_NAME)
from util.common import (log_timing, StreamEngineException, StreamKey, MissingDataException,
                         UnknownFunctionTypeException, TimeRange, compile_datasets, ntp_to_datestring)
from util.qc_executor import QcExecutor
from util.san import fetch_nsan_data, fetch_full_san_data, get_san_lookback_dataset
from util.xray_interpolation import interp1d_DataArray

log = logging.getLogger()
ION_VERSION = getattr(ion_functions, '__version__', 'unversioned')


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
                self.datasets[stream_key] = get_dataset(stream_key, self.time_range, self.limit,
                                                        self.provenance_metadata, should_pad, [])

                if self.datasets[stream_key] is None:
                    if stream_key == self.stream_key:
                        raise MissingDataException("Query returned no results for primary stream")
                    elif stream_key.stream in self.stream_key.stream.source_streams:
                        raise MissingDataException("Query returned no results for source stream")

        self._insert_provenance()
        self._insert_annotations()

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

    @log_timing(log)
    def _execute_algorithm(self, parameter, kwargs):
        """
        Executes a single derived product algorithm
        """
        func = parameter.parameter_function
        log.debug('<%s> _execute_algorithm Parameter: %r', self.request_id, parameter)
        log.debug('<%s> _execute_algorithm Function %r', self.request_id, func)
        log.debug('<%s> _execute_algorithm Keyword Args %r', self.request_id, kwargs.keys())

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

    def _create_parameter_metadata(self, source, value, interpolated=False):
        stream_keys = {sk.stream: sk for sk in self.datasets}
        stream_key = stream_keys.get(source)
        if stream_key:
            dataset = self.datasets[stream_key]
            times = dataset.time.values
            deployments = np.unique(dataset.deployment.values)
            param_meta = {}
            param = value
            param_meta['type'] = "parameter"
            param_meta['source'] = stream_key.as_refdes()
            param_meta['parameter_id'] = param.id
            param_meta['name'] = param.name
            param_meta['data_product_identifier'] = param.data_product_identifier
            param_meta['iterpolated'] = interpolated
            param_meta['time_start'] = times[0]
            param_meta['time_startDT'] = ntp_to_datestring(times[0])
            param_meta['time_end'] = times[-1]
            param_meta['time_endDT'] = ntp_to_datestring(times[-1])
            param_meta['deployments'] = deployments
            return param_meta

    def _build_function_arguments(self, dataset, stream_key, funcmap, deployment):
        kwargs = {}
        messages = []
        begin, end = dataset.time.values[0], dataset.time.values[-1]
        begin_dt, end_dt = ntp_to_datestring(begin), ntp_to_datestring(end)
        arg_metadata = {
            'time_source': {
                'begin': begin,
                'end': end,
                'beginDT': begin_dt,
                'endDT': end_dt,
            }}

        # Step through each item in the function map
        for name, (source, value) in funcmap.iteritems():
            param_meta = None
            # Calibration Value
            if source == 'CAL':
                cal, param_meta = self.coefficients.get(value, deployment, dataset.time.values)
                if cal is not None:
                    kwargs[name] = cal
                    if np.any(np.isnan(cal)):
                        msg = 'There was not Coefficient data for {:s} all times in deployment' \
                              '{:d} in range ({:s} {:s})'.format(name, deployment, begin_dt, end_dt)
                        log.warn(msg)
                        messages.append(msg)

                        # Internal Parameter
            elif source == stream_key.stream and value.name in dataset:
                kwargs[name] = dataset[value.name].values
                param_meta = self._create_parameter_metadata(source, value)

            # External Parameter
            else:
                new_name = '-'.join((source.name, value.name))
                if new_name in dataset:
                    kwargs[name] = dataset[new_name].values
                    param_meta = self._create_parameter_metadata(source, value, True)

            if param_meta is not None:
                arg_metadata[name] = param_meta

        return kwargs, arg_metadata, messages

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
    def _create_derived_product(self, dataset, stream_key, param, deployment):
        """
        Extract the necessary args to create the derived product <param>, call _execute_algorithm
        and insert the result back into dataset.
        :param dataset: source data
        :param stream_key: source stream
        :param param: derived parameter
        :param deployment: deployment number
        :return:
        """
        log.debug('<%s> _create_derived_product %r %r', self.request_id, stream_key.as_refdes(), param)
        streams = {sk.stream for sk in self.datasets}
        function_map = stream_key.stream.create_function_map(param, streams.difference({stream_key}))
        kwargs, arg_metadata, messages = self._build_function_arguments(dataset, stream_key, function_map, deployment)

        if set(kwargs) == set(function_map):
            result, version = self._execute_algorithm(param, kwargs)
            dims = ['index']
            for index, dimension in enumerate(result.shape[1:]):
                name = '%s_dim_%d' % (param.name, index)
                dims.append(name)
            dataset[param.name] = (dims, result, {})
            calc_metadata = self._create_calculation_metadata(param, version, arg_metadata)
            self.provenance_metadata.calculated_metatdata.insert_metadata(param, calc_metadata)
        else:
            # TODO create "error" metadata
            error_info = {'derived_id': param.id, 'derived_name': param.name,
                          'derived_display_name': param.display_name}
            # if 'pdRef' in error_info:
            #     pdRef = error_info.pop('pdRef')
            #     error_parameter = Parameter.query.get(pdRef.pdid)
            #     error_info['missing_id'] = error_parameter.id
            #     error_info['missing_name'] = error_parameter.name
            #     error_info['missing_display_name'] = error_parameter.display_name
            #     error_info['missing_possible_stream_names'] = [s.name for s in
            #                                                    error_parameter.streams]
            #     error_info['missing_possible_stream_ids'] = [s.id for s in error_parameter.streams]

            # error_info['message'] = e.message
            self.provenance_metadata.calculated_metatdata.errors.append(error_info)

            log.debug('<%s> Unable to create derived product: %r missing: %r',
                      self.request_id, param.name, set(function_map).difference(kwargs))

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

        log.debug('<%s> _interpolate: %r %r %r', self.request_id,
                  source_key.as_refdes(), target_key.as_refdes(), parameter)

        source_dataset = self.datasets[source_key]
        target_dataset = self.datasets[target_key]
        name = parameter.name
        if name in source_dataset:
            # TODO determine if this is the desired approach for naming
            new_name = '-'.join((source_key.stream.name, name))
            if new_name not in target_dataset:
                interp_values = interp1d_DataArray(source_dataset.time.values,
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
        funcmap = stream_key.stream.create_function_map(param, set(streams).difference({stream_key}))
        for name in funcmap:
            source, value = funcmap[name]
            if source not in ['CAL', stream_key.stream]:
                source_key = streams.get(source)
                self._interpolate(source_key, stream_key, value)

    @log_timing(log)
    def calculate_derived_products(self):
        """
        Calculate all derived products for this StreamRequest
        :return:
        """
        log.debug('<%s> calculate_derived_products', self.request_id)
        for sk in self.datasets:
            datasets = []
            l1_params = [param for param in sk.stream.derived if param.is_l1]
            l2_params = [param for param in sk.stream.derived if param.is_l2 and not sk.stream.needs_external([param])]

            for deployment, dataset in self.datasets[sk].groupby('deployment'):
                # calculate L1
                log.debug('<%s> executing L1 algorithms for %r deployment %d',
                          self.request_id, sk.as_refdes(), deployment)
                for param in l1_params:
                    self._create_derived_product(dataset, sk, param, deployment)

                # calculate internal-only L2
                log.debug('<%s> executing internal L2 algorithms for %r deployment %d',
                          self.request_id, sk.as_refdes(), deployment)
                for param in l2_params:
                    self._create_derived_product(dataset, sk, param, deployment)

                datasets.append(dataset)
            self.datasets[sk] = xray.concat(datasets, 'index')

        # calculate external L2
        for sk in self.datasets:
            # create a list of external L2 parameters
            l2_external = [param for param in sk.stream.derived if param.is_l2 and sk.stream.needs_external([param])]

            if l2_external:
                log.debug('<%s> interpolating external data for L2 algorithms for %r',
                          self.request_id, sk.as_refdes())
                for param in l2_external:
                    self._import_interpolated(sk, param)

                datasets = []
                for deployment, dataset in self.datasets[sk].groupby('deployment'):
                    log.debug('<%s> executing external L2 algorithms for %r deployment %d',
                              self.request_id, sk.as_refdes(), deployment)
                    for param in l2_external:
                        self._create_derived_product(dataset, sk, param, deployment)
                    datasets.append(dataset)
                self.datasets[sk] = xray.concat(datasets, 'index')

        # import any other required "externals" into the primary dataset
        for sk in self.external_includes:
            for param in self.external_includes[sk]:
                self._interpolate(sk, self.stream_key, param)

        # execute any QC
        for sk, dataset in self.datasets.iteritems():
            for param in sk.stream.parameters:
                self.qc_executor.qc_check(param, dataset)

    @log_timing(log)
    def _locate_externals(self, parameters):
        """
        Locate external data sources for the given list of parameters
        :param parameters: list of type Parameter
        :return: found parameters as dict(StreamKey, Parameter), unfulfilled parameters as set(Parameter)
        """
        external_to_process = set(parameters)
        external_unfulfilled = set()
        stream_parameters = {}

        while external_to_process:
            stream, poss_params = external_to_process.pop()
            # 3 possible cases here.
            # 1) Stream, 1 Parameter
            # 2) None, 1 Parameter
            # 3) None, N Parameters
            if len(poss_params) == 1:
                p = poss_params[0]
                streams = p.streams
                if stream is not None:
                    streams = [stream]

                sk = self.find_stream(self.stream_key, streams)
                if sk:
                    stream_parameters.setdefault(sk, set()).update(sk.stream.needs_internal([p]))
                else:
                    external_unfulfilled.add(p)

            else:
                sk = None
                for p in poss_params:
                    if sk:
                        break
                    for depth in range(3):
                        sk = self.find_stream(self.stream_key, p.streams, depth=depth)
                        if sk:
                            stream_parameters.setdefault(sk, set()).update(sk.stream.needs_internal([p]))
                            break
                if sk is None:
                    external_unfulfilled.update(poss_params)

        return stream_parameters, external_unfulfilled

    @log_timing(log)
    def _get_mobile_externals(self):
        """
        For mobile assets, build the set of externals necessary to provide location data
        :return: set((Stream, (Parameter,)))
        """
        external_to_process = set()
        if self.stream_key.is_mobile:
            dpi = 'PRESWAT_L1'
            external_to_process.add((None, tuple(Parameter.query.filter(
                    Parameter.data_product_identifier == dpi).all())))

        if self.stream_key.is_glider:
            gps_stream = Stream.query.get(761)
            external_to_process.add((gps_stream, (Parameter.query.get(1335),)))
            external_to_process.add((gps_stream, (Parameter.query.get(1336),)))
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
        self.stream_parameters[self.stream_key] = self.stream_key.stream.needs_internal(internal_requested)

        # Identify external parameters needed to support this query
        external_to_process = self.stream_key.stream.needs_external(internal_requested)
        if external_to_process:
            stream_parameters, external_unfulfilled = self._locate_externals(external_to_process)
            self.stream_parameters.update(stream_parameters)
            self.unfulfilled = external_unfulfilled

        # Now identify any parameters needed for mobile assets
        external_to_process = self._get_mobile_externals()
        if external_to_process:
            stream_parameters, external_unfulfilled = self._locate_externals(external_to_process)
            self.stream_parameters.update(stream_parameters)
            self.unfulfilled = self.unfulfilled.union(external_unfulfilled)
            self.external_includes.update(stream_parameters)

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

    @staticmethod
    @log_timing(log)
    def find_stream(stream_key, streams, depth=None):
        """
        Given a primary source, attempt to find one of the supplied streams from the same instrument,
        same node or same subsite
        :param stream_key: StreamKey - defines the source of the primary stream
        :param streams: List - list of target streams
        :return: StreamKey if found, otherwise None
        """
        if not isinstance(streams, (list, tuple)):
            streams = [streams]

        stream_dictionary = build_stream_dictionary()
        method = stream_key.method
        subsite = stream_key.subsite
        node = stream_key.node
        sensor = stream_key.sensor

        if depth is None or depth == 0:
            # Search the same reference designator
            for stream in streams:
                sensors = stream_dictionary.get(stream.name, {}).get(method, {}).get(subsite, {}).get(node, [])
                if sensor in sensors:
                    return StreamKey.from_dict({
                        "subsite": subsite,
                        "node": node,
                        "sensor": sensor,
                        "method": stream_key.method,
                        "stream": stream.name
                    })

        if depth is None or depth == 1:
            # No streams from our target set exist on the same instrument. Check the same node
            for stream in streams:
                sensors = stream_dictionary.get(stream.name, {}).get(method, {}).get(subsite, {}).get(node, [])
                if sensors:
                    return StreamKey.from_dict({
                        "subsite": subsite,
                        "node": node,
                        "sensor": sensors[0],
                        "method": stream_key.method,
                        "stream": stream.name
                    })

        # GLIDER should never descend below this point, all streams MUST come from the same glider
        if stream_key.is_glider:
            return

        if depth is None or depth == 2:
            # No streams from our target set exist on the same node. Check the same subsite
            for stream in streams:
                subsite_dict = stream_dictionary.get(stream.name, {}).get(method, {}).get(subsite, {})
                for node in subsite_dict:
                    if subsite_dict[node]:
                        return StreamKey.from_dict({
                            "subsite": subsite,
                            "node": node,
                            "sensor": subsite_dict[node][0],
                            "method": stream_key.method,
                            "stream": stream.name
                        })


@log_timing(log)
def get_dataset(key, time_range, limit, provenance_metadata, pad_forward, deployments):
    cass_locations, san_locations, messages = get_location_metadata(key, time_range)
    provenance_metadata.add_messages(messages)
    # check for no data
    datasets = []
    if cass_locations.total + san_locations.total != 0:
        san_percent = san_locations.total / float(san_locations.total + cass_locations.total)
        cass_percent = cass_locations.total / float(san_locations.total + cass_locations.total)
    else:
        san_percent = 0
        cass_percent = 0

    if pad_forward:
        # pad forward on some datasets
        datasets.append(get_lookback_dataset(key, time_range, provenance_metadata, deployments))

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
                                           location_metadata=cass_locations))
        else:
            datasets.append(get_full_cass_dataset(key, cass_times, location_metadata=cass_locations))
    return compile_datasets(datasets)


@log_timing(log)
def get_lookback_dataset(key, time_range, provenance_metadata, deployments):
    first_metadata = get_first_before_metadata(key, time_range.start)
    if CASS_LOCATION_NAME in first_metadata:
        locations = first_metadata[CASS_LOCATION_NAME]
        return get_cass_lookback_dataset(key, time_range.start, locations.bin_list[0], deployments)
    elif SAN_LOCATION_NAME in first_metadata:
        locations = first_metadata[SAN_LOCATION_NAME]
        return get_san_lookback_dataset(key, TimeRange(locations.start_time, time_range.start), locations.bin_list[0],
                                        deployments)
    else:
        return None
