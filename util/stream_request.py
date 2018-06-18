import logging
import math

import util.annotation
import util.metadata_service
import util.provenance_metadata_store
from util.annotation import AnnotationStore
from engine import app
from ooi_data.postgres.model import NominalDepth, Parameter, ParameterFunction, Stream
from util.asset_management import AssetManagement
from util.cass import fetch_l0_provenance
from util.common import log_timing, StreamEngineException, StreamKey, MissingDataException, read_size_config
from util.metadata_service import build_stream_dictionary, get_available_time_range, get_streams_for_refdes, \
    get_refdes_streams_for_subsite_node
from util.qc_executor import QcExecutor
from util.stream_dataset import StreamDataset
from numbers import Number

log = logging.getLogger()

CC = 'CC'
DATA_QC_PROPAGATE_FLAGS = 'dataqc_propagateflags'
DPI = 'dpi_'
NEARBY = 'N'
PD = 'PD'
QC_EXECUTED = '_qc_executed'
QC_RESULTS = '_qc_results'
RELATED = 'R'
# these tests were deemed not ready for propagation by the Data Team
QC_TEST_FAIL_SKIP_PROPAGATE = ('dataqc_gradienttest', 'dataqc_localrangetest', 'dataqc_polytrendtest')

PRESSURE_DPI = app.config.get('PRESSURE_DPI')
GPS_STREAM_ID = app.config.get('GPS_STREAM_ID')
LATITUDE_PARAM_ID = app.config.get('LATITUDE_PARAM_ID')
LONGITUDE_PARAM_ID = app.config.get('LONGITUDE_PARAM_ID')
INT_PRESSURE_NAME = app.config.get('INT_PRESSURE_NAME')
MAX_DEPTH_VARIANCE = app.config.get('MAX_DEPTH_VARIANCE')
MAX_DEPTH_VARIANCE_METBK = app.config.get('MAX_DEPTH_VARIANCE_METBK')
ASSET_HOST = app.config.get('ASSET_HOST')
SIZE_ESTIMATES = read_size_config(app.config.get('SIZE_CONFIG'))
DEFAULT_PARTICLE_DENSITY = app.config.get('PARTICLE_DENSITY', 1000)  # default bytes/particle estimate
SECONDS_PER_BYTE = app.config.get('SECONDS_PER_BYTE', 0.0000041)  # default bytes/sec estimate
MINIMUM_REPORTED_TIME = app.config.get('MINIMUM_REPORTED_TIME')


class StreamRequest(object):
    """
    Stores the information from a request, and calculates the required
    parameters and their streams
    """

    def __init__(self, stream_key, parameters, time_range, uflags, qc_parameters=None,
                 limit=None, include_provenance=False, include_annotations=False, strict_range=False,
                 request_id='', collapse_times=False, execute_dpa=True, require_deployment=True):

        if not isinstance(stream_key, StreamKey):
            raise StreamEngineException('Received no stream key', status_code=400)

        # Inputs
        self.request_id = request_id
        self.stream_key = stream_key
        self.requested_parameters = parameters
        self.time_range = time_range
        self.uflags = uflags
        self.qc_executor = QcExecutor(qc_parameters, self)
        self.limit = limit
        self.include_provenance = include_provenance
        self.include_annotations = include_annotations
        self.strict_range = strict_range
        self.execute_dpa = execute_dpa
        self.require_deployment = require_deployment

        # Internals
        self.asset_management = AssetManagement(ASSET_HOST, request_id=self.request_id)
        self.stream_parameters = {}
        self.unfulfilled = set()
        self.datasets = {}
        self.external_includes = {}
        self.annotation_store = AnnotationStore()
        self.qc_affects = {}
        self.qc_propagate_flag = None
        self.qc_test_flag_map = None

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
        # Start fetching calibration data from Asset Management
        am_events = {}
        am_futures = {}
        for stream_key in self.stream_parameters:
            refdes = '-'.join((stream_key.subsite, stream_key.node, stream_key.sensor))
            am_futures[stream_key] = self.asset_management.get_events_async(refdes)

        # Resolve calibration data futures and attach to instrument data
        for stream_key in am_futures:
            events = am_futures[stream_key].result()
            am_events[stream_key] = events

        # Start fetching instrument data
        for stream_key, stream_parameters in self.stream_parameters.iteritems():
            other_streams = set(self.stream_parameters)
            other_streams.remove(stream_key)
            should_pad = stream_key != self.stream_key
            if not stream_key.is_virtual:
                log.debug('<%s> Fetching raw data for %s', self.request_id, stream_key.as_refdes())
                sd = StreamDataset(stream_key, self.uflags, other_streams, self.request_id)
                sd.events = am_events[stream_key]
                try:
                    sd.fetch_raw_data(self.time_range, self.limit, should_pad)
                    self.datasets[stream_key] = sd
                except MissingDataException as e:
                    if stream_key == self.stream_key:
                        raise MissingDataException("Query returned no results for primary stream")
                    elif stream_key.stream in self.stream_key.stream.source_streams:
                        raise MissingDataException("Query returned no results for source stream")
                    else:
                        log.error('<%s> %s', self.request_id, e.message)

            else:
                log.debug('<%s> Creating empty dataset for virtual stream: %s',
                          self.request_id, stream_key.as_refdes())
                sd = StreamDataset(stream_key, self.uflags, other_streams, self.request_id)
                sd.events = am_events[stream_key]
                self.datasets[stream_key] = sd

        self._exclude_flagged_data()
        self._exclude_nondeployed_data()

        # Verify data still exists after masking virtual
        message = 'Query returned no results for %s stream (due to deployment or annotation mask)'
        if self.stream_key.is_virtual:
            found_streams = [stream.stream for stream in self.datasets
                             if self.datasets[stream]]
            if not any(stream in self.stream_key.stream.source_streams for stream in found_streams):
                raise MissingDataException(message % 'source')
        # real
        else:
            primary_stream_dataset = self.datasets[self.stream_key]
            if not primary_stream_dataset.datasets:
                raise MissingDataException(message % 'primary')

        # Remove any empty, non-virtual supporting datasets
        for stream_key in list(self.datasets):
            if not stream_key.is_virtual:
                if not self.datasets[stream_key].datasets:
                    del self.datasets[stream_key]

    def calculate_derived_products(self):
        # Calculate all internal-only data products
        for sk in self.datasets:
            if not sk.is_virtual:
                self.datasets[sk].calculate_all()

        # Allow each StreamDataset to interpolate any needed non-virtual parameters from the other datasets
        # Then calculate any data products which required only non-virtual external input.
        for sk in self.datasets:
            if not sk.is_virtual:
                self.datasets[sk].interpolate_needed(self.datasets, interpolate_virtual=False)
                self.datasets[sk].calculate_all()

        for sk in self.datasets:
            if sk.is_virtual:
                for poss_source in self.datasets:
                    if poss_source.stream in sk.stream.source_streams:
                        self.datasets[sk].calculate_virtual(self.datasets[poss_source])
                        break
        
        # Allow each StreamDataset to interpolate any needed virtual parameters from the other datasets
        # Then calculate any data products which required virtual external input.
        for sk in self.datasets:
            if not sk.is_virtual:
                self.datasets[sk].interpolate_needed(self.datasets, interpolate_virtual=True)
                self.datasets[sk].calculate_all()

        for sk in self.datasets:
            self.datasets[sk].fill_missing()

    def execute_qc(self):
        self._run_qc()

    def insert_provenance(self):
        self._insert_provenance()
        self._add_location()

    @log_timing(log)
    def _run_qc(self):
        propagate_failures = {}
        # execute any QC
        for sk, stream_dataset in self.datasets.iteritems():
            sk_related_refdes_streams = {}
            sk_related_stream_refdes = {}
            sk_nearby_refdes_streams = {}
            for param in sk.stream.parameters:
                param_qc_executed = param.name + QC_EXECUTED
                param_qc_results = param.name + QC_RESULTS
                for dataset in stream_dataset.datasets.itervalues():
                    self.qc_executor.qc_check(param, dataset)
                    # check for QC test failure on 1+ data particles for the parameter
                    if (param_qc_executed in dataset.keys()) and (param_qc_results in dataset.keys()) and \
                            (dataset[param_qc_executed].values.min() > dataset[param_qc_results].values.min()):
                        qc_executed_values = dataset[param_qc_executed].values
                        qc_results_values = dataset[param_qc_results].values
                        # list of tuples with the position where a QC variance exists and the variance
                        qc_variances = [(position, value[0]-value[1])
                                        for position, value in enumerate(zip(qc_executed_values, qc_results_values))
                                        if value[0] != value[1]]
                        # populate these collections as needed, once per sk
                        if not sk_related_refdes_streams:
                            # capture related (by subsite, node) reference-designators and streams in separate
                            # maps, one where reference-designator is key and one where stream is key
                            for refdes, stream in get_refdes_streams_for_subsite_node(sk.subsite, sk.node):
                                sk_related_refdes_streams.setdefault(refdes, set()).add(stream)
                                sk_related_stream_refdes.setdefault(stream, set()).add(refdes)
                            if not sk.is_mobile:
                                # capture nearby (by subsite, depth nearness) reference-designators
                                # and streams if they are not already considered "related"
                                max_depth_var = self.derive_max_depth(sk.sensor)
                                nd = NominalDepth.get_nominal_depth(sk.subsite, sk.node, sk.sensor)
                                for nearby_nd in nd.get_depth_within(max_depth_var):
                                    if not sk_related_refdes_streams.get(nearby_nd.reference_designator):
                                        nearby_streams = set(get_streams_for_refdes(nearby_nd.reference_designator))
                                        if nearby_streams:
                                            sk_nearby_refdes_streams[nearby_nd.reference_designator] = nearby_streams

                        # affected_streams: map of streams (and their parameters) affected by param
                        affected_streams = self._get_affected_streams(param)

                        # put all the related streams (from the keys of sk_related_stream_refdes) into a set
                        # and match those streams to affected_streams and prepare to propagate QC test failure
                        sk_related_streams = set(sk_related_stream_refdes.keys())
                        for matched_stream in sk_related_streams.intersection(affected_streams):
                            # get the parameters from the matching affected_streams without param
                            new_matched_parameters = affected_streams.get(matched_stream, set()).difference({param})
                            if not new_matched_parameters:
                                continue
                            # get the corresponding refdes for use in failure propagation
                            for matched_refdes in sk_related_stream_refdes.get(matched_stream):
                                matching_propagation = propagate_failures.get(
                                    (RELATED, matched_refdes, matched_stream, sk.method), {}).get(param)
                                current_matched_parameters = matching_propagation[1] if matching_propagation else set()
                                matched_parameters = current_matched_parameters.union(new_matched_parameters)
                                if matched_parameters > current_matched_parameters:
                                    propagate_failures.setdefault(
                                        (RELATED, matched_refdes, matched_stream, sk.method), {})[param] = \
                                        (qc_variances, matched_parameters)

                        # match nearby streams to affected_streams and prepare to propagate QC test failure
                        for review_refdes, review_streams in sk_nearby_refdes_streams.items():
                            for matched_stream in review_streams.intersection(affected_streams):
                                new_matched_parameters = affected_streams.get(matched_stream, set()).\
                                    difference({param})
                                if not new_matched_parameters:
                                    continue
                                matching_propagation = propagate_failures.get(
                                    (NEARBY, review_refdes, matched_stream, sk.method), {}).get(param)
                                current_matched_parameters = matching_propagation[1] if matching_propagation else set()
                                matched_parameters = current_matched_parameters.union(new_matched_parameters)
                                if matched_parameters > current_matched_parameters:
                                    propagate_failures.setdefault(
                                        (NEARBY, review_refdes, matched_stream, sk.method), {})[param] = \
                                        (qc_variances, matched_parameters)

        if not propagate_failures:
            return

        # process all propagation failures on the tested streams and data sets
        for sk, stream_dataset in self.datasets.iteritems():
            stream_parameters = set(sk.stream.parameters)
            failed_nearby_params_map = propagate_failures.get((NEARBY, sk.as_three_part_refdes(), sk.stream_name,
                                                               sk.method), {})
            # make a set of the failed input parameters for the sk for matching stream_parameters
            failed_nearby_parameters = set(failed_nearby_params_map.keys())
            # only propagate failures on affected parameters whose upstream parameters are not inputs to this stream
            for matched_parameter in failed_nearby_parameters.difference(stream_parameters):
                propagate_input = failed_nearby_params_map[matched_parameter]
                qc_variances = propagate_input[0]
                affected_parameters = propagate_input[1]
                for propagate_affected in affected_parameters.intersection(stream_parameters):
                    param_qc_executed = propagate_affected.name + QC_EXECUTED
                    for dataset in stream_dataset.datasets.itervalues():
                        if param_qc_executed in dataset.keys():
                            qc_executed_values = dataset[param_qc_executed].values
                            for position, variance in qc_variances:
                                # propagate the failure on this particle if propagating tests failed
                                failed_tests_to_propagate = [(i, self.qc_test_flag_map[i][0])
                                                             for i in self.qc_test_flag_map.keys()
                                                             if i & variance and self.qc_test_flag_map[i][1]]
                                if failed_tests_to_propagate:
                                    qc_executed_values[position] = \
                                            qc_executed_values[position] | self.qc_propagate_flag

            failed_related_parameters = propagate_failures.get((RELATED, sk.as_three_part_refdes(), sk.stream_name,
                                                                sk.method), {})
            for propagate_input in failed_related_parameters.values():
                qc_variances = propagate_input[0]
                affected_parameters = propagate_input[1]
                for propagate_affected in affected_parameters.intersection(stream_parameters):
                    param_qc_executed = propagate_affected.name + QC_EXECUTED
                    for dataset in stream_dataset.datasets.itervalues():
                        if param_qc_executed in dataset.keys():
                            qc_executed_values = dataset[param_qc_executed].values
                            for position, variance in qc_variances:
                                # propagate the failure on this particle if propagating tests failed
                                failed_tests_to_propagate = [(i, self.qc_test_flag_map[i][0])
                                                             for i in self.qc_test_flag_map.keys()
                                                             if i & variance and self.qc_test_flag_map[i][1]]
                                if failed_tests_to_propagate:
                                    qc_executed_values[position] = \
                                            qc_executed_values[position] | self.qc_propagate_flag

    # noinspection PyTypeChecker
    def _insert_provenance(self):
        """
        Insert all source provenance for this request. This is dependent on the data already having been fetched.
        :return:
        """
        if self.include_provenance:
            for stream_key in self.stream_parameters:
                if stream_key in self.datasets:
                    self.datasets[stream_key].insert_instrument_attributes()
                    for deployment, dataset in self.datasets[stream_key].datasets.iteritems():
                        prov_metadata = self.datasets[stream_key].provenance_metadata
                        prov_metadata.add_query_metadata(self, self.request_id, 'JSON')
                        prov_metadata.add_instrument_provenance(stream_key, self.datasets[stream_key].events.events)
                        if 'provenance' in dataset:
                            provenance = dataset.provenance.values.astype('str')
                            prov = fetch_l0_provenance(stream_key, provenance, deployment)
                            prov_metadata.update_provenance(prov)

    def insert_annotations(self):
        """
        Insert all annotations for this request.
        """
        for stream_key in self.stream_parameters:
            self.annotation_store.add_query_annotations(stream_key, self.time_range)

    # builds a map of all streams that are mapped to any parameters that can have affects
    def _get_affected_streams(self, param):
        affected_params = {param}
        # visit parameters that can be affected by this one
        params_to_visit = self.qc_affects.get(param, [])

        while params_to_visit:
            visit_param = params_to_visit.pop()
            affected_params.add(visit_param)
            # capture and prepare to visit any parameters this one can affect
            for review_param in self.qc_affects.get(visit_param, []):
                if review_param in affected_params:
                    continue
                affected_params.add(review_param)
                params_to_visit.add(review_param)

        # build streams map keyed by stream tied to affected parameters
        streams = {}
        for affected_param in affected_params:
            for stream in affected_param.streams:
                streams.setdefault(stream.name, set()).add(affected_param)

        return streams

    def _exclude_flagged_data(self):
        """
        Exclude data from datasets based on annotations
        TODO: Future optimization, avoid querying excluded data when possible
        :return:
        """
        for stream_key, stream_dataset in self.datasets.iteritems():
            stream_dataset.exclude_flagged_data(self.annotation_store)

    def _exclude_nondeployed_data(self):
        """
        Exclude data from datasets that are outside of deployment dates
        :return:
        """
        for stream_key, stream_dataset in self.datasets.iteritems():
            stream_dataset.exclude_nondeployed_data(self.require_deployment)

    def import_extra_externals(self):
        # import any other required "externals" into all datasets
        for source_sk in self.external_includes:
            if source_sk in self.datasets:
                for param in self.external_includes[source_sk]:
                    for target_sk in self.datasets:
                        self.datasets[target_sk].interpolate_into(source_sk, self.datasets[source_sk], param)

        # determine if there is a pressure parameter available (9328)
        pressure_params = [(sk, param) for sk in self.external_includes for param in self.external_includes[sk]
                           if param.data_product_identifier == PRESSURE_DPI]

        if pressure_params:
            # if there is a pressure parameter, integrate it into the stream
            pressure_key, pressure_param = pressure_params.pop()
            pressure_name = '-'.join((pressure_key.stream.name, pressure_param.name))

            if pressure_key in self.datasets:
                self.datasets[self.stream_key].interpolate_into(pressure_key,
                                                                self.datasets.get(pressure_key),
                                                                pressure_param)

                # Add the appropriate pressure_value to each deployment
                for deployment in self.datasets[self.stream_key].datasets:
                    if pressure_name in self.datasets[self.stream_key].datasets[deployment].data_vars:
                        pressure_value = self.datasets[self.stream_key].datasets[deployment].data_vars[pressure_name]
                        del self.datasets[self.stream_key].datasets[deployment][pressure_name]
                        pressure_value.name = INT_PRESSURE_NAME
                        self.datasets[self.stream_key].datasets[deployment][INT_PRESSURE_NAME] = pressure_value

    def _add_location(self):
        log.debug('<%s> Inserting location data for all datasets', self.request_id)
        for stream_dataset in self.datasets.itervalues():
            stream_dataset.add_location()

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
            # all non-virtual streams define PD7, skip
            if poss_params[0].id == 7:
                continue
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

        # The decimal value for the dataqc_propagateflags entry in the parameter_function table
        self.qc_propagate_flag = [int(param_func.qc_flag, 2)
                                  for param_func in ParameterFunction.query.filter_by(function_type_id=3).all()
                                  if DATA_QC_PROPAGATE_FLAGS == param_func.name][0]

        # Map of decimal value for the 6 QC tests and a tuple of the name and a boolean
        # indicating the test is considered one on which test failures are propagated
        self.qc_test_flag_map = dict([(int(param_func.qc_flag, 2),
                                       (param_func.name, param_func.name not in QC_TEST_FAIL_SKIP_PROPAGATE))
                                      for param_func in ParameterFunction.query.filter_by(function_type_id=3).all()
                                      if DATA_QC_PROPAGATE_FLAGS != param_func.name])

        # map key=Parameter.data_product_identifier,value=set of corresponding Parameters
        # build the map from each Parameter with a populated data_product_identifier
        dpi = {}
        for p in Parameter.query:
            if p.data_product_identifier:
                dpi.setdefault(p.data_product_identifier, set()).add(p)

        # review each Parameter having a populated parameter_function_map
        # qc_affects key=the referenced Parameter (via the "dpi_" or "PD" value)
        #          value=the Parameters being reviewed
        for p in Parameter.query:
            if p.is_function:
                for values in p.parameter_function_map.values():
                    # force "values" of a single value to be a list
                    if not isinstance(values, list):
                        values = [values]
                    for value in values:
                        # values that are numbers or calibration coefficients affect no other parameters
                        if isinstance(value, Number) or value.startswith(CC):
                            continue
                        # DPI values reference parameters that are affected
                        # the key is each Parameter found in the dpi map via the "dpi_" value
                        if value.startswith(DPI):
                            dpi_value = value.split(DPI)[-1]
                            for param in dpi.get(dpi_value, []):
                                self.qc_affects.setdefault(param, set()).add(p)
                        # PD numbers reference parameters that are affected
                        # the key is the Parameter found after the "PD" literal
                        elif PD in value:
                            param = Parameter.query.get(value.split(PD)[-1])
                            self.qc_affects.setdefault(param, set()).add(p)

        if self.execute_dpa:
            # Identify external parameters needed to support this query
            external_to_process = self.stream_key.stream.needs_external(internal_requested)
            log.debug('<%s> primary stream external needs: %r', self.request_id, external_to_process)
            if external_to_process:
                stream_parameters, found, external_unfulfilled = self._locate_externals(external_to_process)
                for sk in stream_parameters:
                    self.stream_parameters.setdefault(sk, set()).update(stream_parameters[sk])
                self.unfulfilled = external_unfulfilled
                for sk in found:
                    self.external_includes.setdefault(sk, set()).update(found[sk])

            # Now identify any parameters needed for mobile assets
            external_to_process = self._get_mobile_externals()
            if external_to_process:
                stream_parameters, found, external_unfulfilled = self._locate_externals(external_to_process)
                for sk in stream_parameters:
                    self.stream_parameters.setdefault(sk, set()).update(stream_parameters[sk])
                self.unfulfilled = self.unfulfilled.union(external_unfulfilled)
                for sk in found:
                    self.external_includes.setdefault(sk, set()).update(found[sk])

            if self.unfulfilled:
                log.warn('<%s> Unable to find sources for the following params: %r',
                         self.request_id, self.unfulfilled)

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
            if new_time_range != self.time_range:
                log.info('<%s> Collapsing requested time range: %s to available time range: %s',
                         self.request_id, self.time_range, new_time_range)
                self.time_range = new_time_range

        else:
            # collapse to primary stream
            new_time_range = self.time_range.collapse(get_available_time_range(self.stream_key))
            if new_time_range != self.time_range:
                log.info('<%s> Collapsing requested time range: %s to available time range: %s',
                         self.request_id, self.time_range, new_time_range)
                self.time_range = new_time_range

    @log_timing(log)
    def find_stream(self, stream_key, poss_params, stream=None):
        log.debug('find_stream(%r, %r, %r)', stream_key, poss_params, stream)
        subsite = stream_key.subsite
        node = stream_key.node
        sensor = stream_key.sensor
        stream_dictionary = build_stream_dictionary()

        param_streams = []
        for p in poss_params:
            if stream is None:
                param_streams.append((p, [s.name for s in p.streams]))
            else:
                param_streams.append((p, [stream.name]))

        # First, try to find the stream on the same sensor
        for param, search_streams in param_streams:
            sk = self._find_stream_same_sensor(stream_key, search_streams, stream_dictionary)
            if sk:
                return sk, param

        # Attempt to find an instrument at the same depth (if not mobile)
        if not stream_key.is_mobile:
            nominal_depth = NominalDepth.get_nominal_depth(subsite, node, sensor)
            if nominal_depth is not None:
                co_located = nominal_depth.get_colocated_subsite()
                for param, search_streams in param_streams:
                    sk = self._find_stream_from_list(stream_key, search_streams, co_located, stream_dictionary)
                    if sk:
                        return sk, param

        # Attempt to find an instrument on the same node
        for param, search_streams in param_streams:
            sk = self._find_stream_same_node(stream_key, search_streams, stream_dictionary)
            if sk:
                return sk, param

        # Not found at same depth, attempt to find nearby (if not mobile)
        if not stream_key.is_mobile:
            nominal_depth = NominalDepth.get_nominal_depth(subsite, node, sensor)
            if nominal_depth is not None:
                max_depth_var = self.derive_max_depth(sensor)
                nearby = nominal_depth.get_depth_within(max_depth_var)
                for param, search_streams in param_streams:
                    sk = self._find_stream_from_list(stream_key, search_streams, nearby, stream_dictionary)
                    if sk:
                        return sk, param

        return None, None

    @staticmethod
    def _find_stream_same_sensor(stream_key, streams, stream_dictionary):
        """
        Given a primary source, attempt to find one of the supplied streams from the same instrument
        :param stream_key:
        :param streams:
        :return:
        """
        log.debug('_find_stream_same_sensor(%r, %r, STREAM_DICTIONARY)', stream_key, streams)
        method = stream_key.method
        subsite = stream_key.subsite
        node = stream_key.node
        sensor = stream_key.sensor

        # Search the same reference designator
        for stream in streams:
            sensors = stream_dictionary.get(stream, {}).get(method, {}).get(subsite, {}).get(node, [])
            if sensor in sensors:
                return StreamKey.from_dict({
                    "subsite": subsite,
                    "node": node,
                    "sensor": sensor,
                    "method": method,
                    "stream": stream
                })

    @staticmethod
    def _find_stream_from_list(stream_key, streams, sensors, stream_dictionary):
        log.debug('_find_stream_from_list(%r, %r, %r, STREAM_DICTIONARY)', stream_key, streams, sensors)
        method = stream_key.method
        subsite = stream_key.subsite
        designators = [(c.subsite, c.node, c.sensor) for c in sensors]

        for stream in streams:
            subsite_dict = stream_dictionary.get(stream, {}).get(method, {}).get(subsite, {})
            for _node in subsite_dict:
                for _sensor in subsite_dict[_node]:
                    des = (subsite, _node, _sensor)
                    if des in designators:
                        return StreamKey.from_dict({
                            "subsite": subsite,
                            "node": _node,
                            "sensor": _sensor,
                            "method": method,
                            "stream": stream
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
        log.debug('_find_stream_same_node(%r, %r, STREAM_DICTIONARY)', stream_key, streams)
        method = stream_key.method
        subsite = stream_key.subsite
        node = stream_key.node

        for stream in streams:
            sensors = stream_dictionary.get(stream, {}).get(method, {}).get(subsite, {}).get(node, [])
            if sensors:
                return StreamKey.from_dict({
                    "subsite": subsite,
                    "node": node,
                    "sensor": sensors[0],
                    "method": method,
                    "stream": stream
                })

    def interpolate_from_stream_request(self, stream_request):
        source_sk = stream_request.stream_key
        target_sk = self.stream_key
        if source_sk in stream_request.datasets and target_sk in self.datasets:
            for param in stream_request.requested_parameters:
                self.datasets[target_sk].interpolate_into(source_sk, stream_request.datasets[source_sk], param)
                self.external_includes.setdefault(source_sk, set()).add(param)

    def compute_request_size(self, size_estimates=SIZE_ESTIMATES):
        """
        Estimate the time and size of a NetCDF request based on previous data.
        :param size_estimates:  dictionary containing size estimates for each stream
        :return:  size estimate (in bytes) - also populates self.size_estimate
        """
        default_size = DEFAULT_PARTICLE_DENSITY  # bytes / particle
        size_estimate = sum((size_estimates.get(stream.stream_name, default_size) *
                             util.metadata_service.get_particle_count(stream, self.time_range)
                             for stream in self.stream_parameters))

        return int(math.ceil(size_estimate))

    @staticmethod
    def compute_request_time(file_size):
        return max(MINIMUM_REPORTED_TIME, file_size * SECONDS_PER_BYTE)

    @staticmethod
    def derive_max_depth(sensor):
        return MAX_DEPTH_VARIANCE_METBK if 'METBK' in sensor else MAX_DEPTH_VARIANCE
