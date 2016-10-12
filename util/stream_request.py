import logging

import util.annotation
import util.provenance_metadata_store
from engine import app
from preload_database.model.preload import Parameter, Stream, NominalDepth
from util.asset_management import AssetManagement
from util.cass import fetch_l0_provenance
from util.common import log_timing, StreamEngineException, StreamKey, MissingDataException
from util.metadata_service import build_stream_dictionary, get_available_time_range
from util.qc_executor import QcExecutor
from util.stream_dataset import StreamDataset

log = logging.getLogger()

PRESSURE_DPI = app.config.get('PRESSURE_DPI')
GPS_STREAM_ID = app.config.get('GPS_STREAM_ID')
LATITUDE_PARAM_ID = app.config.get('LATITUDE_PARAM_ID')
LONGITUDE_PARAM_ID = app.config.get('LONGITUDE_PARAM_ID')
INT_PRESSURE_NAME = app.config.get('INT_PRESSURE_NAME')
MAX_DEPTH_VARIANCE = app.config.get('MAX_DEPTH_VARIANCE')
ASSET_HOST = app.config.get('ASSET_HOST')


class StreamRequest(object):
    """
    Stores the information from a request, and calculates the required
    parameters and their streams
    """

    def __init__(self, stream_key, parameters, time_range, uflags, qc_parameters=None,
                 limit=None, include_provenance=False, include_annotations=False, strict_range=False,
                 request_id='', collapse_times=False, external_includes=None):

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

        # Internals
        self.asset_management = AssetManagement(ASSET_HOST, request_id=self.request_id)
        self.stream_parameters = {}
        self.unfulfilled = set()
        self.datasets = {}
        self.external_includes = {} if external_includes is None else external_includes

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
                self.datasets[stream_key] = sd

        # Fetch annotations
        self._insert_annotations()
        self._exclude_flagged_data()

    def calculate_derived_products(self):
        # Calculate all internal-only data products
        for sk in self.datasets:
            if not sk.is_virtual:
                self.datasets[sk].calculate_internal()

        # Allow each StreamDataset to interpolate any needed parameters from the other datasets
        # Then calculate any data products which required external input.
        for sk in self.datasets:
            if not sk.is_virtual:
                self.datasets[sk].interpolate_needed(self.datasets)
                self.datasets[sk].calculate_external()

        for sk in self.datasets:
            if sk.is_virtual:
                for poss_source in self.datasets:
                    if poss_source.stream in sk.stream.source_streams:
                        self.datasets[sk].calculate_virtual(self.datasets[poss_source])
                        break

        self._run_qc()
        self._insert_provenance()
        self._add_location()

    @log_timing(log)
    def _run_qc(self):
        # execute any QC
        for sk, stream_dataset in self.datasets.iteritems():
            for param in sk.stream.parameters:
                for dataset in stream_dataset.datasets.itervalues():
                    self.qc_executor.qc_check(param, dataset)

    # noinspection PyTypeChecker
    def _insert_provenance(self):
        """
        Insert all source provenance for this request. This is dependent on the data already having been fetched.
        :return:
        """
        if self.include_provenance:
            for stream_key in self.stream_parameters:
                if stream_key in self.datasets:
                    for deployment, dataset in self.datasets[stream_key].datasets.iteritems():
                        prov_metadata = self.datasets[stream_key].provenance_metadata
                        prov_metadata.add_query_metadata(self, self.request_id, 'JSON')
                        prov_metadata.add_instrument_provenance(stream_key, self.datasets[stream_key].events.events)
                        if 'provenance' in dataset:
                            provenance = dataset.provenance.values.astype('str')
                            prov = fetch_l0_provenance(stream_key, provenance, deployment)
                            prov_metadata.update_provenance(prov)

    def _insert_annotations(self):
        """
        Insert all annotations for this request. This is dependent on the data already having been fetched.
        :return:
        """
        for stream_key, stream_dataset in self.datasets.iteritems():
            stream_dataset.annotation_store.query_annotations(stream_key, self.time_range)

    def _exclude_flagged_data(self):
        """
        Exclude data from datasets based on annotations
        TODO: Future optimization, avoid querying excluded data when possible
        :return:
        """
        for stream_key, stream_dataset in self.datasets.iteritems():
            stream_dataset.exclude_flagged_data()

    def import_extra_externals(self):
        # import any other required "externals" into all datasets
        for source_sk in self.external_includes:
            if source_sk in self.datasets:
                for param in self.external_includes[source_sk]:
                    for target_sk in self.datasets:
                        self.datasets[target_sk].interpolate_into(source_sk, self.datasets[source_sk], param)

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
                nearby = nominal_depth.get_depth_within(MAX_DEPTH_VARIANCE)
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
