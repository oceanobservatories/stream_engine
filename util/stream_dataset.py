import importlib
import json
import logging

import ion_functions
import numexpr
import numpy as np

from preload_database.model.preload import Parameter, Stream
from util.advlogging import ParameterReport
from util.annotation import AnnotationStore
from util.cass import fetch_nth_data, get_full_cass_dataset, get_cass_lookback_dataset
from util.common import (log_timing, ntp_to_datestring, ntp_to_datetime, UnknownFunctionTypeException,
                         StreamEngineException, TimeRange, MissingDataException)
from util.datamodel import create_empty_dataset, compile_datasets, add_location_data, _get_fill_value
from util.metadata_service import (SAN_LOCATION_NAME, CASS_LOCATION_NAME, get_first_before_metadata,
                                   get_location_metadata)

from util.provenance_metadata_store import ProvenanceMetadataStore
from util.san import fetch_nsan_data, fetch_full_san_data, get_san_lookback_dataset
from util.xray_interpolation import interp1d_data_array
from engine import app

log = logging.getLogger(__name__)

ION_VERSION = getattr(ion_functions, '__version__', 'unversioned')
INSTRUMENT_ATTRIBUTE_MAP = app.config.get('INSTRUMENT_ATTRIBUTE_MAP')


class StreamDataset(object):
    def __init__(self, stream_key, uflags, external_streams, request_id):
        self.stream_key = stream_key
        self.provenance_metadata = ProvenanceMetadataStore(request_id)
        self.annotation_store = AnnotationStore()
        self.uflags = uflags
        self.external_streams = external_streams
        self.request_id = request_id
        self.datasets = {}
        self.events = None

        self.internal_only = [p for p in stream_key.stream.derived if not stream_key.stream.needs_external([p])]
        self.external = [p for p in stream_key.stream.derived if stream_key.stream.needs_external([p])]
        self.l1_params = [p for p in self.internal_only if p.is_l1]
        self.l2_params = [p for p in self.internal_only if p.is_l2]
        self.external_l1 = [p for p in self.external if p.is_l1]
        self.external_l2 = [p for p in self.external if p.is_l2]

        if self.stream_key.is_virtual:
            self.time_param = Parameter.query.get(self.stream_key.stream.time_parameter)
        else:
            self.time_param = None

    def fetch_raw_data(self, time_range, limit, should_pad):
        dataset = self.get_dataset(time_range, limit, self.provenance_metadata,
                                   should_pad, [], self.request_id)
        self._insert_dataset(dataset)

    def _insert_dataset(self, dataset):
        """
        Insert the supplied dataset into this StreamDataset
        This method should not be called twice, it will replace existing data if called again.
        """
        if dataset:
            # RSN data shall obtain deployment information from asset management.
            # Replace these values prior to grouping with the actual deployment number
            if self.events and self.stream_key.method.startswith('streamed'):
                for deployment_number in sorted(self.events.deps):
                    mask = dataset.time.values > self.events.deps[deployment_number].ntp_start
                    dataset.deployment.values[mask] = deployment_number

            for deployment, group in dataset.groupby('deployment'):
                self.datasets[deployment] = group
        else:
            raise MissingDataException("Query returned no results for stream %s" % self.stream_key)

    def insert_instrument_attributes(self):
        """
        Add applicable instrument attributes to the dataset attributes.
        """
        for deployment in self.datasets:
            ds = self.datasets[deployment]
            if self.events is not None and deployment in self.events.deps:
                events = self.events.deps[deployment]
                sensor = events._get_sensor()
                for attribute in INSTRUMENT_ATTRIBUTE_MAP:
                    value = sensor.get(attribute)
                    if isinstance(value, bool):
                        value = str(value)
                    elif isinstance(value, (list, dict)):
                        value = json.dumps(value)
                    elif value is None:
                        value = 'Not specified.'
                    ds.attrs[INSTRUMENT_ATTRIBUTE_MAP[attribute]] = value

    def calculate_internal(self):
        if not self.time_param:
            # Calculate internal L1 and L2 parameters
            for deployment, dataset in self.datasets.iteritems():
                self._calculate_parameter_list(dataset, self.stream_key, self.l1_params, deployment, 'internal L1')
                self._calculate_parameter_list(dataset, self.stream_key, self.l2_params, deployment, 'internal L2')

    def interpolate_needed(self, external_datasets):
        if not self.time_param:
            for param in self.external:
                self._interpolate_and_import_needed(param, external_datasets)

    def calculate_external(self):
        if not self.time_param:
            for deployment, dataset in self.datasets.iteritems():
                self._calculate_parameter_list(dataset, self.stream_key, self.external_l1, deployment, 'external L1')
                self._calculate_parameter_list(dataset, self.stream_key, self.external_l2, deployment, 'external L2')

    def add_location(self):
        log.debug('<%s> Inserting location data for %s datasets',
                  self.request_id, self.stream_key.as_three_part_refdes())
        if not self.stream_key.is_glider:
            for deployment in self.datasets:
                lat, lon, depth = self.events.get_location_data(deployment)
                add_location_data(self.datasets[deployment], lat, lon)

    @log_timing(log)
    def calculate_virtual(self, source_stream_dataset):
        # Calculate virtual streams
        log.info('<%s> Compute virtual stream', self.request_id)

        if self.time_param:
            for deployment, source_dataset in source_stream_dataset.datasets.iteritems():
                dataset = create_empty_dataset(self.stream_key, self.request_id)
                self.datasets[deployment] = dataset
                # compute the time parameter
                self._create_derived_product(dataset, self.stream_key, self.time_param, deployment,
                                             source_dataset=source_dataset)
                dataset['time'] = dataset[self.time_param.name].copy()
                deployments = np.empty_like(dataset.time.values, dtype='int32')
                deployments[:] = deployment
                dataset['deployment'] = ('obs', deployments, {'name': 'deployment'})
                for param in self.stream_key.stream.parameters:
                    if param != self.time_param:
                        self._create_derived_product(dataset, self.stream_key, param, deployment,
                                                     source_dataset=source_dataset)

    def _mask_datasets(self, masks):
        deployments = list(self.datasets)
        for deployment in deployments:
            mask = masks.get(deployment)
            if mask is None or mask.all():
                continue
            if mask.any():
                size = np.count_nonzero(np.logical_not(mask))
                log.info('<%s> Masking %d datapoints from %s deployment %d',
                         self.request_id, size, self.stream_key, deployment)
                self.datasets[deployment] = self.datasets[deployment].isel(obs=mask)
            else:
                log.info('<%s> Masking ALL datapoints from %s deployment %d',
                         self.request_id, self.stream_key, deployment)
                del self.datasets[deployment]

    def exclude_flagged_data(self):
        masks = {}
        if self.annotation_store.has_exclusion():
            for deployment in self.datasets:
                dataset = self.datasets[deployment]
                mask = self.annotation_store.get_exclusion_mask(dataset.time.values)
                masks[deployment] = mask

            self._mask_datasets(masks)

    def exclude_nondeployed_data(self):
        masks = {}
        if self.events is not None:
            for deployment in self.datasets:
                dataset = self.datasets[deployment]
                if deployment in self.events.deps:
                    deployment_event = self.events.deps[deployment]
                    mask = (dataset.time.values >= deployment_event.ntp_start) & \
                           (dataset.time.values < deployment_event.ntp_stop)
                    masks[deployment] = mask
            self._mask_datasets(masks)

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
                if self.events is not None:
                    cal, param_meta = self.events.get_tiled_cal(value, deployment, times)
                    if cal is not None:
                        kwargs[name] = cal
                        if np.any(np.isnan(cal)):
                            msg = '<{:s}> There was not coefficient data for {:s} for all times in deployment ' \
                                  '{:d} in range ({:s} {:s})'.format(self.request_id, name, deployment, begin_dt, end_dt)
                            log.warn(msg)

            # Internal Parameter
            elif source == stream_key.stream and value.name in dataset:
                kwargs[name] = dataset[value.name].values
                param_meta = self._create_parameter_metadata(value, deployment)

            # Virtual stream parameter
            elif source_dataset and value.name in source_dataset:
                kwargs[name] = source_dataset[value.name].values
                param_meta = self._create_parameter_metadata(value, deployment)

            # External Parameter
            else:
                new_name = '-'.join((source.name, value.name))
                if new_name in dataset:
                    kwargs[name] = dataset[new_name].values
                    param_meta = self._create_parameter_metadata(value, deployment, True)

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
        external_streams = [external.stream for external in self.external_streams]

        function_map, missing = stream_key.stream.create_function_map(param, external_streams)
        kwargs = arg_metadata = None

        if not missing:
            kwargs, arg_metadata = self._build_function_arguments(dataset, stream_key, function_map,
                                                                  deployment, source_dataset)
            missing = {k: function_map[k] for k in set(function_map) - set(kwargs)}

        if not missing and kwargs:
            result, version = self._execute_algorithm(param, kwargs)
            if not isinstance(result, np.ndarray):
                log.warn('<%s> Algorithm for %r returned non ndarray', self.request_id, param.name)
                result = np.array([result])

            self._log_algorithm_inputs(param, kwargs, result, stream_key, dataset)
            calc_metadata = self._create_calculation_metadata(param, version, arg_metadata)
            self.provenance_metadata.calculated_metadata.insert_metadata(param, calc_metadata)

            try:
                self._insert_data(dataset, param, result,
                                  provenance_metadata=self.provenance_metadata,
                                  request_id=self.request_id)
            except ValueError:
                self._insert_data(dataset, param, None,
                                  provenance_metadata=self.provenance_metadata,
                                  request_id=self.request_id)

        else:
            try:
                self._insert_data(dataset, param, None,
                                  provenance_metadata=self.provenance_metadata,
                                  request_id=self.request_id)
            except ValueError:
                # Swallow this raised error, it has already been logged.
                pass

            error_info = {'derived_id': param.id, 'derived_name': param.name,
                          'derived_display_name': param.display_name, 'missing': []}
            for key in missing:
                source, value = missing[key]
                missing_dict = {
                    'source': source,
                    'value': value
                }
                error_info['missing'].append(missing_dict)
            error_info = self._resolve_db_objects(error_info)
            self.provenance_metadata.calculated_metadata.errors.append(error_info)
            log.error('<%s> Unable to create derived product: %r missing: %r',
                      self.request_id, param.name, error_info)

    @staticmethod
    def _insert_data(dataset, param, data, provenance_metadata=None, request_id=None):
        """
        Insert the specified parameter into this dataset. If data is None, use the fill value
        :param dataset:
        :param param:
        :param data:
        :return:
        """
        dims = ['obs']

        # IF dimensions are defined in preload, use those
        # otherwise, create dimensions dynamically based on the
        # shape of the data
        if param.dimensions:
            dims += [d.value for d in param.dimensions]
        else:
            if data is not None:
                for index, _ in enumerate(data.shape[1:]):
                    name = '%s_dim_%d' % (param.name, index)
                    dims.append(name)

        # IF data is missing and specified dimensions aren't already defined
        # we cannot determine the correct shape, limit dimensions to obs
        missing = [d for d in dims if d not in dataset]
        if missing and data is None:
            log.error('Unable to resolve all dimensions for derived parameter: %r. Filling as scalar', missing)
            dims = ['obs']

        fill_value = _get_fill_value(param)

        # Data is None, replace with fill values
        if data is None:
            shape = tuple([len(dataset[d]) for d in dims])
            data = np.zeros(shape)
            data[:] = fill_value

        try:
            attrs = param.attrs

            # Override the fill value supplied by preload if necessary
            attrs['_FillValue'] = fill_value

            coord_columns = 'time lat lon'
            if param.name not in coord_columns:
                attrs['coordinates'] = coord_columns
            dataset[param.name] = (dims, data, attrs)

        except ValueError as e:
            message = 'Unable to insert parameter: %r. Data shape (%r) does not match expected shape (%r)' % \
                      (param, data.shape, e)
            to_attach = {'type': 'FunctionError', "parameter": str(param),
                         'function': str(param.parameter_function), 'message': message}
            if provenance_metadata:
                provenance_metadata.calculated_metadata.errors.append(to_attach)
            log.error('<%s> %s', request_id, message)
            raise

    def _resolve_db_objects(self, obj):
        if isinstance(obj, dict):
            return {self._resolve_db_objects(k): self._resolve_db_objects(obj[k]) for k in obj}
        if isinstance(obj, (list, tuple)):
            return [self._resolve_db_objects(x) for x in obj]
        if isinstance(obj, (Stream, Parameter)):
            return repr(obj)
        return obj

    @log_timing(log)
    def _interpolate_and_import_needed(self, param, external_datasets):
        """
        Given a StreamKey and Parameter, calculate the parameters which need to be interpolated into
        the dataset defined by StreamKey for Parameter
        :param param: Parameter defining the L2 parameter which requires data from an external dataset
        :return:
        """
        log.debug('<%s> _interpolate_and_import_needed for: %r %r', self.request_id, self.stream_key.as_refdes(), param)
        streams = {sk.stream: sk for sk in external_datasets}
        funcmap, missing = self.stream_key.stream.create_function_map(param, streams.keys())
        if not missing:
            for name in funcmap:
                source, value = funcmap[name]
                if source not in ['CAL', self.stream_key.stream]:
                    source_key = streams.get(source)
                    if source_key in external_datasets:
                        self.interpolate_into(source_key, external_datasets[source_key], value)

        else:
            log.error('<%s> Unable to interpolate data: %r, error locating data',
                      self.request_id, param)

    def interpolate_into(self, source_key, source_dataset, parameter):
        if source_key != self.stream_key:
            log.debug('<%s> interpolate_into: %s source: %s param: %r',
                      self.request_id, self.stream_key, source_key, parameter)
            new_name = '-'.join((source_key.stream.name, parameter.name))
            for deployment, ds in self.datasets.iteritems():
                try:
                    ds[new_name] = source_dataset.get_interpolated(ds.time.values, parameter)
                except StreamEngineException as e:
                    log.error(e.message)

    @log_timing(log)
    def get_interpolated(self, target_times, parameter):
        """
        Interpolate <parameter> from this dataset to the supplied times
        :param target_times: Times to interpolate to
        :param parameter: Parameter defining the data to be interpolated
        :return: DataArray containing the interpolated data
        """
        log.info('<%s> get_interpolated source: %s parameter: %r',
                 self.request_id, self.stream_key.as_refdes(), parameter)
        name = parameter.name
        datasets = [self.datasets[deployment][['obs', 'time', name]] for deployment in sorted(self.datasets)
                    if name in self.datasets[deployment]]
        if datasets:
            shape = datasets[0][name].shape
            if len(shape) != 1:
                raise StreamEngineException('<%s> Attempted to interpolate >1d data (%s): %s' %
                                            (self.request_id, name, shape))

            # Two possible choices here.
            # 1) Requested times are contained in a single deployment -> pull from deployment
            # 2) Requested times span multiple deployments. Collapse all deployments to a single dataset
            start, end = target_times[0], target_times[-1]
            # Search for a single deployment which covers this request
            for dataset in datasets:
                ds_start, ds_end = dataset.time.values[0], dataset.time.values[-1]
                if ds_start <= start and ds_end >= end:
                    return interp1d_data_array(dataset.time.values,
                                               dataset[name],
                                               time=target_times)

            # No single deployment contains this data. Create a temporary dataset containing all
            # deployments which contain data for the target parameter, then interpolate
            ds = compile_datasets(datasets)
            return interp1d_data_array(ds.time.values,
                                       ds[name],
                                       time=target_times)

    def _calculate_parameter_list(self, dataset, sk, params, deployment, name):
        if params:
            log.info('<%s> executing %s algorithms for %r deployment %d',
                     self.request_id, name, sk.as_refdes(), deployment)
            for param in params:
                self._create_derived_product(dataset, sk, param, deployment)

    def _create_parameter_metadata(self, param, deployment, interpolated=False):
        """
        Given a source stream and parameter, generate the corresponding parameter metadata
        :param param: Parameter
        :param interpolated: Boolean indicating if this data was interpolated
        :return: Dictionary containing metadata describing this Stream/Parameter
        """
        dataset = self.datasets[deployment]

        if self.time_param and self.time_param.name in dataset:
            # virtual stream
            times = dataset[self.time_param.name].values
            t1, t2 = times[0], times[-1]
            t1_dt, t2_dt = ntp_to_datestring(t1), ntp_to_datestring(t2)

        elif 'time' in dataset:
            # regular stream
            times = dataset.time.values
            t1, t2 = times[0], times[-1]
            t1_dt, t2_dt = ntp_to_datestring(t1), ntp_to_datestring(t2)

        else:
            # time not found!
            t1 = t2 = t1_dt = t2_dt = None

        return {'type': "parameter",
                'source': self.stream_key.as_refdes(),
                'parameter_id': param.id,
                'name': param.name,
                'data_product_identifier': param.data_product_identifier,
                'interpolated': interpolated,
                'time_start': t1,
                'time_startDT': t1_dt,
                'time_end': t2,
                'time_endDT': t2_dt,
                'deployments': [deployment]}

    def _log_algorithm_inputs(self, parameter, kwargs, result, stream_key, dataset):
        flag = self.uflags.get('advancedStreamEngineLogging', False)
        if flag:
            if 'time' in dataset:
                ds_start, ds_end = dataset.time.values[0], dataset.time.values[-1]
            elif stream_key.stream.time_parameter is parameter:
                ds_start, ds_end = result[0], result[-1]
            else:
                ds_start = ds_end = 0

            user = self.uflags.get('userName', '_nouser')
            prefix = self.uflags.get('requestTime', 'time-unspecified')
            log.debug('<%s> _log_algorithm_inputs (%r)', self.request_id, parameter)
            begin_dt, end_dt = ntp_to_datetime(ds_start), ntp_to_datetime(ds_end)
            begin_date = begin_dt.strftime('%Y%m%dT%H%M%S')
            end_date = end_dt.strftime('%Y%m%dT%H%M%S')
            log_dir = '{:s}-{:s}'.format(prefix, self.stream_key.as_dashed_refdes())
            log_name = '{:s}-{:s}-{:s}-{:s}'.format(
                begin_date, end_date, self.stream_key.as_dashed_refdes(), parameter.name
            )
            report = ParameterReport(user, log_dir, log_name)
            report.set_calculated_parameter(parameter.id, parameter.name, parameter.parameter_function.function)
            for key, value in kwargs.iteritems():
                report.add_parameter_argument(parameter.id, key, value.tolist())
            if result is not None:
                report.add_result(result.tolist())
            else:
                report.add_result(None)
            return report.write()

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
                             "parameter": str(parameter),
                             'function': str(func.function_type)}
                raise UnknownFunctionTypeException(func.function_type.value, payload=to_attach)

        except UnknownFunctionTypeException:
            raise
        except Exception as e:
            log.error('<%s> Exception executing algorithm for %r: %s', self.request_id, parameter, e)
            to_attach = {'type': 'FunctionError', "parameter": str(parameter),
                         'function': str(func), 'message': str(e)}
            self.provenance_metadata.calculated_metadata.errors.append(to_attach)
            result = version = None

        return result, version

    @log_timing(log)
    def get_dataset(self, time_range, limit, provenance_metadata, pad_forward, deployments, request_id=None):
        """
        :param time_range:
        :param limit:
        :param provenance_metadata:
        :param pad_forward:
        :param deployments:
        :param request_id:
        :return:
        """
        cass_locations, san_locations, messages = get_location_metadata(self.stream_key, time_range)
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
            datasets.append(self.get_lookback_dataset(self.stream_key, time_range, deployments, request_id))

        if san_locations.total > 0:
            # put the range down if we are within the time range
            t1 = max(time_range.start, san_locations.start_time)
            t2 = min(time_range.stop, san_locations.end_time)
            san_times = TimeRange(t1, t2)
            if limit:
                datasets.append(fetch_nsan_data(self.stream_key, san_times, num_points=int(limit * san_percent),
                                                location_metadata=san_locations))
            else:
                datasets.append(fetch_full_san_data(self.stream_key, san_times, location_metadata=san_locations))
        if cass_locations.total > 0:
            t1 = max(time_range.start, cass_locations.start_time)
            t2 = min(time_range.stop, cass_locations.end_time)
            # issues arise when sending cassandra a query with the exact time range.
            # Data points at the start and end will be left out of the results.  This is an issue for full data
            # queries, to compensate for this we add .1 seconds to the given start and end time
            t1 -= .1
            t2 += .1
            cass_times = TimeRange(t1, t2)
            if limit:
                datasets.append(fetch_nth_data(self.stream_key, cass_times, num_points=int(limit * cass_percent),
                                               location_metadata=cass_locations, request_id=request_id))
            else:
                datasets.append(get_full_cass_dataset(self.stream_key, cass_times,
                                                      location_metadata=cass_locations, request_id=request_id))
        return compile_datasets(datasets)

    @log_timing(log)
    def get_lookback_dataset(self, key, time_range, deployments, request_id=None):
        first_metadata = get_first_before_metadata(key, time_range.start)
        if CASS_LOCATION_NAME in first_metadata:
            locations = first_metadata[CASS_LOCATION_NAME]
            return get_cass_lookback_dataset(key, time_range.start, locations.bin_list[0], deployments, request_id)
        elif SAN_LOCATION_NAME in first_metadata:
            locations = first_metadata[SAN_LOCATION_NAME]
            return get_san_lookback_dataset(key, TimeRange(locations.start_time, time_range.start),
                                            locations.bin_list[0], deployments)
        else:
            return None
