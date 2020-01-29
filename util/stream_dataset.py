import importlib
import json
import logging
import datetime
import sys

import ion_functions
import numexpr
import numpy as np
import time
import ntplib

from ooi_data.postgres.model import Parameter, Stream
from util.advlogging import ParameterReport
from util.cass import fetch_nth_data, get_full_cass_dataset, get_cass_lookback_dataset, get_cass_lookforward_dataset
from util.common import (log_timing, ntp_to_datestring, ntp_to_datetime, UnknownFunctionTypeException,
                         StreamEngineException, TimeRange, MissingDataException)
from util.datamodel import create_empty_dataset, compile_datasets, add_location_data, _get_fill_value
from util.metadata_service import (SAN_LOCATION_NAME, CASS_LOCATION_NAME,
                                   get_first_before_metadata, get_first_after_metadata,
                                   get_location_metadata)

from util.provenance_metadata_store import ProvenanceMetadataStore
from util.san import fetch_nsan_data, fetch_full_san_data, get_san_lookback_dataset
from util.xray_interpolation import interp1d_data_array
from engine import app

log = logging.getLogger(__name__)

PYTHON_VERSION = '.'.join(map(str, (sys.version_info[0:3])))
ION_VERSION = getattr(ion_functions, '__version__', 'unversioned')
INSTRUMENT_ATTRIBUTE_MAP = app.config.get('INSTRUMENT_ATTRIBUTE_MAP')


class StreamDataset(object):
    def __init__(self, stream_key, uflags, external_streams, request_id):
        self.stream_key = stream_key
        self.provenance_metadata = ProvenanceMetadataStore(request_id)
        self.uflags = uflags
        self.external_streams = external_streams
        self.request_id = request_id
        self.datasets = {}
        self.events = None

        self.params = {}
        self.missing = {}
        self.external = [p for p in stream_key.stream.derived if stream_key.stream.needs_external([p])]

        if self.stream_key.is_virtual:
            self.time_param = Parameter.query.get(self.stream_key.stream.time_parameter)
        else:
            self.time_param = None

    def fetch_raw_data(self, time_range, limit, should_pad):
        dataset = self.get_dataset(time_range, limit, self.provenance_metadata,
                                   should_pad, self.request_id)
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
                self.datasets[deployment] = self._prune_duplicate_times(group)
                self.params[deployment] = [p for p in self.stream_key.stream.derived]

        else:
            raise MissingDataException("Query returned no results for stream %s" % self.stream_key)

    @staticmethod
    def _prune_duplicate_times(dataset):
        mask = np.diff(np.insert(dataset.time.values, 0, 0.0)) != 0
        if not mask.all():
            dataset = dataset.isel(obs=mask)
            dataset['obs'] = np.arange(dataset.obs.size)
        return dataset

    def calculate_all(self, source_datasets=None, ignore_missing_optional_params=False):
        """
        Brute force resolution of parameters - continue to loop as long as we can progress
        """
        source_datasets = source_datasets if source_datasets else {}
        for deployment, dataset in self.datasets.iteritems():
            source_dataset = source_datasets.get(deployment)
            while self.params[deployment]:
                remaining = []
                for param in self.params[deployment]:
                    missing = self._try_create_derived_product(dataset, self.stream_key,
                                                               param, deployment, source_dataset,
                                                               ignore_missing_optional_params)
                    if missing:
                        remaining.append(param)
                        self.missing.setdefault(deployment, {})[param] = missing
                if len(remaining) == len(self.params[deployment]):
                    break
                self.params[deployment] = remaining

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

                    if attribute == 'lastModifiedTimestamp':
                        value = datetime.datetime.utcfromtimestamp(value / 1000.0).isoformat()

                    ds.attrs[INSTRUMENT_ATTRIBUTE_MAP[attribute]] = value

    def interpolate_needed(self, external_datasets, interpolate_virtual=False):
        """
        Given a set of external Datasets, calculate the parameters which need to be interpolated into
        those datasets
        :param external_datasets: The Datasets which need parameters interpolated into them
        :param interpolate_virtual: A flag for whether or not virtual stream data should be interpolated. If set to 
        True, interpolation will be performed for virtual streams only. If set to False, interpolation will be
        performed for non-virtual streams only. This allows interpolation before and after calculation of virtual
        streams without duplication of effort. If virtual streams are interpolated before the virtual streams are
        calculated, the system will be attempting to interpolate on a dataset that is not yet populated.
        :return:
        """
        if not self.time_param:
            for param in self.external:
                self._interpolate_and_import_needed(param, external_datasets, interpolate_virtual)

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
                missing = self._try_create_derived_product(dataset, self.stream_key, self.time_param, deployment,
                                                           source_dataset=source_dataset)
                if missing:
                    self.missing.setdefault(deployment, {})[self.time_param] = missing
                    continue

                dataset['time'] = dataset[self.time_param.name].copy()
                deployments = np.empty_like(dataset.time.values, dtype='int32')
                deployments[:] = deployment
                dataset['deployment'] = ('obs', deployments, {'name': 'deployment'})
                self.params[deployment] = [p for p in self.stream_key.stream.derived if not p == self.time_param]
        self.calculate_all(source_datasets=source_stream_dataset.datasets)

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

    def exclude_flagged_data(self, annotation_store):
        masks = {}
        if annotation_store.has_exclusion():
            for deployment in self.datasets:
                dataset = self.datasets[deployment]
                mask = annotation_store.get_exclusion_mask(self.stream_key, dataset.time.values)
                masks[deployment] = mask

            self._mask_datasets(masks)

    def exclude_nondeployed_data(self, require_deployment=True):
        """
        Exclude data outside of deployment times.
        :param require_deployment: True to exclude all data without deployment information,
                                   False to include data without deployment info.
        :return: Nothing, this function directly modifies the underlying dataset.
        """
        masks = {}
        if self.events is not None:
            for deployment in self.datasets:
                dataset = self.datasets[deployment]
                if deployment in self.events.deps:
                    # if a deployment exists use it to restrict the range of values
                    deployment_event = self.events.deps[deployment]
                    masks[deployment] = (dataset.time.values >= deployment_event.ntp_start) & \
                        (dataset.time.values < deployment_event.ntp_stop)
                elif require_deployment:
                    # if a deployment doesn't exist and we require_deployment, restrict all values
                    masks[deployment] = np.zeros_like(dataset.time.values).astype('bool')
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
                            msg = '<{:s}> There was not coefficient data for {:s} for all times in deployment {:d} ' \
                                  'in range ({:s} {:s})'.format(self.request_id, name, deployment, begin_dt, end_dt)
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
                    param_meta = self._create_parameter_metadata(value, deployment, source.name)

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

    def fill_missing(self):
        for deployment, dataset in self.datasets.iteritems():
            for param in self.params[deployment]:
                missing = self.missing.get(deployment, {}).get(param, {})
                self._insert_missing(dataset, param, missing)

    @staticmethod
    def is_missing_arg_optional(missing_arg_map, param):
        """
        Check if the missing args are specified as optional in the function_parameter_map
        of the parameter.
        :param missing_arg_map: map of missing arguments for the function contained in the parameter
        :param param: the Parameter who's parameter_function_map specifies the args
        :return: True if all of the missing args are optional, else False
        """
        for arg_name in missing_arg_map.keys():
            arg_possible_params = param.parameter_function_map.get(arg_name, None)
            # An arg in the function map that specified the string 'None'
            # as a possible parameter is an optional argument. The actual
            # python function that takes these arguments should specify a
            # default for this argument since this arg may not be passed
            # when the function is called.
            if isinstance(arg_possible_params, (list, tuple)):
                if 'None' not in arg_possible_params:
                    return False
            elif arg_possible_params != 'None':
                return False
        return True

    @log_timing(log)
    def _try_create_derived_product(self, dataset, stream_key, param, deployment, source_dataset=None,
                                    ignore_missing_optional_params=False):
        """
        Extract the necessary args to create the derived product <param>, call _execute_algorithm
        and insert the result back into dataset.
        :param dataset: source data
        :param stream_key: source stream
        :param param: derived parameter
        :param deployment: deployment number
        :return:  dictionary {parameter: [sources]}
        """
        log.info('<%s> _create_derived_product %r %r', self.request_id, stream_key.as_refdes(), param)
        external_streams = [external.stream for external in self.external_streams]

        function_map, missing = stream_key.stream.create_function_map(param, external_streams)

        # Consider a param as missing function arguments if we are not allowed to ignore
        # optional arguments in this pass or if the missing args are not specified as optional.
        # An arg missing at this point would be a dpi where a stream exposing that dpi parameter
        # could not be found and that dpi was not specified as optional arg to this param function.
        if missing and not (ignore_missing_optional_params and self.is_missing_arg_optional(missing, param)):
            return missing

        kwargs, arg_metadata = self._build_function_arguments(dataset, stream_key, function_map,
                                                              deployment, source_dataset)
        missing = {k: function_map[k] for k in set(function_map) - set(kwargs)}

        # Function arguments can be missing at this point if the param in a supporting dataset
        # was resolved but there was not any records in that dataset for the time range. In such
        # a case, the dataset would have not been added (or even removed) from StreamRequest.datasets map
        # (see StreamRequest.fetch_raw_data()) so the param in that dataset is considered missing. Continue
        # processing only if that missing param is considered to be an optional argument to this function.
        if missing and not (ignore_missing_optional_params and self.is_missing_arg_optional(missing, param)):
            return missing

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

    def _insert_missing(self, dataset, param, missing):
        """
        insert missing notification into provenance and fill values into the dataset
        """
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

        # the preload defined parameter dimensions
        param_dimensions = []
        if param.dimensions:
            param_dimensions = [d.value for d in param.dimensions]

        if '-obs' in param_dimensions:
            # remove obs dimension from parameter's dimensions and data (13025 AC2)
            param_dimensions.remove('-obs')
            dims = param_dimensions
            data = data[0] if data is not None else None
        elif param_dimensions:
            # append parameter dimensions onto obs
            dims += param_dimensions
        else:
            # create dimensions dynamically based on the
            # shape of the data
            if data is not None:
                for index, _ in enumerate(data.shape[1:]):
                    name = '%s_dim_%d' % (param.name, index)
                    dims.append(name)

        # IF data is missing and specified dimensions aren't already defined
        # we cannot determine the correct shape, limit dimensions to obs
        missing = [d for d in dims if d not in dataset.dims]
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
    def _interpolate_and_import_needed(self, param, external_datasets, interpolate_virtual=False):
        """
        Given a StreamKey and Parameter, calculate the parameters which need to be interpolated into
        the dataset defined by StreamKey for Parameter
        :param param: Parameter defining the L2 parameter which requires data from an external dataset
        :param external_datasets: The Datasets which need parameters interpolated into them
        :param interpolate_virtual: A flag for whether or not virtual stream data should be interpolated. If set to 
        True, interpolation will be performed for virtual streams only. If set to False, interpolation will be
        performed for non-virtual streams only.
        :return:
        """
        log.debug('<%s> _interpolate_and_import_needed for: %r %r', self.request_id, self.stream_key.as_refdes(), param)
        streams = {sk.stream: sk for sk in external_datasets}
        funcmap, missing = self.stream_key.stream.create_function_map(param, streams.keys())
        if not missing or self.is_missing_arg_optional(missing, param):
            for name in funcmap:
                source, value = funcmap[name]
                if source not in ['CAL', self.stream_key.stream]:
                    source_key = streams.get(source)
                    # prevent trying to interpolate with unpopulated virtual streams
                    # if interpolating virtual streams, skip other parameters
                    if (interpolate_virtual and source_key.is_virtual) or not (
                            interpolate_virtual or source_key.is_virtual):
                        if source_key in external_datasets:
                            self.interpolate_into(source_key, external_datasets[source_key], value)

        else:
            log.error('<%s> Unable to interpolate data: %r, error locating data',
                      self.request_id, param)

    def interpolate_into(self, source_key, source_dataset, parameter):
        if source_key != self.stream_key:
            log.debug('<%s> interpolate_into: %s source: %s param: %r',
                      self.request_id, self.stream_key, source_key, parameter)
            only_same_deployment = self.interpolate_only_from_same_deployment(source_key, parameter)
            new_name = '-'.join((source_key.stream.name, parameter.name))
            for deployment, ds in self.datasets.iteritems():
                if new_name in ds:
                    continue
                try:
                    ds[new_name] = source_dataset.get_interpolated(ds.time.values, parameter,
                                                                   deployment if only_same_deployment else None)
                except StreamEngineException as e:
                    log.error(e.message)

    def interpolate_only_from_same_deployment(self, source_key, parameter):
        """
        Determine whether we should interpolate data only from the same deployment
        as the target dataset and not allow interpolation from across multiple
        source datasets deployments.
        :param source_key: RefDes + Stream for dataset to interpolate from
        :param parameter: Parameter defining the data to be interpolated
        :return: Boolean
        """
        # If the data level of the parameter in the source dataset is 0 (raw data)
        # and the source and target datasets are from the same instrument,
        # assume that data from the source can only be used from the same deployment
        # as the target dataset that it is getting interpolated into.
        # In contrast, a finished data product (data level greater than 0) and can be
        # used interpolated into an different instrument's dataset regardless of
        # deployment. Interpolating pressure form a CTD would be an example of the latter.
        if parameter.data_level == 0 and (source_key.as_tuple()[:4] == self.stream_key.as_tuple()[:4]):
            log.debug('Interpolating parameter %r from %s only from same deployment',
                      parameter, source_key)
            return True
        return False

    @log_timing(log)
    def get_interpolated(self, target_times, parameter, required_deployment=None):
        """
        Interpolate <parameter> from this dataset to the supplied times
        :param target_times: Times to interpolate to
        :param parameter: Parameter defining the data to be interpolated
        :param required_deployment: Only interpolate from this deployment
        :return: DataArray containing the interpolated data
        """
        log.info('<%s> get_interpolated source: %s parameter: %r',
                 self.request_id, self.stream_key.as_refdes(), parameter)
        name = parameter.name
        datasets = [self.datasets[deployment][['obs', 'time', name]] for deployment in sorted(self.datasets)
                    if name in self.datasets[deployment]
                    and (required_deployment is None or deployment == required_deployment)]
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
                if len(datasets) == 1 or (ds_start <= start and ds_end >= end):
                    return interp1d_data_array(dataset.time.values,
                                               dataset[name],
                                               time=target_times)

            # No single deployment contains this data. Create a temporary dataset containing all
            # deployments which contain data for the target parameter, then interpolate
            ds = compile_datasets(datasets)
            return interp1d_data_array(ds.time.values,
                                       ds[name],
                                       time=target_times)

    def _get_external_stream_key(self, external_stream_name):
        """
        Get the external stream key that matches the given stream name.
        :param external_stream_name: the name of the external stream
        :return: the matching external stream key or None if no match was found
        """
        match = None
        for external_stream_key in self.external_streams:
            if external_stream_key.stream_name == external_stream_name:
                match = external_stream_key
                break
        return match

    def _create_parameter_metadata(self, param, deployment, interpolated_stream_name=None):
        """
        Given a source stream and parameter, generate the corresponding parameter metadata
        :param param: Parameter
        :param interpolated_stream_name: The stream name for an interpolated parameter
        :return: Dictionary containing metadata describing this Stream/Parameter
        """

        dataset = self.datasets[deployment]
        source = self.stream_key.as_refdes()
        interpolated = False

        if interpolated_stream_name:
            interpolated = True
            external_stream_key = self._get_external_stream_key(interpolated_stream_name)
            if external_stream_key:
                source = external_stream_key.as_refdes()
            else:
                log.warn("Unable to locate external stream key for: "+interpolated_stream_name)
                source = "Unknown"

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
                'source': source,
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
            if 'time' not in kwargs:
                report.add_parameter_argument(parameter.id, 'time', dataset.time.values.tolist())
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
            if func.function_type == 'PythonFunction' and func.owner != '__builtin__':
                module = importlib.import_module(func.owner)
                version = ION_VERSION
                result = getattr(module, func.function)(**kwargs)

            elif func.function_type == 'PythonFunction' and func.owner == '__builtin__':
                version = 'Python ' + PYTHON_VERSION
                # evaluate the function in an empty global namespace
                # using the provided func.args as the local namespace
                result = np.array(eval(func.function, {}, kwargs))

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
    def get_dataset(self, time_range, limit, provenance_metadata, pad_dataset, request_id=None):
        """
        :param time_range:
        :param limit:
        :param provenance_metadata:
        :param pad_dataset:
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

        # If this is a supporting stream (ie. not the primary requested stream),
        # get extra data points on both sides immediately outside of the requested
        # time range for higher quality interpolation of supporting stream data
        # into the primary data set at the request time boundaries. The extra
        # data points must be within the time range of the deployments.
        if pad_dataset and app.config['LOOKBACK_QUERY_LIMIT'] > 0:
            # Get the start time of the first and stop time of the last deployments
            # within the requested time range.
            deployment_time_range = self.get_deployment_time_range(time_range)
            if deployment_time_range.get("start", None):
                datasets.append(self.get_lookback_dataset(self.stream_key, time_range,
                                                          deployment_time_range["start"], request_id))
            if deployment_time_range.get("stop", None):
                datasets.append(self.get_lookforward_dataset(self.stream_key, time_range,
                                                             deployment_time_range["stop"], request_id))

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
    def get_lookback_dataset(self, key, request_time_range, deployment_start_time, request_id=None):
        first_metadata_before = get_first_before_metadata(key, request_time_range.start)
        if CASS_LOCATION_NAME in first_metadata_before:
            locations = first_metadata_before[CASS_LOCATION_NAME]

            return get_cass_lookback_dataset(key, request_time_range.start, locations.bin_list[0],
                                             deployment_start_time, request_id)
        elif SAN_LOCATION_NAME in first_metadata_before:
            locations = first_metadata_before[SAN_LOCATION_NAME]

            # Note that the deployments list was originally hardcoded to an empty list ([]) in
            # StreamDataset.fetch_raw_data(). That same hard code is moved to the function
            # call below to preserve the behavior of that function while changing the behavior
            # of the above call to get_cass_lookback_dataset. I would think that the
            # hard coded empty list of deployment numbers should be removed from the call
            # below at some point in the future as well. Note that since the list of deployments
            # is empty, the function call below returns an empty list.
            return get_san_lookback_dataset(key, TimeRange(locations.start_time, request_time_range.start),
                                            locations.bin_list[0], [])
        else:
            return None

    @log_timing(log)
    def get_lookforward_dataset(self, key, request_time_range, deployment_stop_time, request_id=None):
        first_metadata_after = get_first_after_metadata(key, request_time_range.stop)
        if CASS_LOCATION_NAME in first_metadata_after:
            locations = first_metadata_after[CASS_LOCATION_NAME]

            return get_cass_lookforward_dataset(key, request_time_range.stop, locations.bin_list[0],
                                                deployment_stop_time, request_id)
        elif SAN_LOCATION_NAME in first_metadata_after:
            locations = first_metadata_after[SAN_LOCATION_NAME]

            # Note that the deployments list was originally hardcoded to an empty list ([]) in
            # StreamDataset.fetch_raw_data(). That same hard code when passed to get_san_lookback_dataset()
            # above results in an empty list getting returned from that function. Instead of implementing an
            # analogous "do nothing" get_san_lookforward_dataset() function, we just return an empty set
            # here until that function is properly implemented.
            return []
        else:
            return None

    @log_timing(log)
    def get_deployment_time_range(self, request_time_range):
        # The expected deployments are the intersection of those that:
        # end after the start of the requested time range and
        # start before the end of the requested time range
        expected_deployment_numbers = []
        for dep_no in sorted(self.events.deps):
            if (request_time_range.start is None or
                    self.events.deps[dep_no].ntp_stop is None or
                    self.events.deps[dep_no].ntp_stop >= request_time_range.start) and \
               (request_time_range.stop is None or
                    self.events.deps[dep_no].ntp_start is None or
                    self.events.deps[dep_no].ntp_start < request_time_range.stop):
                expected_deployment_numbers.append(dep_no)

        # The deployment time range is the start time of the first
        # deployment and the stop time of the last deployment
        deployment_time_range = {"start": None, "stop": None}
        if expected_deployment_numbers:
            deployment_time_range["start"] = self.events.deps[expected_deployment_numbers[0]].ntp_start
            deployment_time_range["stop"] = self.events.deps[expected_deployment_numbers[-1]].ntp_stop
            if deployment_time_range["stop"] is None:
                deployment_time_range["stop"] = ntplib.system_to_ntp_time(time.time())

        return deployment_time_range

