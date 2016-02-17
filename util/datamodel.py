"""
This module is for classes and functions related to the data structures
stream_engine uses to model science data.  Data is currently being backed by a
dictionary representation boiling down to raw np ndarrays, but in the future
data will be in xray Datasets.
"""
import datetime
import json
import logging

import msgpack
import xray
import numpy as np
import pandas as pd

from engine import app
import xray_interpolation as xinterp
from common import MissingTimeException, ntp_to_datestring, MissingDataException, get_fill_value, StreamEngineException
from preload_database.model.preload import Parameter

__author__ = 'Stephen Zakrewsky'

log = logging.getLogger(__name__)


def _get_ds_attrs(stream_key, request_uuid):
    refdes = stream_key.as_dashed_refdes()
    attrs = {
        'subsite': stream_key.subsite,
        'node': stream_key.node,
        'sensor': stream_key.sensor,
        'collection_method': stream_key.method,
        'stream': stream_key.stream.name,
        'title': '{:s} for {:s}'.format(app.config['NETCDF_TITLE'], refdes),
        'institution': '{:s}'.format(app.config['NETCDF_INSTITUTION']),
        'source': '{:s}'.format(refdes),
        'history': '{:s} {:s}'.format(datetime.datetime.utcnow().isoformat(), app.config['NETCDF_HISTORY_COMMENT']),
        'references': '{:s}'.format(app.config['NETCDF_REFERENCE']),
        'comment': '{:s}'.format(app.config['NETCDF_COMMENT']),
        'Conventions': '{:s}'.format(app.config['NETCDF_CONVENTIONS']),
        'Metadata_Conventions': '{:s}'.format(app.config['NETCDF_METADATA_CONVENTIONS']),
        'feature_Type': '{:s}'.format(app.config['NETCDF_FEATURE_TYPE']),
        'featureType': '{:s}'.format(app.config['NETCDF_FEATURE_TYPE']),
        'cdm_data_type': '{:s}'.format(app.config['NETCDF_CDM_DATA_TYPE']),
        'nodc_template_version': '{:s}'.format(app.config['NETCDF_NODC_TEMPLATE_VERSION']),
        'standard_name_vocabulary': '{:s}'.format(app.config['NETCDF_STANDARD_NAME_VOCABULARY']),
        'summary': '{:s}'.format(app.config['NETCDF_SUMMARY']),
        'uuid': '{:s}'.format(str(request_uuid)),
        'requestUUID': '{:s}'.format(str(request_uuid)),
        'id': '{:s}'.format(refdes),
        'naming_authority': '{:s}'.format(app.config['NETCDF_NAMING_AUTHORITY']),
        'creator_name': '{:s}'.format(app.config['NETCDF_CREATOR_NAME']),
        'creator_url': '{:s}'.format(app.config['NETCDF_CREATOR_URL']),
        'infoUrl': '{:s}'.format(app.config['NETCDF_INFO_URL']),
        'sourceUrl': '{:s}'.format(app.config['NETCDF_SOURCE_URL']),
        'creator_email': '{:s}'.format(app.config['NETCDF_CREATOR_EMAIL']),
        'project': '{:s}'.format(app.config['NETCDF_PROJECT']),
        'processing_level': '{:s}'.format(app.config['NETCDF_PROCESSING_LEVEL']),
        'keywords_vocabulary': '{:s}'.format(app.config['NETCDF_KEYWORDS_VOCABULARY']),
        'keywords': '{:s}'.format(app.config['NETCDF_KEYWORDS']),
        'acknowledgement': '{:s}'.format(app.config['NETCDF_ACKNOWLEDGEMENT']),
        'contributor_name': '{:s}'.format(app.config['NETCDF_CONTRIBUTOR_NAME']),
        'contributor_role': '{:s}'.format(app.config['NETCDF_CONTRIBUTOR_ROLE']),
        'date_created': '{:s}'.format(datetime.datetime.utcnow().isoformat()),
        'date_modified': '{:s}'.format(datetime.datetime.utcnow().isoformat()),
        'publisher_name': '{:s}'.format(app.config['NETCDF_PUBLISHER_NAME']),
        'publisher_url': '{:s}'.format(app.config['NETCDF_PUBLISHER_URL']),
        'publisher_email': '{:s}'.format(app.config['NETCDF_PUBLISHER_EMAIL']),
        'license': '{:s}'.format(app.config['NETCDF_LICENSE']),
    }
    return attrs


def _group_by_stream_key(ds, pd_data, stream_key, location_information, deployment, provenance_metadata):
    time_data, time_parameter = _get_time_data(pd_data, stream_key)
    # sometimes we will get duplicate timestamps
    # INITIAL solution is to remove any duplicate timestamps
    # and the corresponding data by creating a mask to match
    # only valid INCREASING times
    mask = np.diff(np.insert(time_data, 0, 0.0)) != 0
    time_data = time_data[mask]
    attrs = {'units': 'seconds since 1900-01-01 0:0:0', 'standard_name': 'time',
             'long_name': 'time',
             'axis': 'T',
             'calendar': app.config["NETCDF_CALENDAR_TYPE"]}
    ds['time'] = ('obs', time_data, attrs)

    for param_id in pd_data:

        # Keep time_parameter for streams with source stream
        # dependency (e.g., Metbk_hourly.met_timeflx)
        virtual_request = stream_key.stream.source_streams
        if (
                    (param_id == time_parameter and not virtual_request) or
                        stream_key.as_refdes() not in pd_data[param_id]
        ):
            continue

        param = Parameter.query.get(param_id)
        # param can be None if this is not a real parameter,
        # like deployment for deployment number
        param_name = param_id if param is None else param.name

        data = pd_data[param_id][stream_key.as_refdes()]['data']
        if len(mask) != len(data):
            log.error("Length of mask does not equal length of data")
            continue

        if not isinstance(data, np.ndarray):
            log.warn("Expected parameter '%s' (%s) to be in np format but was %s.  Converting", param_name, param_id,
                     type(data))
            data = np.array(data)
        data = data[mask]
        if param is not None:
            data = data.astype(param.value_encoding.value)
        else:
            if param_name in app.config['INTERNAL_OUTPUT_MAPPING']:
                data = data.astype(app.config['INTERNAL_OUTPUT_MAPPING'][param_name])

        if param_name in app.config['INTERNAL_OUTPUT_EXCLUDE_LIST']:
            continue

        dims = ['obs']
        if len(data.shape) > 1:
            for index, dimension in enumerate(data.shape[1:]):
                name = '%s_dim_%d' % (param_name, index)
                dims.append(name)

        if param_name in ['lat', 'lon', 'depth']:
            array_attrs = {}
        else:
            array_attrs = {'coordinates': 'time lat lon depth'}

        if param:
            if param.unit is not None:
                array_attrs['units'] = param.unit.value
                if param.unit.value.startswith('seconds since'):
                    array_attrs['calendar'] = app.config["NETCDF_CALENDAR_TYPE"]
            if get_fill_value(param) is not None:
                array_attrs['_FillValue'] = get_fill_value(param)
            # Long name needs to be display name to comply with cf 1.6.
            # http://cfconventions.org/Data/cf-conventions/cf-conventions-1.6/build/cf-conventions.html#long-name

            if param.display_name is not None:
                array_attrs['long_name'] = param.display_name
            elif param.name is not None:
                array_attrs['long_name'] = param.name
            else:
                log.warn('Could not produce long_name attribute for {:s} defaulting to parameter name'.format(
                        str(param_name)))
                array_attrs['long_name'] = param_name
            if param.standard_name is not None:
                array_attrs['standard_name'] = param.standard_name
            if param.description is not None:
                array_attrs['comment'] = param.description
            if param.data_product_identifier is not None:
                array_attrs['data_product_identifier'] = param.data_product_identifier
        else:
            # To comply with cf 1.6 giving long name the same as parameter name
            array_attrs['long_name'] = param_name

            if param_name in ['lat', 'lon']:
                array_attrs['units'] = "degrees"

        ds[param_name] = (dims, data, array_attrs)

    fix_lat_lon_depth(ds, stream_key, deployment, location_information)
    if 'deployment' not in ds:
        ds['deployment'] = ('obs', np.array([deployment] * ds.obs.size, dtype=np.int32), {'long_name': 'deployment'})
    if provenance_metadata is not None:
        prov = provenance_metadata.get_provenance_dict()
        prov_values = {}
        for k, v in prov.iteritems():
            prov_values[k] = v['file_name'] + " " + v['parser_name'] + " " + v['parser_version']
        prov_array = []
        if 'provenance' in ds:
            for i in ds['provenance'].values:
                if i in prov_values:
                    prov_array.append(prov_values[i])
                else:
                    prov_array.append('')
            prov_array = np.array(prov_array, dtype=str)
            ds['l0_provenance_information'] = ('obs', prov_array, {'long_name': 'l0_provenance_data',
                                                                   'description': 'file name, parser name, and parser version'})


def fix_lat_lon_depth(ds, stream_key, deployment, location_information):
    location_vals = {}
    for loc in location_information.get(stream_key.as_three_part_refdes(), []):
        if loc['deployment'] == deployment:
            location_vals = loc
            break
    if 'lat' not in ds.variables:
        lat = location_vals.get('lat')
        if lat is None:
            log.warn('No latitude!! Using fill value')
            lat = 90.0
        latarr = np.empty(ds.time.size)
        latarr.fill(lat)
        ds['lat'] = ('obs', latarr, {'axis': 'Y', 'units': 'degrees_north', 'standard_name': 'latitude'})
    else:
        ds['lat'].attrs['axis'] = 'Y'
        ds['lat'].standard_name = 'latitude'

    if 'lon' not in ds.variables:
        lon = location_vals.get('lon')
        if lon is None:
            log.warn('No longitude!! Using fill value')
            lon = -180.0
        lonarr = np.empty(ds.time.size)
        lonarr.fill(lon)
        ds['lon'] = ('obs', lonarr, {'axis': 'X', 'units': 'degrees_east', 'standard_name': 'longitude'})
    else:
        ds['lon'].attrs['axis'] = 'X'
        ds['lon'].standard_name = 'longitude'


def _add_dynamic_attributes(ds, stream_key, location_information, deployment):
    if len(ds.keys()) == 0:
        raise MissingDataException("No data present in dataset")

    time_data = ds['time']
    # Do a final update to insert the time_coverages, and geospatial lat and lons
    ds.attrs['time_coverage_start'] = ntp_to_datestring(time_data.values[0])
    ds.attrs['time_coverage_end'] = ntp_to_datestring(time_data.values[-1])
    # Take an estimate of the number of seconds between values in the data range.
    if time_data.size > 0:
        total_time = time_data.values[-1] - time_data.values[0]
        hz = total_time / float(len(time_data.values))
        ds.attrs['time_coverage_resolution'] = 'P{:.2f}S'.format(hz)
    else:
        ds.attrs['time_coverage_resolution'] = 'P0S'

    location_vals = {}
    for loc in location_information.get(stream_key.as_three_part_refdes(), []):
        if loc['deployment'] == deployment:
            location_vals = loc
            break

    if 'location_name' in location_vals:
        ds.attrs['location_name'] = str(location_vals['location_name'])
    ds.attrs['geospatial_lat_min'] = min(ds.variables['lat'].values)
    ds.attrs['geospatial_lat_max'] = max(ds.variables['lat'].values)
    ds.attrs['geospatial_lon_min'] = min(ds.variables['lon'].values)
    ds.attrs['geospatial_lon_max'] = max(ds.variables['lon'].values)
    ds.attrs['geospatial_lat_units'] = 'degrees_north'
    ds.attrs['geospatial_lat_resolution'] = app.config["GEOSPATIAL_LAT_LON_RES"]
    ds.attrs['geospatial_lon_units'] = 'degrees_east'
    ds.attrs['geospatial_lon_resolution'] = app.config["GEOSPATIAL_LAT_LON_RES"]
    depth_units = str(location_vals.get('depth_units', app.config["Z_DEFAULT_UNITS"]))
    ds.attrs['geospatial_vertical_units'] = depth_units
    ds.attrs['geospatial_vertical_resolution'] = app.config['Z_RESOLUTION']
    ds.attrs['geospatial_vertical_positive'] = app.config['Z_POSITIVE']


class StreamData(object):
    def __init__(self, stream_request, data, provenance_metadata, annotation_store):
        self.stream_request = stream_request
        self.stream_keys = stream_request.stream_keys
        self.data = data
        self.location_information = stream_request.location_information
        if stream_request.include_annotations:
            self.annotation_store = annotation_store
        else:
            self.annotation_store = None
        if stream_request.include_provenance:
            self.provenance_metadata = provenance_metadata
        else:
            self.provenance_metadata = None
        self.deployments = sorted(data.keys())
        self.deployment_streams = self._build_deployment_stream_map(stream_request, data)
        self.deployment_times = {}

    def _build_deployment_stream_map(self, sr, data):
        dp_map = {}
        for deployment in data:
            streams = set()
            dep_data = data[deployment]
            for sk in sr.stream_keys:
                tp = sk.stream.time_parameter
                if tp in dep_data:
                    if sk.as_refdes() in dep_data[tp]:
                        streams.add(sk.as_refdes())
            dp_map[deployment] = streams
        return dp_map

    def groups(self, stream_key=None, deployment=None):
        if stream_key is None:
            stream_keys = self.stream_keys
        else:
            stream_keys = [stream_key]

        if deployment is None:
            deployments = self.deployments
        else:
            deployments = [deployment]

        request_id = self.stream_request.request_id
        for sk in stream_keys:
            for d in deployments:
                if self.check_stream_deployment(sk, d):
                    pd_data = self.data[d]
                    ds = _open_new_ds(sk, d, request_id, self.provenance_metadata, self.annotation_store)
                    _group_by_stream_key(ds, pd_data, sk, self.location_information, d, self.provenance_metadata)
                    _add_dynamic_attributes(ds, sk, self.location_information, d)
                    times = self.deployment_times.get(d, None)
                    if times is not None:
                        ds = xinterp.interp1d_Dataset(ds, time=times)
                    yield sk, d, ds

    def check_stream_deployment(self, stream_key, deployment):
        if deployment not in self.data:
            return False
        if stream_key.as_refdes() not in self.deployment_streams[deployment]:
            return False
        return True

    def get_time_data(self, stream_key):
        deployment_times = {}
        for d in self.deployments:
            pd_data = self.data[d]
            deployment_times[d] = _get_time_data(pd_data, stream_key)[0]
        return deployment_times


def fix_data_arrays(data, unpacked):
    if unpacked is None:
        return
    if len(unpacked) != data.shape[0]:
        app.logger.warn("Mismatched dimensions could not fill array")
        return
    if len(data.shape) == 1:
        for idx, val in enumerate(unpacked):
            if idx < len(data):
                """ Don't overwrite the fill value when data is None """
                if unpacked[idx] is not None:
                    data[idx] = unpacked[idx]
    else:
        if isinstance(unpacked, list):
            for data_sub, unpacked_sub in zip(data, unpacked):
                fix_data_arrays(data_sub, unpacked_sub)


def to_xray_dataset(cols, data, stream_key, request_uuid, san=False):
    """
    Make an xray dataset from the raw cassandra data
    """
    if len(data) == 0:
        return None

    attrs = _get_ds_attrs(stream_key, request_uuid)
    params = {p.name: p for p in stream_key.stream.parameters if not p.is_function}

    if san:
        attrs['title'] = '{:s} for {:s}'.format("SAN offloaded netCDF", stream_key.as_dashed_refdes())
        attrs['history'] = '{:s} {:s}'.format(datetime.datetime.utcnow().isoformat(), 'generated netcdf for SAN')

    dataset = xray.Dataset(attrs=attrs)
    dataframe = pd.DataFrame(data=data, columns=cols)

    # sometimes we will get duplicate timestamps
    # INITIAL solution is to remove any duplicate timestamps
    # and the corresponding data by creating a mask to match
    # only valid INCREASING times
    mask = np.diff(np.insert(dataframe.time, 0, 0.0)) != 0
    dataframe = dataframe[mask]

    for column in dataframe.columns:
        if column in app.config['INTERNAL_OUTPUT_EXCLUDE_LIST']:
            continue

        param = params.get(column)
        if param:
            encoding = param.value_encoding
            fill_val = get_fill_value(param)
            is_array = param.parameter_type == 'array<quantity'
        else:
            encoding = str
            fill_val = ''
            is_array = False

        if column in app.config['INTERNAL_OUTPUT_MAPPING']:
            encoding = app.config['INTERNAL_OUTPUT_MAPPING'][column]

        data = replace_values(dataframe[column].values, encoding, fill_val, is_array, column)

        # Fix up the dimensions for possible multi-d objects
        dims = ['obs']
        if len(data.shape) > 1:
            for index, dim in enumerate(data.shape[1:]):
                name = "{:s}_dim_{:d}".format(column, index)
                dims.append(name)

        if column == 'time':
            array_attrs = {
                'units': 'seconds since 1900-01-01 0:0:0',
                'standard_name': 'time',
                'long_name': 'time',
                'axis': 'T',
                'calendar': app.config["NETCDF_CALENDAR_TYPE"]
            }
        elif column in params:
            array_attrs = params[column].attrs
        else:
            array_attrs = {'name': column}

        dataset.update({column: xray.DataArray(data, dims=dims, attrs=array_attrs)})

    return dataset


def replace_values(data_slice, value_encoding, fill_value, is_array, name):
    """
    Replace any missing values in the parameter
    :param data_slice: pandas series to replace missing values in
    :param value_encoding: Type information about the parameter
    :param fill_value: Fill value for the parameter
    :param is_array: Flag indicating if this is a msgpack array
    :return: data_slice with missing values filled with fill value
    """
    # Nones can only be in ndarrays with dtype == object.  NetCDF
    # doesn't like objects.  First replace Nones with the
    # appropriate fill value.
    #
    # pandas does some funny things to missing values if the whole column is missing it becomes a None filled object
    # Otherwise pandas will replace floats with Not A Number correctly.
    # Integers are cast as floats and missing values replaced with Not A Number
    # The below case will take care of instances where the whole series is missing or if it is an array or
    # some other object we don't know how to fill.
    if is_array:
        unpacked = [msgpack.unpackb(x) for x in data_slice]
        no_nones = filter(None, unpacked)
        # Get the maximum sized array using np
        if len(no_nones) > 0:
            shapes = [np.array(x).shape for x in no_nones]
            max_len = max((len(x) for x in shapes))
            shapes = filter(lambda x: len(x) == max_len, shapes)
            max_shape = max(shapes)
            shp = tuple([len(unpacked)] + list(max_shape))
            data_slice = np.empty(shp, dtype=value_encoding)
            data_slice.fill(fill_value)
            try:
                fix_data_arrays(data_slice, unpacked)
            except Exception:
                log.exception("Error filling arrays with data for parameter %s replacing with fill values", name)
                data_slice.fill(fill_value)
        else:
            data_slice = np.array([[] for _ in unpacked], dtype=value_encoding)
    if data_slice.dtype == 'object' and not is_array:
        nones = np.equal(data_slice, None)
        if np.any(nones):
            if fill_value is not None:
                data_slice[nones] = fill_value
                data_slice = data_slice.astype(value_encoding)
            else:
                log.warn("No fill value for param %s", name)
                # If there are nones either fill with specific value for ints, floats, string, or throw an error
                if value_encoding in ['int', 'uint8', 'uint16', 'uint32', 'uint64', 'int8', 'int16', 'int32', 'int64']:
                    data_slice[nones] = -999999999
                    data_slice = data_slice.astype('int64')
                elif value_encoding in ['float16', 'float32', 'float64', 'float96']:
                    data_slice[nones] = np.nan
                    data_slice = data_slice.astype('float64')
                elif value_encoding == 'string':
                    data_slice[nones] = ''
                    data_slice = data_slice.astype('str')
                else:
                    raise StreamEngineException('Do not know how to fill for data type %s', value_encoding)

    # otherwise if the returned data is a float we need to check and make sure it is not supposed to be an int
    elif data_slice.dtype == 'float64':
        # Int's are upcast to floats if there is a missing value.
        if value_encoding in ['int', 'uint8', 'uint16', 'uint32', 'uint64', 'int8', 'int16', 'int32', 'int64']:
            # We had a missing value because it was upcast
            indexes = np.where(np.isnan(data_slice))
            if len(indexes) > 0:
                if fill_value is not None:
                    data_slice[indexes] = fill_value
                    data_slice = data_slice.astype(value_encoding)
                else:
                    log.warn("No fill value for param %s", name)
                    data_slice[indexes] = -999999999
                    data_slice = data_slice.astype('int64')

    # Pandas also treats strings as objects.  NetCDF doesn't
    # like objects.  So convert objects to strings.
    if data_slice.dtype == object:
        try:
            data_slice = data_slice.astype(value_encoding)
        except ValueError as e:
            log.error('Unable to convert %s to value type (%s) (may be caused by jagged arrays): %s',
                      name, value_encoding, e)
    return data_slice


def compile_datasets(datasets):
    """
    Given a list of datasets. Possibly containing None. Return a single
    dataset with unique indexes and sorted by the 'time' parameter
    :param datasets: :return:
    """
    # filter out the Nones
    datasets = filter(None, datasets)
    if len(datasets) == 0:
        return None
    datasets.sort(key=lambda val: val['time'].values[0])
    # now determine if they are in order or not..
    idx = 0
    for ds in datasets:
        # Determine if the max and the min are all in order
        new_index = [i for i in range(idx, idx + len(ds['index']))]
        ds['index'] = new_index
        idx = new_index[-1] + 1
    dataset = xray.concat(datasets, dim='obs')
    sorted_idx = dataset.time.argsort()
    dataset = dataset.reindex({'obs': sorted_idx})
    return dataset
