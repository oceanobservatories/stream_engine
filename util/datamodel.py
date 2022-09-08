"""
This module is for classes and functions related to the data structures
stream_engine uses to model science data.  Data is currently being backed by xray Datasets.
"""
import datetime
import logging

import msgpack
import numpy as np
import pandas as pd
import xarray as xr

from common import StreamEngineException
from multi_concat import multi_concat
from engine import app

__author__ = 'Stephen Zakrewsky'

log = logging.getLogger(__name__)


FILL_VALUES = app.config['FILL_VALUES']
LAT_FILL = app.config['LAT_FILL']
LON_FILL = app.config['LON_FILL']
DEPTH_FILL = app.config['DEPTH_FILL']


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


def create_empty_dataset(stream_key, request_id):
    attrs = _get_ds_attrs(stream_key, request_id)
    return xr.Dataset(attrs=attrs)


def add_location_data(ds, lat, lon):
    lat = lat if lat else LAT_FILL
    lon = lon if lon else LON_FILL
    lat_array = np.full_like(ds.time.values, lat)
    lon_array = np.full_like(ds.time.values, lon)

    ds['lat'] = ('obs', lat_array, {'axis': 'Y', 'units': 'degrees_north', 'standard_name': 'latitude'})
    ds['lon'] = ('obs', lon_array, {'axis': 'X', 'units': 'degrees_east', 'standard_name': 'longitude'})


def to_xray_dataset(cols, data, stream_key, request_uuid, san=False, keep_exclusions=False):
    """
    Make an xray dataset from the raw cassandra data
    """
    if not data:
        return None

    attrs = _get_ds_attrs(stream_key, request_uuid)
    params = {p.name: p for p in stream_key.stream.parameters if not p.is_function}

    if san:
        attrs['title'] = '{:s} for {:s}'.format("SAN offloaded netCDF", stream_key.as_dashed_refdes())
        attrs['history'] = '{:s} {:s}'.format(datetime.datetime.utcnow().isoformat(), 'generated netcdf for SAN')

    datasets = {}
    dataframe = pd.DataFrame(data=data, columns=cols)

    for deployment, dataframe_group in dataframe.groupby('deployment'):
        dataset = xr.Dataset(attrs=attrs)
        for column in dataframe.columns:
            if not keep_exclusions and column in app.config['INTERNAL_OUTPUT_EXCLUDE_LIST']:
                continue

            param = params.get(column)

            if param:
                encoding = param.value_encoding
                param_dims = [dim.value for dim in param.dimensions]
                fill_val = _get_fill_value(param)
                is_array = param.parameter_type == 'array<quantity>'
            elif column == 'bin':
                encoding = 'uint64'
                fill_val = np.array('0').astype(encoding)
                is_array = False
                param_dims = []
            else:
                encoding = 'str'
                fill_val = ''
                is_array = False
                param_dims = []

            if column in app.config['INTERNAL_OUTPUT_MAPPING']:
                encoding = app.config['INTERNAL_OUTPUT_MAPPING'][column]

            data = _replace_values(dataframe_group[column].values, encoding, fill_val, is_array, column)
            data = _force_dtype(data, encoding)
            if data is None:
                log.error('<%s> Unable to encode data NAME: %s FROM: %s TO: %s, dropping from dataset',
                          request_uuid, column, data.dtype, encoding)

            # Fix up the dimensions for possible multi-d objects
            dims = ['obs']
            if len(data.shape) > 1:
                if param_dims:
                    dims += param_dims
                else:
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

            coord_columns = 'time lat lon'
            if column not in coord_columns:
                array_attrs['coordinates'] = coord_columns

            # Override the fill value supplied by preload if necessary
            array_attrs['_FillValue'] = fill_val

            dataset.update({column: xr.DataArray(data, dims=dims, attrs=array_attrs)})
        datasets[deployment] = dataset
    return datasets


def _force_dtype(data_slice, value_encoding):
    try:
        return data_slice.astype(value_encoding)
    except ValueError:
        return None


def _replace_values(data_slice, value_encoding, fill_value, is_array, name):
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
        if no_nones:
            shapes = [np.array(x).shape for x in no_nones]
            max_len = max((len(x) for x in shapes))
            shapes = filter(lambda x: len(x) == max_len, shapes)
            max_shape = max(shapes)
            shp = tuple([len(unpacked)] + list(max_shape))
            # temporarily encode strings as object to avoid dealing with length
            # then cast as string below
            if value_encoding == 'string':
                data_slice = np.empty(shp, dtype='object')
            else:
                data_slice = np.empty(shp, dtype=value_encoding)
            data_slice.fill(fill_value)
            try:
                _fix_data_arrays(data_slice, unpacked)
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
                raise StreamEngineException('Do not know how to fill for data type %s', value_encoding)

    # otherwise if the returned data is a float we need to check and make sure it is not supposed to be an int
    elif data_slice.dtype == 'float64':
        # Int's are upcast to floats if there is a missing value.
        if value_encoding in ['int', 'uint8', 'uint16', 'uint32', 'uint64', 'int8', 'int16', 'int32', 'int64']:
            # We had a missing value because it was upcast
            indexes = np.where(np.isnan(data_slice))
            if indexes:
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


def _fix_data_arrays(data, unpacked):
    if unpacked is None:
        return
    if len(data.shape) == 1:
        for idx, _ in enumerate(unpacked):
            if idx < len(data):
                # Don't overwrite the fill value when data is None
                if unpacked[idx] is not None:
                    data[idx] = unpacked[idx]
    else:
        if isinstance(unpacked, list):
            for data_sub, unpacked_sub in zip(data, unpacked):
                _fix_data_arrays(data_sub, unpacked_sub)


def compile_datasets(datasets):
    """
    Given a list of datasets. Possibly containing None. Return a single
    dataset with unique indexes and sorted by the 'time' parameter
    :param datasets:
    :return:
    """
    # filter out the Nones
    datasets = filter(None, datasets)
    if not datasets:
        return None

    try:
        # run an updated version of xarray.concat which handles mismatched coordinates and non-obs dimensions
        dataset = multi_concat(datasets, dim='obs')
    except ValueError:
        # concatenation failed to run normally and the ValueError suggests an index might be at fault
        # for each index except 'obs', set the index values to the sequence 0, 1, 2, ...
        for dataset in datasets:
            non_obs_indices = [key for key in dataset.indexes if key != u'obs']
            for key in non_obs_indices:
                dataset[key] = (key, np.arange(dataset.dims[key]), dataset[key].attrs)
        # with the indices reset, try the concatenation again
        dataset = multi_concat(datasets, dim='obs')

    # recreate the obs dimension
    dataset['obs'] = np.arange(dataset.obs.size)
    # sort the dataset by time
    sorted_idx = dataset.time.argsort()
    dataset = dataset.reindex({'obs': sorted_idx})
    # recreate the obs dimension again to ensure it is sequential
    dataset['obs'] = np.arange(dataset.obs.size)
    return dataset


def _get_fill_value(param):
    try:
        return np.array(param.fill_value).astype(param.value_encoding)
    except (ValueError, TypeError) as e:
        log.error('Invalid fill value specified for parameter: %r (%r)', param, e)
        return FILL_VALUES.get(param.value_encoding)
