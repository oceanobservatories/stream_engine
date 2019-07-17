import logging

import os
import numpy as np

from engine import app
from util.xarray_overrides import xr
from ooi_data.postgres.model import Stream, Parameter
from util.common import MissingDataException, ntp_to_datestring, log_timing


GPS_STREAM_ID = app.config.get('GPS_STREAM_ID')
LATITUDE_PARAM_ID = app.config.get('LATITUDE_PARAM_ID')
LONGITUDE_PARAM_ID = app.config.get('LONGITUDE_PARAM_ID')
FILL_VALUES = app.config['FILL_VALUES']
NETCDF_UNLIMITED_DIMS = app.config.get('NETCDF_UNLIMITED_DIMS')


log = logging.getLogger(__name__)


def make_encoding(ds):
    encoding = {}
    compress = True
    comp_level = app.config.get('HDF5_COMP_LEVEL', 1)
    if comp_level <= 0:
        compress = False
    chunksize = app.config.get('NETCDF_CHUNKSIZES')

    for k in ds.data_vars:
        values = ds[k].values
        shape = values.shape

        encoding[k] = {'zlib': compress, 'complevel': comp_level}

        if 0 not in shape:
            if values.dtype.kind == 'O':
                values = values.astype('str')

            if values.dtype.kind == 'S':
                size = values.dtype.itemsize
                if size > 1:
                    shape = shape + (size,)

            dim0 = min(shape[0], chunksize)
            shape = (dim0,) + shape[1:]
            encoding[k]['chunksizes'] = shape
    return encoding


def add_dynamic_attributes(ds):
    if not ds.keys():
        raise MissingDataException("No data present in dataset")

    time_data = ds['time'].values
    # Do a final update to insert the time_coverages, and geospatial latitude/longitude
    ds.attrs['time_coverage_start'] = ntp_to_datestring(time_data[0])
    ds.attrs['time_coverage_end'] = ntp_to_datestring(time_data[-1])
    # Take an estimate of the number of seconds between values in the data range.
    if time_data.size > 0:
        total_time = time_data[-1] - time_data[0]
        hz = total_time / float(time_data.size)
        ds.attrs['time_coverage_resolution'] = 'P{:.2f}S'.format(hz)
        diff = np.diff(time_data)
    else:
        ds.attrs['time_coverage_resolution'] = 'P0S'

    if 'lat' in ds:
        ds.attrs['geospatial_lat_min'] = min(ds.variables['lat'].values)
        ds.attrs['geospatial_lat_max'] = max(ds.variables['lat'].values)
        ds.attrs['geospatial_lat_units'] = 'degrees_north'
        ds.attrs['geospatial_lat_resolution'] = app.config["GEOSPATIAL_LAT_LON_RES"]
    if 'lon' in ds:
        ds.attrs['geospatial_lon_min'] = min(ds.variables['lon'].values)
        ds.attrs['geospatial_lon_max'] = max(ds.variables['lon'].values)
        ds.attrs['geospatial_lon_units'] = 'degrees_east'
        ds.attrs['geospatial_lon_resolution'] = app.config["GEOSPATIAL_LAT_LON_RES"]

    ds.attrs['geospatial_vertical_units'] = app.config["Z_DEFAULT_UNITS"]
    ds.attrs['geospatial_vertical_resolution'] = app.config['Z_RESOLUTION']
    ds.attrs['geospatial_vertical_positive'] = app.config['Z_POSITIVE']


def replace_fixed_lat_lon(ds, stream_key):
    if stream_key is None or stream_key.is_mobile:
        return ds

    if 'lat' in ds and 'lon' in ds:
        ds.attrs['lat'] = ds.variables['lat'].values[0]
        ds.attrs['lon'] = ds.variables['lon'].values[0]
        ds = ds.drop('lat').drop('lon')

    return ds


def force_data_array_type(data_array, orig_data_type, new_data_type):
    data_array.attrs['original_dtype'] = str(orig_data_type)
    new_data_array = data_array.astype(new_data_type)
    new_data_array.attrs = data_array.attrs
    new_data_array.name = data_array.name
    return new_data_array


def ensure_no_int64(ds):
    for each in ds:
        if ds[each].dtype == 'int64':
            ds[each] = ds[each].astype('int32')


def rename_glider_lat_lon(stream_key, dataset):
    """
    Rename INTERPOLATED glider GPS lat/lon values to lat/lon
    """
    if stream_key.is_glider:
        gps_stream = Stream.query.get(GPS_STREAM_ID)
        lat_param = Parameter.query.get(LATITUDE_PARAM_ID)
        lon_param = Parameter.query.get(LONGITUDE_PARAM_ID)
        lat_name = '-'.join((gps_stream.name, lat_param.name))
        lon_name = '-'.join((gps_stream.name, lon_param.name))
        if lat_name in dataset and lon_name in dataset:
            return dataset.rename({lat_name: 'lat', lon_name: 'lon'})
    return dataset


def prep_classic(ds):
    for data_array_name in ds.data_vars:
        data_array = ds.get(data_array_name)
        data_type = data_array.dtype
        if data_type in [np.int64, np.uint32, np.uint64]:
            ds[data_array_name] = force_data_array_type(data_array, data_type, np.str)
        elif data_type == np.uint16:
            ds[data_array_name] = force_data_array_type(data_array, data_type, np.int32)
        elif data_type == np.uint8:
            ds[data_array_name] = force_data_array_type(data_array, data_type, np.int16)


def write_netcdf(ds, file_path, classic=False):
    if classic:
        prep_classic(ds)
        ds.to_netcdf(path=file_path, format="NETCDF4_CLASSIC")
    else:
        ensure_no_int64(ds)
        encoding = make_encoding(ds)
        ds.to_netcdf(file_path, encoding=encoding, unlimited_dims=NETCDF_UNLIMITED_DIMS)


def max_shape(shape1, shape2):
    if shape1 == shape2:
        return shape1

    # this should never happen
    # but, if it does, pad out shapes to match
    while len(shape1) < len(shape2):
        shape1 = shape1 + (0, )
    while len(shape2) < len(shape1):
        shape2 = shape2 + (0, )

    result = []
    for dim1, dim2 in zip(shape1, shape2):
        result.append(max(dim1, dim2))
    return tuple(result)


def max_dtype(dtype1, dtype2):
    """
    Given two dtypes, return a dtype which can hold them both
    Note that it is possible that this dtype is different than both input dtypes!
    :param dtype1:
    :param dtype2:
    :return:
    """
    # Both dtypes are the same, return one
    if dtype1 == dtype2:
        return dtype1

    kind1 = dtype1.kind
    kind2 = dtype2.kind
    f32 = np.dtype(np.float32)
    f64 = np.dtype(np.float64)

    # Both dtypes are of the same kind but differing sizes
    # return the larger of the two
    if kind1 == kind2:
        if dtype1.itemsize > dtype2.itemsize:
            return dtype1
        else:
            return dtype2

    # Dtypes differ in kind
    # special case string, allow it to override all other dtypes
    # special case uint64
    if any((kind1 == 'S', kind2 == 'S')):
        return np.dtype(np.str)

    # possible types below this point (bool, float, integer, unsigned integer)
    # bool fits into any other data type, the other one wins
    if kind1 == 'b':
        return dtype2
    if kind2 == 'b':
        return dtype1

    # possible types below this point (float, integer, unsigned integer)
    # if given float, float must win, promote to smallest float which can accurately represent both sets of values
    if 'f' in {kind1, kind2}:
        try:
            np.zeros(1).astype(dtype1).astype(f32, casting='safe')
            np.zeros(1).astype(dtype2).astype(f32, casting='safe')
            return f32
        except TypeError:
            return f64

    # possible types below this point (integer, unsigned integer)
    # one signed, one unsigned integer
    # special case, one of these is unsigned int64, promote to float64
    if any((dtype1.name == 'uint64', dtype2.name == 'uint64')):
        return f64

    # find the smallest signed integer to fit both cases
    if kind1 == 'u' and kind2 == 'i':
        if dtype1.itemsize == 8:
            return np.dtype(np.str)
        else:
            return max_dtype(np.dtype('i%d' % (dtype1.itemsize * 2)), dtype2)

    if kind2 == 'u' and kind1 == 'i':
        if dtype2.itemsize == 8:
            return np.dtype(np.str)
        else:
            return max_dtype(np.dtype('i%d' % (dtype2.itemsize * 2)), dtype1)

    raise Exception('i missed something')


def max_dimension_names(names1, names2):
    if names1 == names2:
        return names1

    if len(names1) >= len(names2):
        return names1

    return names2


@log_timing(log)
def analyze_datasets(dir_path, files, request_id=None):
    parameters = {}
    for f in files:
        path = os.path.join(dir_path, f)
        with xr.open_dataset(path, decode_times=False, mask_and_scale=False, decode_coords=False) as ds:
            for var in ds.data_vars:
                # most variables are dimensioned on obs, drop the obs dimension
                shape = ds[var].shape[1:]
                dims = ds[var].dims[1:]
                dtype = ds[var].dtype

                # handle variables not dimensioned on obs (13025 AC2)
                if 'obs' not in ds[var].dims:
                    shape = ds[var].shape
                    dims = ds[var].dims

                if dtype.kind == 'S':
                    fill = ds[var].attrs.get('_FillValue', FILL_VALUES.get('string'))
                else:
                    fill = ds[var].attrs.get('_FillValue', FILL_VALUES.get(dtype.name))

                if var not in parameters:
                    parameters[var] = {
                        'shape': shape,
                        'dtype': dtype,
                        'dims': dims,
                        'fill': fill
                    }
                else:
                    parameters[var] = {
                        'shape': max_shape(shape, parameters[var]['shape']),
                        'dtype': max_dtype(dtype, parameters[var]['dtype']),
                        'dims': max_dimension_names(dims, parameters[var]['dims']),
                        'fill': fill
                    }
    return parameters
