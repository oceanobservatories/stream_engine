import json
import logging
import os
import tempfile
import zipfile
import shutil

import numpy as np

from engine import app
from preload_database.model.preload import Stream, Parameter
from util.common import log_timing, MissingDataException, ntp_to_datestring
from xarray.backends import api
from netcdf_store import NetCDF4DataStoreUnlimited, UNLIMITED_DIMS

NETCDF_ENGINE = 'netcdf4_unlimited'
GPS_STREAM_ID = app.config.get('GPS_STREAM_ID')
LATITUDE_PARAM_ID = app.config.get('LATITUDE_PARAM_ID')
LONGITUDE_PARAM_ID = app.config.get('LONGITUDE_PARAM_ID')

api.WRITEABLE_STORES[NETCDF_ENGINE] = NetCDF4DataStoreUnlimited

log = logging.getLogger(__name__)


class NetcdfGenerator(object):
    def __init__(self, stream_request, classic, disk_path=None):
        self.stream_request = stream_request
        self.request_id = stream_request.request_id
        self.classic = classic
        self.disk_path = disk_path

    def write(self):
        if self.disk_path is not None:
            return self.create_raw_files()
        else:
            return self.create_zip()

    @log_timing(log)
    def create_raw_files(self):
        base_path = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'], self.disk_path)
        # ensure the directory structure is there
        if not os.path.isdir(base_path):
            os.makedirs(base_path)
        file_paths = self._create_files(base_path)
        # build json return
        return json.dumps({'code': 200, 'message': str(file_paths)}, indent=2)

    @log_timing(log)
    def create_zip(self):
        with tempfile.NamedTemporaryFile() as tzf:
            with zipfile.ZipFile(tzf.name, 'w') as zf:
                temp_dir = tempfile.mkdtemp()
                file_paths = self._create_files(temp_dir)
                for file_path in file_paths:
                    zf.write(file_path, os.path.basename(file_path))
                shutil.rmtree(temp_dir)
            return tzf.read()

    def _create_files(self, base_path):
        file_paths = []
        for stream_key, stream_dataset in self.stream_request.datasets.iteritems():
            for deployment, ds in stream_dataset.datasets.iteritems():
                self._add_dynamic_attributes(ds)
                start = ds.attrs['time_coverage_start'].translate(None, '-:')
                end = ds.attrs['time_coverage_end'].translate(None, '-:')
                # provenance types will be written to JSON files
                prov_fname = 'deployment%04d_%s_provenance_%s-%s.json' % (deployment,
                                   stream_key.as_dashed_refdes(), start, end)
                prov_json = os.path.join(base_path, prov_fname)
                file_paths.append(prov_json)
                stream_dataset.provenance_metadata.dump_json(prov_json)
                file_name = 'deployment%04d_%s_%s-%s.nc' % (deployment, stream_key.as_dashed_refdes(), start, end)
                file_path = os.path.join(base_path, file_name)
                ds = self._rename_glider_lat_lon(stream_key, ds)
                self.to_netcdf(ds, file_path)
                file_paths.append(file_path)
        return file_paths

    @staticmethod
    def _rename_glider_lat_lon(stream_key, dataset):
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

    @log_timing(log)
    def to_netcdf(self, ds, file_path):
        if self.classic:
            for data_array_name in ds.data_vars:
                data_array = ds.get(data_array_name)
                data_type = data_array.dtype
                if data_type in [np.int64, np.uint32, np.uint64]:
                    ds[data_array_name] = self.convert_data_array(data_array, data_type, np.str)
                elif data_type == np.uint16:
                    ds[data_array_name] = self.convert_data_array(data_array, data_type, np.int32)
            ds.to_netcdf(path=file_path, format="NETCDF4_CLASSIC")
        else:
            self._ensure_no_int64(ds)
            encoding = self.make_encoding(ds)
            ds.to_netcdf(file_path, engine = NETCDF_ENGINE, encoding = encoding)

    def make_encoding(self, ds):
        encoding = {}
        compr = True
        comp_level = app.config.get('HDF5_COMP_LEVEL', 1)
        if comp_level <= 0:
            compr = False
        chunksize = app.config.get('NETCDF_CHUNKSIZES')
        udim = app.config.get('NETCDF_UNLIMITED_DIMS')

        for k in ds.data_vars:
            values = ds[k].values
            shape = values.shape

            if 0 in shape:
                encoding[k] = {'zlib': compr, 'complevel': comp_level, UNLIMITED_DIMS: udim}
            else:
                if values.dtype.kind == 'O':
                    values = values.astype('str')

                if values.dtype.kind == 'S':
                    size = values.dtype.itemsize
                    if size > 1:
                        shape = shape + (size,)

                dim0 = min(shape[0], chunksize)
                shape = (dim0,) + shape[1:]
                encoding[k] = {'zlib': compr, 'chunksizes': shape, 'complevel': comp_level, UNLIMITED_DIMS: udim}
        return encoding

    def _ensure_no_int64(self, ds):
        for each in ds:
            if ds[each].dtype == 'int64':
                ds[each] = ds[each].astype('int32')
                log.info('<%s> Forced %s from int64 to int32', self.request_id, each)

    @log_timing(log)
    def convert_data_array(self, data_array, orig_data_type, dest_data_type):
        data_array.attrs['original_dtype'] = str(orig_data_type)
        new_data_array = data_array.astype(dest_data_type)
        new_data_array.attrs = data_array.attrs
        new_data_array.name = data_array.name
        return new_data_array

    def _add_dynamic_attributes(self, ds):
        if not ds.keys():
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
