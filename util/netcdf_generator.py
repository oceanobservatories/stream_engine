import json
import logging
import os
import tempfile
import zipfile
from time import sleep

import numpy as np

from engine import app
from util.common import log_timing, MissingDataException, ntp_to_datestring

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
        file_paths = []
        base_path = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'], self.disk_path)
        # ensure the directory structure is there
        if not os.path.isdir(base_path):
            os.makedirs(base_path)
        for stream_key, stream_dataset in self.stream_request.datasets.iteritems():
            for deployment, ds in stream_dataset.datasets.iteritems():
                self._add_dynamic_attributes(ds, stream_key, deployment)
                start = ds.attrs['time_coverage_start'].translate(None, '-:')
                end = ds.attrs['time_coverage_end'].translate(None, '-:')
                # provenance types will be written to JSON files
                prov_json = '%s/deployment%04d_%s_provenance_%s-%s.json' % (base_path, 
                         deployment, stream_key.as_dashed_refdes(), start, end)
                stream_dataset.provenance_metadata.dump_json(prov_json)
                file_path = '%s/deployment%04d_%s_%s-%s.nc' % (base_path, 
                         deployment, stream_key.as_dashed_refdes(), start, end)
                self.to_netcdf(ds, file_path)
                file_paths.append(file_path)
        # build json return
        return json.dumps({'code': 200, 'message': str(file_paths)}, indent=2)

    @log_timing(log)
    def create_zip(self):
        with tempfile.NamedTemporaryFile() as tzf:
            with zipfile.ZipFile(tzf.name, 'w') as zf:
                self.write_to_zipfile(zf)
            return tzf.read()

    @log_timing(log)
    def write_to_zipfile(self, zf):
        for stream_key, dataset in self.stream_request.datasets.iteritems():
            for deployment, ds in dataset.groupby('deployment'):
                with tempfile.NamedTemporaryFile() as tf:
                    self.to_netcdf(ds, tf.name)
                    zf.write(tf.name, 'deployment%04d_%s.nc' % (deployment, stream_key.as_dashed_refdes(),))

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
            compr = True
            comp_level = app.config.get('HDF5_COMP_LEVEL', 1)
            if comp_level <= 0:
                compr = False
            self._ensure_no_int64(ds)
            ds.to_netcdf(file_path, encoding={k: {'zlib': compr, 'complevel': comp_level} for k in ds})

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

    def _add_dynamic_attributes(self, ds, stream_key, deployment):
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

        location_vals = {}
        for loc in self.stream_request.location_information.get(stream_key.as_three_part_refdes(), []):
            if loc['deployment'] == deployment:
                location_vals = loc
                break

        if 'location_name' in location_vals:
            ds.attrs['location_name'] = str(location_vals['location_name'])

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

        depth_units = str(location_vals.get('depth_units', app.config["Z_DEFAULT_UNITS"]))
        ds.attrs['geospatial_vertical_units'] = depth_units
        ds.attrs['geospatial_vertical_resolution'] = app.config['Z_RESOLUTION']
        ds.attrs['geospatial_vertical_positive'] = app.config['Z_POSITIVE']

    def _add_provenance(self, ds, provenance_metadata):
        provenance_metadata.add_to_dataset(ds, self.stream_request.request_id)
