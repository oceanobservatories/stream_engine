import json
import logging
import os
import tempfile
import zipfile

import numpy

from engine import app
from util.common import log_timing


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
        for stream_key, dataset in self.stream_request.datasets.iteritems():
            for deployment, ds in dataset.groupby('deployment'):
                file_path = '%s/deployment%04d_%s.nc' % (base_path, deployment, stream_key.as_dashed_refdes())
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
                if data_type == numpy.int64 or data_type == numpy.uint32 or data_type == numpy.uint64:
                    ds[data_array_name] = self.convert_data_array(data_array, data_type, numpy.str)
                elif data_type == numpy.uint16:
                    ds[data_array_name] = self.convert_data_array(data_array, data_type, numpy.int32)
            ds.to_netcdf(path=file_path, format="NETCDF4_CLASSIC")
        else:
            ds.to_netcdf(file_path)

    @log_timing(log)
    def convert_data_array(self, data_array, orig_data_type, dest_data_type):
        data_array.attrs['original_dtype'] = str(orig_data_type)
        new_data_array = data_array.astype(dest_data_type)
        new_data_array.attrs = data_array.attrs
        new_data_array.name = data_array.name
        return new_data_array
