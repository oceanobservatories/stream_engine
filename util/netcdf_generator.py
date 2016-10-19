import json
import logging
import os
import shutil
import tempfile
import zipfile

from engine import app
from util.common import log_timing
from util.netcdf_utils import rename_glider_lat_lon, add_dynamic_attributes, write_netcdf

log = logging.getLogger(__name__)


class NetcdfGenerator(object):
    def __init__(self, stream_request, classic, disk_path=None):
        self.stream_request = stream_request
        self.request_id = stream_request.request_id
        self.classic = classic
        self.disk_path = disk_path

    def write(self):
        if self.disk_path is not None:
            return self._create_raw_files()
        else:
            return self._create_zip()

    @log_timing(log)
    def _create_raw_files(self):
        base_path = os.path.join(app.config['LOCAL_ASYNC_DIR'], self.disk_path)
        # ensure the directory structure is there
        if not os.path.isdir(base_path):
            os.makedirs(base_path)
        file_paths = self._create_files(base_path)
        # build json return
        return json.dumps({'code': 200, 'message': str(file_paths)}, indent=2)

    @log_timing(log)
    def _create_zip(self):
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
                add_dynamic_attributes(ds)
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
                ds = rename_glider_lat_lon(stream_key, ds)
                write_netcdf(ds, file_path, classic=self.classic)
                file_paths.append(file_path)
        return file_paths
