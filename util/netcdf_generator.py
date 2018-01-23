import json
import logging
import os
import shutil
import tempfile
import zipfile

from engine import app
from util.common import log_timing, WriteErrorException
from util.netcdf_utils import rename_glider_lat_lon, add_dynamic_attributes, write_netcdf
from util.datamodel import find_depth_variable

log = logging.getLogger(__name__)

# get pressure parameters (9328)
PRESSURE_DPI = app.config.get('PRESSURE_DPI')
INT_PRESSURE_NAME = app.config.get('INT_PRESSURE_NAME')

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
            try:
                os.makedirs(base_path)
            except OSError:
                if not os.path.isdir(base_path):
                    raise WriteErrorException('Unable to create local output directory: %s' % self.disk_path)

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

    def _filter_params(self, ds, params):
        missing_params = []
        params_to_filter = []
        # aggregation logic assumes the presence of a 'time' parameter, so do not allow it to get removed
        default_params = ['time', 'deployment', 'id', 'lat', 'lon', 'quality_flag']

        for param in params:
            # look for param in both data_vars and coords (13025 AC2)
            if param not in ds.data_vars and param not in ds.coords:
                missing_params.append(param)
            else:
                params_to_filter.append(param)

        # only warn about missing parameters for directly requested data stream
        if missing_params and 'stream' in ds and ds.stream == self.stream_request.stream_key.stream_name:
            log.warning('one or more selected parameters (%s) not found in the dataset', missing_params)

        if params_to_filter:
            log.debug('filtering parameters: %s', params_to_filter)
        else:
            log.warning('no parameters to remove')
            return ds

        params_to_filter.extend(default_params)
        if self.stream_request.include_provenance:
            params_to_filter.append('provenance')
        if self.stream_request.include_annotations:
            params_to_filter.append('annotations')

        for key in ds.data_vars:
            if key not in params_to_filter:
                ds = ds.drop(key)
        return ds

    def _setup_coordinate_variables(self, ds):
        """
        Find the depth variable in the dataset and set assocciated coordinate attributes (10745)
        :param ds: the dataset on which to operate
        :return: the dataset with the coordinate variables and associated attributes setup
        """
        # find the depth_variable (10745 AC1)
        depth_variable = find_depth_variable(ds.data_vars)
        coordinates_key = 'coordinates'
        coordinate_variables = "time lat lon"
        if depth_variable:
            coordinate_variables += " " + depth_variable
        for var in ds.data_vars:
            # coordinate variable shouldn't have coordinate attribute (10745 AC3)
            # only scientific variables should have coordinate attribute (10745 AC2)
            if var not in coordinate_variables \
                    and var not in app.config.get('NETCDF_NONSCI_VARIABLES',[]):
                ds[var].attrs[coordinates_key] = coordinate_variables
            elif coordinates_key in ds[var].attrs:
                del ds[var].attrs[coordinates_key]
            # make sure coordinate variables have axis defined (10745 AC4)
            if var == 'lat':
                ds[var].attrs['axis'] = 'Y'
            elif var == 'lon':
                ds[var].attrs['axis'] = 'X'
            elif var == depth_variable:
                ds[var].attrs['axis'] = 'Z'

        return ds


    def _create_files(self, base_path):
        file_paths = []
        for stream_key, stream_dataset in self.stream_request.datasets.iteritems():
            for deployment, ds in stream_dataset.datasets.iteritems():
                add_dynamic_attributes(ds)
                start = ds.attrs['time_coverage_start'].translate(None, '-:')
                end = ds.attrs['time_coverage_end'].translate(None, '-:')
                
                # provenance types will be written to JSON files
                if self.stream_request.include_provenance:
                    prov_fname = 'deployment%04d_%s_provenance_%s-%s.json' % (deployment,
                                                                              stream_key.as_dashed_refdes(), start, end)
                    prov_json = os.path.join(base_path, prov_fname)
                    file_paths.append(prov_json)
                    stream_dataset.provenance_metadata.dump_json(prov_json)
                
                # annotation data will be written to JSON files
                if self.stream_request.include_annotations:
                    anno_fname = 'deployment%04d_%s_annotations_%s-%s.json' % (deployment,
                                                                               stream_key.as_dashed_refdes(),
                                                                               start, end)
                    anno_json = os.path.join(base_path, anno_fname)
                    file_paths.append(anno_json)
                    stream_dataset.annotation_store.dump_json(anno_json)
                
                file_name = 'deployment%04d_%s_%s-%s.nc' % (deployment, stream_key.as_dashed_refdes(), start, end)
                file_path = os.path.join(base_path, file_name)
                ds = rename_glider_lat_lon(stream_key, ds)

                # include all directly requested_parameters
                params_to_include = [p.name for p in self.stream_request.requested_parameters]

                # also include any indirectly derived pressure parameter (9328)
                pressure_params = [(sk, param) for sk in self.stream_request.external_includes
                                   for param in self.stream_request.external_includes[sk]
                                   if param.data_product_identifier == PRESSURE_DPI]
                if pressure_params:
                    params_to_include.append(INT_PRESSURE_NAME)

                # include all external parameters associated with the directly requested parameters (12886)
                for external_stream_key in self.stream_request.external_includes:
                    for parameter in self.stream_request.external_includes[external_stream_key]:
                        params_to_include.append(parameter.name)
                        long_parameter_name = external_stream_key.stream_name+"-"+parameter.name
                        if long_parameter_name in ds:
                            # rename the parameter without the stream_name prefix (12544 AC1)
                            ds = ds.rename({long_parameter_name : parameter.name})
                            # record the instrument and stream (12544 AC2)
                            ds[parameter.name].attrs['instrument'] = external_stream_key.as_three_part_refdes()
                            ds[parameter.name].attrs['stream'] = external_stream_key.stream_name

                # associated variables with their contributors (12544 AC3)
                for requested_parameter in self.stream_request.requested_parameters:
                    if requested_parameter.needs and requested_parameter.name in ds:
                        for k, need_list in requested_parameter.needs:
                            for need in need_list:
                                if need.name in params_to_include:
                                    if 'ancillary_variables' in ds[requested_parameter.name].attrs:
                                        ds[requested_parameter.name].attrs['ancillary_variables'] += "," + need.name
                                    else:
                                        ds[requested_parameter.name].attrs['ancillary_variables'] = need.name
                                    break

                # setup coordinate variables (10745)
                ds = self._setup_coordinate_variables(ds)

                if params_to_include:
                    ds = self._filter_params(ds, params_to_include)
                write_netcdf(ds, file_path, classic=self.classic)
                file_paths.append(file_path)
        return file_paths
