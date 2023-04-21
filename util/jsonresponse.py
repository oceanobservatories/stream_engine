import os
import json
import logging
from collections import OrderedDict
from datetime import datetime

import numpy as np

from util.common import log_timing, ntp_to_short_iso_datestring, get_annotation_filename, WriteErrorException
from engine import app
from ooi_data.postgres.model import Parameter, Stream

# QC parameter identification patterns
from util.common import QC_SUFFIXES

__author__ = 'Stephen Zakrewsky'


log = logging.getLogger(__name__)


LAT_FILL = app.config.get('LAT_FILL')
LON_FILL = app.config.get('LON_FILL')
PRESSURE_DPI = app.config.get('PRESSURE_DPI')
INT_PRESSURE_NAME = app.config.get('INT_PRESSURE_NAME')


class JsonResponse(object):

    def __init__(self, stream_request):
        self.stream_request = stream_request
        self.request_id = stream_request.request_id

    @log_timing(log)
    def json(self):
        stream_key = self.stream_request.stream_key
        stream_dataset = self.stream_request.datasets[stream_key]
        parameters = self.stream_request.requested_parameters
        external_includes = self.stream_request.external_includes
        data = self._particles(stream_dataset, stream_key, parameters, external_includes)

        prov = anno = None
        if self.stream_request.include_provenance:
            prov = self._provenance(stream_dataset.provenance_metadata, stream_dataset.datasets.keys())
        if self.stream_request.include_annotations:
            anno = self._annotations(self.stream_request.annotation_store)

        if prov or anno:
            out = OrderedDict()
            out['data'] = data
            if prov:
                out.update(prov)
            if anno:
                out.update(anno)
        else:
            out = data

        return json.dumps(out, indent=2, cls=NumpyJSONEncoder)

    @log_timing(log)
    def write_json(self, path):
        file_paths = []
        base_path = os.path.join(app.config['LOCAL_ASYNC_DIR'], path)

        if not os.path.isdir(base_path):
            try:
                os.makedirs(base_path)
            except OSError:
                if not os.path.isdir(base_path):
                    raise WriteErrorException('Unable to create local output directory: %s' % path)

        # annotation data will be written to a JSON file
        if self.stream_request.include_annotations:
            anno_fname = get_annotation_filename(self.stream_request)
            anno_json = os.path.join(base_path, anno_fname)
            file_paths.append(anno_json)
            self.stream_request.annotation_store.dump_json(anno_json)

        stream_key = self.stream_request.stream_key
        stream_dataset = self.stream_request.datasets[stream_key]
        parameters = self.stream_request.requested_parameters
        external_includes = self.stream_request.external_includes
        for deployment, ds in stream_dataset.datasets.iteritems():
            times = ds.time.values
            start = ntp_to_short_iso_datestring(times[0])
            end = ntp_to_short_iso_datestring(times[-1])

            # provenance types will be written to JSON files
            if self.stream_request.include_provenance:
                prov_fname = 'deployment%04d_%s_provenance_%s-%s.json' % (deployment,
                                                                          stream_key.as_dashed_refdes(), start, end)
                prov_json = os.path.join(base_path, prov_fname)
                file_paths.append(prov_json)
                stream_dataset.provenance_metadata.dump_json(prov_json, deployment)

            filename = 'deployment%04d_%s_%s-%s.json' % (deployment, stream_key.as_dashed_refdes(), start, end)
            file_path = os.path.join(base_path, filename)
 
            with open(file_path, 'w') as filehandle:
                data = self._deployment_particles(ds, stream_key, parameters, external_includes)
                json.dump(data, filehandle, indent=2, separators=(',', ': '), cls=NumpyJSONEncoder)
            file_paths.append(file_path)

        return json.dumps({"code": 200, "message": str(file_paths)}, indent=2)

    @log_timing(log)
    def _deployment_particles(self, ds, stream_key, parameters, external_includes):
        particles = []
 
        # extract the underlying numpy arrays from the dataset (indexing into the dataset is expensive)
        data = {}
        for p in ds.data_vars:
            data[p] = ds[p].values

        # Extract the parameter names from the parameter objects
        params = [p.netcdf_name for p in parameters]

        # check if we should include and have pressure data
        if stream_key.is_mobile:
            pressure_params = [(sk, param) for sk in external_includes for param in external_includes[sk]
                               if param.data_product_identifier == PRESSURE_DPI]
            # only need to append pressure name (9328)
            if pressure_params:
                params.append(INT_PRESSURE_NAME)

        # check if we should include and have positional data
        if stream_key.is_glider:
            # get the lat,lon data from the GPS, DR (dead-reckoning) and interpolated fields
            gps_lat_data = data.pop('glider_gps_position-m_gps_lat', None)
            gps_lon_data = data.pop('glider_gps_position-m_gps_lon', None)
            dr_lat_data = data.pop('glider_gps_position-m_lat', None)
            dr_lon_data = data.pop('glider_gps_position-m_lon', None)
            interp_lat_data = data.pop('glider_gps_position-interp_lat', None)
            interp_lon_data = data.pop('glider_gps_position-interp_lon', None)

            if gps_lat_data is not None and gps_lat_data.ndim > 0 and gps_lon_data is not None and \
                    gps_lon_data.ndim > 0:
                data['m_gps_lat'] = gps_lat_data
                data['m_gps_lon'] = gps_lon_data
                params.extend(('m_gps_lat', 'm_gps_lon'))

            if dr_lat_data is not None and dr_lat_data.ndim > 0 and dr_lon_data is not None and dr_lon_data.ndim > 0:
                data['m_lat'] = dr_lat_data
                data['m_lon'] = dr_lon_data
                params.extend(('m_lat', 'm_lon'))

            if interp_lat_data is not None and interp_lat_data.ndim > 0 and interp_lon_data is not None and \
                    interp_lon_data.ndim > 0:
                data['lat'] = interp_lat_data
                data['lon'] = interp_lon_data
                params.extend(('lat', 'lon'))

        # remaining externals
        for sk in external_includes:
            for param in external_includes[sk]:
                name = '-'.join((sk.stream_name, param.netcdf_name))
                if name in data:
                    params.append(name)

        if self.stream_request.include_provenance:
            params.append('provenance')

        # add any QC if it exists
        for param in params:
            for qc_suffix in QC_SUFFIXES:
                qc_key = '%s%s' % (param, qc_suffix)
                if qc_key in data:
                    params.append(qc_key)

        # don't look for dimensional coordinate variables in data (13025 AC2)
        for dim in ds.coords:
            if dim in params:
                params.remove(dim)

        # Warn for any missing parameters
        missing = [p for p in params if p not in data]
        if missing:
            log.warn('<%s> Failed to get data for %r: Not in Dataset', self.request_id, missing)

        params = [p for p in params if p in data]

        for index in xrange(len(ds.time)):
            # Create our particle from the list of parameters
            particle = {}
            for p in params:
                if p in ds and 'obs' not in ds[p].dims:
                    # data has no obs dimension (13025 AC2)
                    particle[p] = data[p]
                else:
                    # data is bound by obs dimension
                    particle[p] = data[p][index]
            particle['pk'] = stream_key.as_dict()
            particle['pk']['time'] = data['time'][index]
            if 'deployment' in data:
                particle['pk']['deployment'] = data['deployment'][index]
            particles.append(particle)
 
        return particles

    @log_timing(log)
    def _particles(self, stream_data, stream_key, parameters, external_includes):
        """
        Convert an xray Dataset into a list of dictionaries, each representing a single point in time
        """
        particles = []

        for deployment in sorted(stream_data.datasets):
            ds = stream_data.datasets[deployment]
            deployment_particles = self._deployment_particles(ds, stream_key, parameters, external_includes)
            particles.extend(deployment_particles)
        return particles

    @staticmethod
    def _provenance(prov_metadata, deployments):
        if prov_metadata is not None:
            return prov_metadata.get_json(deployments)

    @staticmethod
    def _annotations(anno_store):
        if anno_store is not None:
            return {'annotations': anno_store.as_dict_list()}

    @staticmethod
    def _reconstruct(value):
        parts = value.split(' ')
        return {
            'file_name': parts[0],
            'parser_name': parts[1],
            'parser_version': parts[2]
        }


class NumpyJSONEncoder(json.JSONEncoder):
    """
    numpy array indexing will often return numpy scalars, for
    example a = array([0.5]), type(a[0]) will be numpy.float64.
    The problem is that numpy types are not json serializable.
    However, they have a lot of the same methods as ndarrays, so
    for example, tolist() can be called on a numpy scalar or
    numpy ndarray to convert to regular python types.
    """
    def default(self, o):
        if isinstance(o, (np.generic, np.ndarray)):
            return o.tolist()
        if isinstance(o, (Stream, Parameter)):
            return repr(o)
        elif isinstance(o, datetime):
            return str(o)
        else:
            return json.JSONEncoder.default(self, o)
