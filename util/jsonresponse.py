from collections import OrderedDict
import json
import logging
from numbers import Number

import numpy as np
from common import log_timing
from engine import app


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
            prov = self._provenance(stream_dataset.provenance_metadata)
        if self.stream_request.include_annotations:
            anno = self._annotations(stream_dataset.annotation_store)

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
    def _particles(self, stream_data, stream_key, parameters, external_includes):
        """
        Convert an xray Dataset into a list of dictionaries, each representing a single point in time
        """
        particles = []

        for deployment in sorted(stream_data.datasets):
            ds = stream_data.datasets[deployment]
            # extract the underlying numpy arrays from the dataset (indexing into the dataset is expensive)
            data = {}
            for p in ds.data_vars:
                data[p] = ds[p].values

            # Extract the parameter names from the parameter objects
            params = [p.name for p in parameters]

            # check if we should include and have pressure data
            if stream_key.is_mobile:
                pressure_params = [(sk, param) for sk in external_includes for param in external_includes[sk]
                                   if param.data_product_identifier == PRESSURE_DPI]
                if pressure_params:
                    pressure_key, pressure_param = pressure_params.pop()
                    pressure_name = '-'.join((pressure_key.stream.name, pressure_param.name))
                    if pressure_name in ds:
                        data[INT_PRESSURE_NAME] = ds[pressure_name].values
                        params.append(INT_PRESSURE_NAME)

            # check if we should include and have positional data
            if stream_key.is_glider:
                lat_data = data.get('glider_gps_position-m_gps_lat')
                lon_data = data.get('glider_gps_position-m_gps_lon')
                if lat_data is not None and lon_data is not None:
                    data['lat'] = lat_data
                    data['lon'] = lon_data
                    params.extend(('lat', 'lon'))

            if self.stream_request.include_provenance:
                params.append('provenance')

            # add any QC if it exists
            for param in params:
                    qc_postfixes = ['qc_results', 'qc_executed']
                    for qc_postfix in qc_postfixes:
                        qc_key = '%s_%s' % (param, qc_postfix)
                        if qc_key in data:
                            params.append(qc_key)

            # Warn for any missing parameters
            missing = [p for p in params if p not in data]
            if missing:
                log.warn('<%s> Failed to get data for %r: Not in Dataset', self.request_id, missing)

            params = [p for p in params if p in data]

            pk = stream_key.as_dict()
            for index in xrange(len(ds.time)):
                # Create our particle from the list of parameters
                particle = {p: data[p][index] for p in params}
                particle['pk'] = pk
                particle['pk']['time'] = data['time'][index]
                if 'deployment' in data:
                    particle['pk']['deployment'] = data['deployment'][index]

                particles.append(particle)
        return particles

    @staticmethod
    def _provenance(prov_metadata):
        if prov_metadata is not None:
            return prov_metadata.get_json()

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
        else:
            return json.JSONEncoder.default(self, o)
