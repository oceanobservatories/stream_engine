__author__ = 'Stephen Zakrewsky'

from collections import OrderedDict
from common import CachedFunction
import datamodel
import json
import logging
import numpy as np
import xray_interpolation as xinterp


log = logging.getLogger(__name__)


class JsonResponse(object):

    def __init__(self, request, pd_data, provenance_metadata=None, annotation_store=None):
        self.request = request
        self.pd_data = pd_data
        self.provenance_metadata = provenance_metadata
        self.annotation_store = annotation_store

    def json(self, stream_to_params):
        groups = {}
        for stream_key in stream_to_params.keys():
            ds = datamodel.as_xray(stream_key, self.pd_data, self.provenance_metadata, self.annotation_store)
            if self.request.times is not None:
                ds = xinterp.interp1d_Dataset(ds, time=self.request.times)
            groups[stream_key.as_dashed_refdes()] = self._json(ds, stream_key, stream_to_params[stream_key])

        if len(groups.keys()) == 1:
            return json.dumps(groups.itervalues().next(), indent=2, cls=NumpyJSONEncoder)
        else:
            return json.dumps(groups, indent=2, cls=NumpyJSONEncoder)

    def _json(self, ds, stream_key, parameters):
        # convert data into a list of particles
        particles = []
        for index in xrange(len(ds['time'])):
            particle = OrderedDict()
            if not stream_key.stream.is_virtual:
                particle['pk'] = stream_key.as_dict()
                # Add non-param data to particle
                particle['pk']['deployment'] = ds['deployment'].values[index]
                particle['pk']['time'] = ds['time'].values[index]
                particle['provenance'] = str(ds['provenance'].values[index])

            for param in parameters:
                if param.name not in ds:
                    log.info("Failed to get data for %d: Not in Dataset", (param.id,))
                    continue
                particle[param.name] = ds[param.name].values[index]

                qc_postfixes = ['qc_results', 'qc_executed']
                for qc_postfix in qc_postfixes:
                    qc_key = '%s_%s' % (param.name, qc_postfix)
                    if qc_key in ds:
                        particle[qc_key] = ds[qc_key].values[index]

            particles.append(particle)

        if 'l0_provenance_keys' in ds or 'annotations' in ds:
            out = OrderedDict()
            out['data'] = particles
            if 'l0_provenance_keys' in ds:
                out['provenance'] = {key: self._reconstruct(value) for key, value in zip(ds['l0_provenance_keys'].values, ds['l0_provenance_data'].values)}
                out['computed_provenance'] = json.loads(ds['computed_provenance'].values[0])
                out['query_parameter_provenance'] = json.loads(ds['query_parameter_provenance'].values[0])
                out['provenance_messages'] = ds['provenance_messages'].values
            if 'annotations' in ds:
                out['annotations'] = [json.loads(i) for i in ds['annotations'].values]
        else:
            out = particles
        return out

    def _reconstruct(self, value):
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