__author__ = 'Stephen Zakrewsky'

from collections import OrderedDict
from common import CachedFunction
import datamodel
import json
import logging
import numpy as np


log = logging.getLogger(__name__)


class JsonResponse(object):

    def __init__(self, pd_data, qc_stream_parameters, provenance_metadata=None, annotation_store=None):
        self.pd_data = pd_data
        self.qc_stream_parameters = qc_stream_parameters
        self.provenance_metadata = provenance_metadata
        self.annotation_store = annotation_store

    def json(self, stream_key, parameters):
        ds = datamodel.as_xray(stream_key, self.pd_data, self.provenance_metadata, self.annotation_store)
        return self._json(stream_key, parameters, ds)

    def _json(self, stream_key, parameters, ds):
        # setup some convenience bindings
        qc_stream_parameters = self.qc_stream_parameters
        provenance_metadata = self.provenance_metadata
        annotation_store = self.annotation_store

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

        if provenance_metadata is not None or annotation_store is not None:
            out = OrderedDict()
            out['data'] = particles
            if provenance_metadata is not None:
                out['provenance'] = provenance_metadata.get_provenance_dict()
                out['computed_provenance'] = provenance_metadata.calculated_metatdata.get_dict()
                out['query_parameter_provenance'] = provenance_metadata.get_query_dict()
                out['provenance_messages'] = provenance_metadata.messages
            if annotation_store is not None:
                out['annotations'] = annotation_store.get_json_representation()
        else:
            out = particles

        return json.dumps(out, indent=2, cls=NumpyJSONEncoder)

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