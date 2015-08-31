__author__ = 'Stephen Zakrewsky'

from cass import store_qc_results
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
        pd_data = self.pd_data
        qc_stream_parameters = self.qc_stream_parameters
        provenance_metadata = self.provenance_metadata
        annotation_store = self.annotation_store

        # convert data into a list of particles
        particles = []

        virtual_id_sub = None
        for index in xrange(len(ds['time'])):
            particle = OrderedDict()
            particle_id = None
            particle_bin = None

            if not stream_key.stream.is_virtual:
                particle['pk'] = stream_key.as_dict()

                # Add non-param data to particle
                particle['pk']['deployment'] = ds['deployment'].values[index]
                particle['pk']['time'] = ds['time'].values[index]
                particle['provenance'] = str(ds['provenance'].values[index])
                particle_id = ds['id'].values[index]
                particle_bin = ds['bin'].values[index]
            else:
                if 'id' in pd_data:
                    if virtual_id_sub is None:
                        for key in pd_data['id']:
                            if len(pd_data['id'][key]['data']) == len(ds['time']):
                                virtual_id_sub = key
                                break
                    particle_id = pd_data['id'][virtual_id_sub]['data'].values[index]

                if 'bin' in pd_data:
                    if virtual_id_sub is None:
                        for key in pd_data['bin']:
                            if len(pd_data['bin'][key]['data']) == len(ds['time']):
                                virtual_id_sub = key
                                break
                    particle_bin = pd_data['bin'][virtual_id_sub]['data'].values[index]

            for param in parameters:
                if param.name not in ds:
                    log.info("Failed to get data for {}: Not in Dataset".format(param.id))
                    continue

                val = None
                try:
                    val = ds[param.name].values[index]
                except Exception as e:
                    log.info("Failed to get data for {}: {}".format(param.id, e))
                    continue

                particle[param.name] = val

                # add qc results to particle
                for qc_function_name in qc_stream_parameters.get(param.name, []):
                    qc_function_results = '%s_%s' % (param.name, qc_function_name)

                    if qc_function_results in ds:
                        value = ds[qc_function_results].values[index]

                        qc_results_key = '%s_%s' % (param.name, 'qc_results')
                        qc_ran_key = '%s_%s' % (param.name, 'qc_executed')
                        if qc_results_key not in particle:
                            particle[qc_results_key] = 0b0000000000000000

                        qc_results_value = particle[qc_results_key]
                        qc_cached_function = CachedFunction.from_qc_function(qc_function_name)
                        qc_results_mask = int(qc_cached_function.qc_flag, 2)

                        particle[qc_ran_key] = qc_results_mask | particle.get(qc_ran_key, qc_results_mask)

                        if value == 0:
                            qc_results_value = ~qc_results_mask & qc_results_value
                        elif value == 1:
                            qc_results_value = qc_results_mask ^ qc_results_value

                        particle[qc_results_key] = qc_results_value

                        if particle_id is not None and particle_bin is not None:
                            if not stream_key.stream.is_virtual:
                                store_qc_results(qc_results_value, particle.get('pk'), particle_id, particle_bin, param.name)
                            else:
                                if virtual_id_sub is not None:
                                    sub_pk = stream_key.as_dict()
                                    sub_pk['deployment'] = pd_data['deployment'][virtual_id_sub]['data'].values[index]
                                    store_qc_results(qc_results_value, sub_pk, particle_id, particle_bin, param.name)

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