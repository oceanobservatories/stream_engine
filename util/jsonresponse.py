__author__ = 'Stephen Zakrewsky'

from cass import store_qc_results
from collections import OrderedDict
from common import CachedFunction, MissingTimeException
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
        # setup some convenience bindings
        primary_key = stream_key
        pd_data = self.pd_data
        parameters = parameters
        qc_stream_parameters = self.qc_stream_parameters
        provenance_metadata = self.provenance_metadata
        annotation_store = self.annotation_store

        # convert data into a list of particles
        particles = []

        time_param = primary_key.stream.time_parameter
        if time_param not in pd_data:
            raise MissingTimeException("Time param: {} is missing from the primary stream".format(time_param))

        virtual_id_sub = None
        qc_batch = list()
        for index in range(len(pd_data[time_param][primary_key.as_refdes()]['data'])):
            particle = OrderedDict()
            particle_id = None
            particle_bin = None

            if not primary_key.stream.is_virtual:
                particle['pk'] = primary_key.as_dict()

                # Add non-param data to particle
                particle['pk']['deployment'] = pd_data['deployment'][primary_key.as_refdes()]['data'][index]
                particle['pk']['time'] = pd_data[primary_key.stream.time_parameter][primary_key.as_refdes()]['data'][index]
                particle['provenance'] = str(pd_data['provenance'][primary_key.as_refdes()]['data'][index])
                particle_id = pd_data['id'][primary_key.as_refdes()]['data'][index]
                particle_bin = pd_data['bin'][primary_key.as_refdes()]['data'][index]
            else:
                if 'id' in pd_data:
                    if virtual_id_sub is None:
                        for key in pd_data['id']:
                            if len(pd_data['id'][key]['data']) == len(pd_data[time_param][primary_key.as_refdes()]['data']):
                                virtual_id_sub = key
                                break
                    particle_id = pd_data['id'][virtual_id_sub]['data'][index]

                if 'bin' in pd_data:
                    if virtual_id_sub is None:
                        for key in pd_data['bin']:
                            if len(pd_data['bin'][key]['data']) == len(pd_data[time_param][primary_key.as_refdes()]['data']):
                                virtual_id_sub = key
                                break
                    particle_bin = pd_data['bin'][virtual_id_sub]['data'][index]

            for param in parameters:
                if param.id in pd_data:
                    val = None
                    try:
                        val = pd_data[param.id][primary_key.as_refdes()]['data'][index]
                    except Exception as e:
                        log.info("Failed to get data for {}: {}".format(param.id, e))
                        continue

                    if isinstance(val, np.ndarray):
                        val = val.tolist()

                    particle[param.name] = val

                # add qc results to particle
                for qc_function_name in qc_stream_parameters.get(param.name, []):
                    qc_function_results = '%s_%s' % (param.name, qc_function_name)

                    if qc_function_results in pd_data\
                            and pd_data.get(qc_function_results, {}).get(primary_key.as_refdes(), {}).get('data') is not None:
                        value = pd_data[qc_function_results][primary_key.as_refdes()]['data'][index]

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
                            if not primary_key.stream.is_virtual:
                                qc_batch.append((qc_results_value, particle.get('pk'), particle_id, particle_bin, param.name))
                            else:
                                if virtual_id_sub is not None:
                                    sub_pk = primary_key.as_dict()
                                    sub_pk['deployment'] = pd_data['deployment'][virtual_id_sub]['data'][index]
                                    qc_batch.append((qc_results_value, sub_pk, particle_id, particle_bin, param.name))

            particles.append(particle)

        if len(qc_batch) > 0:
                store_qc_results(qc_batch)

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

        return json.dumps(out, indent=2)
