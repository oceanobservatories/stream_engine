__author__ = 'Stephen Zakrewsky'

from collections import OrderedDict
import json
import logging
import numpy as np
from common import log_timing


log = logging.getLogger(__name__)


class JsonResponse(object):

    def __init__(self, stream_data):
        self.stream_data = stream_data

    @log_timing(log)
    def json(self, stream_to_params):
        groups = {}
        for stream_key in stream_to_params.keys():
            groups[stream_key.as_dashed_refdes()] = []
            for sk, dn, ds in self.stream_data.groups(stream_key):
                groups[stream_key.as_dashed_refdes()].extend(
                    self._particles(ds, stream_key, stream_to_params[stream_key]))

        if len(groups.keys()) == 1:
            data = groups.itervalues().next()
        else:
            data = groups

        metadata = self._metadata(self.stream_data)
        if metadata:
            out = OrderedDict()
            out['data'] = data
            out.update(metadata)
        else:
            out = data

        return json.dumps(out, indent=2, cls=NumpyJSONEncoder)

    @log_timing(log)
    def _particles(self, ds, stream_key, parameters):
        # convert data into a list of particles
        particles = []
        warned = set()
        data = {}

        for p in ds.data_vars:
            data[p] = ds[p].values

        if 'time' not in data:
            data['time'] = ds.time.values

        for index in xrange(len(ds.time)):
            particle = OrderedDict()

            if not stream_key.stream.is_virtual:
                particle['pk'] = stream_key.as_dict()
                # Add non-param data to particle
                particle['pk']['deployment'] = data['deployment'][index]
                particle['pk']['time'] = data['time'][index]
                particle['provenance'] = str(data['provenance'][index])

            for param in parameters:
                if param.name not in data:
                    if param.name not in warned:
                        log.info("Failed to get data for %d: Not in Dataset", param.id)
                        # Only one once for missing parameter
                        warned.add(param.name)
                    continue
                particle[param.name] = data[param.name][index]

                qc_postfixes = ['qc_results', 'qc_executed']
                for qc_postfix in qc_postfixes:
                    qc_key = '%s_%s' % (param.name, qc_postfix)
                    if qc_key in data:
                        particle[qc_key] = data[qc_key][index]
            particles.append(particle)
        return particles

    @log_timing(log)
    def _metadata(self, stream_data):
        if stream_data.provenance_metadata is not None or stream_data.annotation_store is not None:
            out = OrderedDict()
            if stream_data.provenance_metadata is not None:
                out['provenance'] = stream_data.provenance_metadata.get_provenance_dict()
                out['streaming_provenance'] = stream_data.provenance_metadata.get_streaming_provenance()
                out['computed_provenance'] = stream_data.provenance_metadata.calculated_metatdata.get_dict()
                out['query_parameter_provenance'] = stream_data.provenance_metadata.get_query_dict()
                out['provenance_messages'] = stream_data.provenance_metadata.messages
            if stream_data.annotation_store is not None:
                out['annotations'] = stream_data.annotation_store.get_json_representation()
            return out

    @log_timing(log)
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
