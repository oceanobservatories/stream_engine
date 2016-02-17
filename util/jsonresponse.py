from collections import OrderedDict
import json
import logging
import numpy as np
from common import log_timing


__author__ = 'Stephen Zakrewsky'


log = logging.getLogger(__name__)


class JsonResponse(object):

    def __init__(self, stream_request):
        self.stream_request = stream_request
        self.request_id = stream_request.request_id

    @log_timing(log)
    def json(self):
        stream_key = self.stream_request.stream_key
        dataset = self.stream_request.datasets[stream_key]
        parameters = self.stream_request.requested_parameters
        external_includes = self.stream_request.external_includes
        data = self._particles(dataset, stream_key, parameters, external_includes)

        metadata = self._metadata(self.stream_request)
        if metadata:
            out = OrderedDict()
            out['data'] = data
            out.update(metadata)
        else:
            out = data

        return json.dumps(out, indent=2, cls=NumpyJSONEncoder)

    @log_timing(log)
    def _particles(self, ds, stream_key, parameters, external_includes):
        """
        Convert an xray Dataset into a list of dictionaries, each representing a single point in time
        """
        particles = []

        # extract the underlying numpy arrays from the dataset (indexing into the dataset is expensive)
        data = {}
        for p in ds.data_vars:
            data[p] = ds[p].values

        # Sometimes time is a coordinate, not a data_var, make sure we get it
        if 'time' not in data:
            data['time'] = ds.time.values

        # Extract the parameter names from the parameter objects
        params = [p.name for p in parameters]
        external_params = ['-'.join((sk.stream.name, param.name))
                           for sk in external_includes for param in external_includes[sk]]
        params.extend(external_params)

        # Warn for any missing parameters
        missing = [p for p in params if p not in data]
        if missing:
            log.warn('<%s> Failed to get data for %r: Not in Dataset', self.request_id, missing)

        for index in xrange(len(ds.time)):
            # Create our particle from the list of parameters
            particle = {p: data[p][index] for p in params if p not in missing}

            # Add primary key, deployment, provenance
            if not stream_key.stream.source_streams:
                particle['pk'] = stream_key.as_dict()
                # Add non-param data to particle
                particle['pk']['deployment'] = data['deployment'][index]
                # TODO: remove, time is already present outside primary key.
                # TODO: Currently UI uses for stacked timeseries, need to remove dependency
                particle['pk']['time'] = data['time'][index]
                particle['provenance'] = str(data['provenance'][index])

                # # Add location information
                # particle['latitude'] = ds['lat'].values[index] if 'lat' in ds else LAT_FILL
                # particle['longitude'] = ds['lon'].values[index] if 'lon' in ds else LON_FILL

            # Add any QC if it exists
            for param in params:
                qc_postfixes = ['qc_results', 'qc_executed']
                for qc_postfix in qc_postfixes:
                    qc_key = '%s_%s' % (param, qc_postfix)
                    if qc_key in data:
                        particle[qc_key] = data[qc_key][index]
            particles.append(particle)
        return particles

    @staticmethod
    def _metadata(stream_request):
        if stream_request.include_provenance or stream_request.include_annotations:
            out = OrderedDict()
            if stream_request.provenance_metadata is not None:
                out['provenance'] = stream_request.provenance_metadata.get_provenance_dict()
                out['streaming_provenance'] = stream_request.provenance_metadata.get_streaming_provenance()
                out['instrument_provenance'] = stream_request.provenance_metadata.get_instrument_provenance()
                out['computed_provenance'] = stream_request.provenance_metadata.calculated_metatdata.get_dict()
                out['query_parameter_provenance'] = stream_request.provenance_metadata.get_query_dict()
                out['provenance_messages'] = stream_request.provenance_metadata.messages
                out['requestUUID'] = stream_request.provenance_metadata.request_uuid
            if stream_request.annotation_store is not None:
                out['annotations'] = stream_request.annotation_store.get_json_representation()
            return out

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
