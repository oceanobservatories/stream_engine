import json
import os
from collections import defaultdict, OrderedDict
from multiprocessing.pool import ThreadPool

import logging
import numpy as np
import requests
import xarray as xr

from engine import app
from util.calculated_provenance_metadata_store import CalculatedProvenanceMetadataStore
from util.common import ntp_to_datestring
from util.jsonresponse import NumpyJSONEncoder

metadata_threadpool = ThreadPool(10)

log = logging.getLogger(__name__)

class ProvenanceMetadataStore(object):
    def __init__(self, request_uuid):
        self.request_uuid = request_uuid
        self._prov_set = set()
        self.calculated_metadata = CalculatedProvenanceMetadataStore()
        self.messages = []
        self._prov_dict = {}
        self._streaming_provenance = {}
        self._instrument_provenance = {}
        self._query_metadata = OrderedDict()

    def add_messages(self, messages):
        self.messages.extend(messages)

    def add_metadata(self, value):
        self._prov_set.add(value)

    def update_provenance(self, provenance):
        for i in provenance:
            self._prov_dict[i] = provenance[i]

    def update_streaming_provenance(self, stream_prov):
        for i in stream_prov:
            self._streaming_provenance[i] = stream_prov[i]

    def get_streaming_provenance(self):
        return self._streaming_provenance

    def get_provenance_dict(self):
        return self._prov_dict

    def add_instrument_provenance(self, stream_key, st, et):
        url = app.config['ASSET_URL'] + 'assets/byReferenceDesignator/{:s}/{:s}/{:s}?startDT={:s}?endDT={:s}'.format(
                stream_key.subsite, stream_key.node, stream_key.sensor, ntp_to_datestring(st), ntp_to_datestring(et))
        self._instrument_provenance[stream_key] = metadata_threadpool.apply_async(_send_query_for_instrument, (url,))

    def get_instrument_provenance(self):
        try:
            vals = defaultdict(list)
            for key, value in self._instrument_provenance.iteritems():
                vals[key.as_three_part_refdes()].extend(value.get())
            return vals
        except ValueError:
            return defaultdict(list)

    def add_query_metadata(self, stream_request, query_uuid, query_type):
        self._query_metadata['query_type'] = query_type
        self._query_metadata['query_uuid'] = query_uuid
        self._query_metadata['begin'] = stream_request.time_range.start
        self._query_metadata['beginDT'] = ntp_to_datestring(stream_request.time_range.start)
        self._query_metadata['end'] = stream_request.time_range.stop
        self._query_metadata['endDT'] = ntp_to_datestring(stream_request.time_range.stop)
        self._query_metadata['limit'] = stream_request.limit
        self._query_metadata['requested_stream'] = stream_request.stream_key.as_dashed_refdes()
        self._query_metadata['include_provenance'] = stream_request.include_provenance
        self._query_metadata['include_annotations'] = stream_request.include_annotations
        self._query_metadata['strict_range'] = stream_request.strict_range

    def add_to_dataset(self, ds, request_id):
        if 'provenance' in ds:
            prov_values = {}
            prov_array = []
            for k, v in self.get_provenance_dict().iteritems():
                prov_values[k] = v['file_name'] + " " + v['parser_name'] + " " + v['parser_version']
            log.debug('<%s> Building %d provenance values', request_id, len(ds.provenance.values))
            for i in ds.provenance.values:
                if i in prov_values:
                    prov_array.append(prov_values[i])
                else:
                    prov_array.append('')

            prov_array = np.array(prov_array, dtype=str)
            ds['l0_provenance_information'] = ('obs', prov_array,
                                               {'long_name': 'l0_provenance_data',
                                                'description': 'file name, parser name, and parser version'})

            if prov_values:
                ds['l0_provenance_keys'] = xr.DataArray(np.array(prov_values.keys()), dims=['l0_provenance'],
                                                        attrs={'long_name': 'l0 Provenance Keys'})
                ds['l0_provenance_data'] = xr.DataArray(np.array(prov_values.values()), dims=['l0_provenance'],
                                                        attrs={'long_name': 'l0 Provenance Entries'})

        if self._streaming_provenance:
            ds['streaming_provenance'] = xr.DataArray([json.dumps(self._streaming_provenance)],
                                                      dims=['streaming_provenance_dim'],
                                                      attrs={'long_name': 'Streaming Provenance Information'})

        comp_prov = self.calculated_metadata.get_dict()
        comp_prov = json.dumps(comp_prov, cls=NumpyJSONEncoder)
        if comp_prov:
            ds['computed_provenance'] = xr.DataArray([comp_prov], dims=['computed_provenance_dim'],
                                                     attrs={'long_name': 'Computed Provenance Information'})

        if self._query_metadata:
            ds['query_parameter_provenance'] = xr.DataArray([json.dumps(self._query_metadata)],
                                                            dims=['query_parameter_provenance_dim'],
                                                            attrs={
                                                                'long_name': 'Query Parameter Provenance Information'})

    def get_json(self):
        out = OrderedDict()
        out['provenance'] = self._prov_dict
        out['streaming_provenance'] = self._streaming_provenance
        out['instrument_provenance'] = self.get_instrument_provenance()
        out['computed_provenance'] = self.calculated_metadata.get_dict()
        out['query_parameter_provenance'] = self._query_metadata
        out['provenance_messages'] = self.messages
        out['requestUUID'] = self.request_uuid
        return out

    def dump_json(self, filepath):
        try:
            if not os.path.exists(os.path.dirname(filepath)):
                os.makedirs(os.path.dirname(filepath))
            with open(filepath, 'a') as fh:
                json.dump(self.get_json(), fh, indent=2, separators=(',', ': '))
        except EnvironmentError as e:
            log.error('Failed to write provenance file: %s', e)

def _send_query_for_instrument(url):
    results = requests.get(url)
    jres = results.json()
    return jres
