import json
import logging
import os
from collections import OrderedDict

import requests

from util.calculated_provenance_metadata_store import CalculatedProvenanceMetadataStore
from util.common import ntp_to_datestring

log = logging.getLogger(__name__)


class ProvenanceMetadataStore(object):
    def __init__(self, request_uuid):
        self.request_uuid = request_uuid
        self._prov_set = set()
        self.calculated_metadata = CalculatedProvenanceMetadataStore()
        self.messages = []
        self._prov_dict = {}
        self._instrument_provenance = {}
        self._query_metadata = OrderedDict()

    def add_messages(self, messages):
        self.messages.extend(messages)

    def add_metadata(self, value):
        self._prov_set.add(value)

    def update_provenance(self, provenance):
        for i in provenance:
            self._prov_dict[i] = provenance[i]

    def get_provenance_dict(self):
        return self._prov_dict

    def add_instrument_provenance(self, stream_key, events):
        self._instrument_provenance[stream_key.as_three_part_refdes()] = events

    def get_instrument_provenance(self):
        return self._instrument_provenance

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


    def get_json(self):
        out = OrderedDict()
        out['provenance'] = self._prov_dict
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
