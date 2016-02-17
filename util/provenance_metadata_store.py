from collections import defaultdict, OrderedDict
from multiprocessing.pool import ThreadPool

import requests

from engine import app
from util.calculated_provenance_metadata_store import CalculatedProvenanceMetadataStore
from util.common import ntp_to_datestring


metadata_threadpool = ThreadPool(10)


class ProvenanceMetadataStore(object):
    def __init__(self, request_uuid):
        self.request_uuid = request_uuid
        self._prov_set = set()
        self.calculated_metatdata = CalculatedProvenanceMetadataStore()
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
        vals = defaultdict(list)
        for key, value in self._instrument_provenance.iteritems():
            vals[key.as_three_part_refdes()].extend(value.get())
        return vals

    def add_query_metadata(self, stream_request, query_uuid, query_type):
        self._query_metadata["query_type"] = query_type
        self._query_metadata['query_uuid'] = query_uuid
        self._query_metadata['begin'] = stream_request.time_range.start
        self._query_metadata['beginDT'] = ntp_to_datestring(stream_request.time_range.start)
        self._query_metadata['end'] = stream_request.time_range.stop
        self._query_metadata['endDT'] = ntp_to_datestring(stream_request.time_range.stop)
        self._query_metadata['limit'] = stream_request.limit
        self._query_metadata["requested_stream"] = stream_request.stream_key.as_dashed_refdes()
        self._query_metadata["include_provenance"] = stream_request.include_provenance
        self._query_metadata["include_annotations"] = stream_request.include_annotations
        self._query_metadata["strict_range"] = stream_request.strict_range

    def get_query_dict(self):
        return self._query_metadata


def _send_query_for_instrument(url):
    results = requests.get(url)
    jres = results.json()
    return jres