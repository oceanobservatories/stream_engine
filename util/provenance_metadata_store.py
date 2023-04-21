import json
import logging
import os
from collections import OrderedDict

import requests

from util.calculated_provenance_metadata_store import CalculatedProvenanceMetadataStore
from util.common import ntp_to_datestring, WriteErrorException, sort_dict, PROVENANCE_KEYORDER
from util.jsonresponse import NumpyJSONEncoder

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

    def update_provenance(self, provenance, deployment):
        for i in provenance:
            self._prov_dict.setdefault(deployment, {})[i] = provenance[i]

    def get_provenance_dict(self):
        return self._prov_dict

    def get_provenance(self, deployments):
        data_file_prov = {}
        for deployment in deployments:
            data_file_prov.update(self._prov_dict.get(deployment, {}))
        return data_file_prov

    def add_instrument_provenance(self, stream_key, events):
        # reorder the JSON fields for readability
        # The JSON spec is unordered - this ordering should not be relied upon by code!
        # sorted_first = False so that sensor.calibration comes after the other sensor fields
        events = [sort_dict(e, PROVENANCE_KEYORDER, sorted_first=False) for e in events]
        self._instrument_provenance[stream_key.as_three_part_refdes()] = events

    def get_instrument_provenance(self, deployments):
        ret_dict = {}
        for ref_des, deployment_events in self._instrument_provenance.iteritems():
            # There is one ProvenanceMetadataStore per StreamDataset, which can contain multiple deployments
            # of one logical instrument (one reference designator).
            # Select only the deployments that are specified by the caller.
            dep_events = [dep_event for dep_event in deployment_events
                          if dep_event["deploymentNumber"] in deployments]
            # Only include calibrations that apply to the deployment events
            trim_calibration_data(dep_events)
            # Change .xlsx filename suffix back to .csv as it is in GitHub asset-management
            rename_data_sources(dep_events)
            ret_dict[ref_des] = dep_events
        return ret_dict

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

    def get_json(self, deployments):
        out = OrderedDict()
        out['provenance'] = self.get_provenance(deployments)
        out['instrument_provenance'] = self.get_instrument_provenance(deployments)
        out['computed_provenance'] = self.calculated_metadata.get_dict()
        out['query_parameter_provenance'] = self._query_metadata
        out['provenance_messages'] = self.messages
        out['requestUUID'] = self.request_uuid
        return out

    def dump_json(self, filepath, deployment):
        try:
            parent_dir = os.path.dirname(filepath)
            if not os.path.exists(parent_dir):
                try:
                    os.makedirs(parent_dir)
                except OSError:
                    if not os.path.isdir(parent_dir):
                        raise WriteErrorException('Unable to create local output directory: %s' % parent_dir)

            with open(filepath, 'w') as fh:
                json.dump(self.get_json([deployment]), fh, indent=2, separators=(',', ': '), cls=NumpyJSONEncoder)
        except EnvironmentError as e:
            log.error('Failed to write provenance file: %s', e)


def _send_query_for_instrument(url):
    results = requests.get(url)
    jres = results.json()
    return jres


def trim_calibration_data(deployment_events):
    # Only include calibrations that apply to the deployments
    for deployment_event in deployment_events:
        sensor = deployment_event.get("sensor", None)
        if not sensor:
            return
        calibration_list = sensor.get("calibration", [])
        for calibration in calibration_list:
            calibration_data_list = calibration.get("calData", [])
            calibration_data_list.sort(key=lambda x: x["eventStartTime"], reverse=False)
            first_cal_index = 0
            last_cal_index = 0
            for i in range(len(calibration_data_list)):
                cal_data = calibration_data_list[i]
                if cal_data["eventStartTime"] <= deployment_event["eventStartTime"]:
                    first_cal_index = i
                if deployment_event["eventStopTime"] is None or \
                        cal_data["eventStartTime"] < deployment_event["eventStopTime"]:
                    last_cal_index  = i
            calibration["calData"] = calibration_data_list[first_cal_index:last_cal_index+1]


def rename_data_sources(deployment_events):
    # .xlsx files are created from the original .csv files during asset_management load.
    # Revert to the original name in the provenance
    for deployment_event in deployment_events:
        deployment_event["dataSource"] = deployment_event["dataSource"].replace(".xlsx", ".csv", 1)

        deploy_cruise_info = deployment_event.get("deployCruiseInfo", None)
        if deploy_cruise_info:
            deploy_cruise_info["dataSource"] = deploy_cruise_info["dataSource"].replace(".xlsx", ".csv", 1)

        recover_cruise_info = deployment_event.get("recoverCruiseInfo", None)
        if recover_cruise_info:
            recover_cruise_info["dataSource"] = recover_cruise_info["dataSource"].replace(".xlsx", ".csv", 1)

        sensor = deployment_event.get("sensor", None)
        if sensor:
            for calibration in sensor.get("calibration", []):
                for cal_data in calibration.get("calData", []):
                    cal_data["dataSource"] = cal_data["dataSource"].replace("_Cal_Info.xlsx", ".csv", 1)

