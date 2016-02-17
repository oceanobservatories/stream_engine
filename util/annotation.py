import json
from datetime import datetime

import numpy as np
import xray as xr

from util.cass import fetch_annotations

# Seconds from NTP epoch to UNIX epoch
NTP_OFFSET_SECS = 2208988800


class AnnotationStore(object):
    """
    Handles the storage of annotations during the lifecycle of a request.
    """

    def __init__(self):
        self._store = set()

    def add_annotations(self, anotations):
        for i in anotations:
            self._add_annotation(i)

    def _add_annotation(self, annotation):
        self._store.add(annotation)

    def get_annotations(self):
        return list(self._store)

    def as_data_array(self):
        annotations = [json.dumps(x.as_dict()) for x in self._store]
        if annotations:
            return xr.DataArray(np.array(annotations), dims=['dataset_annotations'],
                                attrs={'long_name': 'Data Annotations'})


def query_annotations(key, time_range):
    """
    Query edex for annotations on a stream request.
    :param key: "Key from fetch pd data"
    :param time_range: Time range to use  (float, float)
    :return:
    """
    # Query Cassandra annotations table
    result = fetch_annotations(key, time_range)

    result_list = []

    for r in result:
        # Annotations columns in order defined in cass.py
        subsite, node, sensor, time1, time2, parameters, provenance, annotation, method, deployment, myid = r

        ref_des = '-'.join([subsite, node, sensor])
        startt = datetime.utcfromtimestamp(time1 - NTP_OFFSET_SECS).isoformat() + "Z"
        endt = datetime.utcfromtimestamp(time2 - NTP_OFFSET_SECS).isoformat() + "Z"

        # Add to JSON document
        anno = Annotation(ref_des, startt, endt, parameters, provenance, annotation, method, deployment, str(myid))
        result_list.append(anno)

    return result_list


class Annotation(object):
    def __init__(self, refdes, start, end, parameters, provenance, annotation, method, deployment, ident):
        self.referenceDesignator = refdes
        self.beginDT = start
        self.endDT = end
        self.parameters = parameters
        self.provenance = provenance
        self.annotation = annotation
        self.method = method
        self.deployment = deployment
        self.ident = ident

    def as_dict(self):
        return {
            'referenceDesignator': self.referenceDesignator,
            'beginDT': self.beginDT,
            'endDT': self.endDT,
            'parameters': self.parameters,
            'provenance': self.provenance,
            'annotation': self.annotation,
            'method': self.method,
            'deployment': self.deployment,
            'id': self.ident,
        }

    def __eq__(self, other):
        if not isinstance(other, Annotation):
            return False
        return self.ident == other.ident

    def __hash__(self):
        return hash(self.ident)

    @staticmethod
    def from_dict(d):
        return Annotation(d["referenceDesignator"], d["beginDT"], d["endDT"], d["parameters"], d["provenance"],
                          d["annotation"], d["method"], d["deployment"], d["id"])
