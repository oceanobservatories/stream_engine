import logging
from numbers import Number

import requests
import numpy as np

from datetime import datetime
from cachetools import TTLCache, cached
from concurrent.futures import ThreadPoolExecutor
from simplejson import JSONDecodeError

from util.common import ntp_to_datestring

log = logging.getLogger(__name__)
cache = TTLCache(100, 3600)
executor = ThreadPoolExecutor(max_workers=4)


class CalibrationValue(object):
    """
    Class to represent a single calibration value
    """
    def __init__(self, deployment, name, value):
        self.deployment = deployment
        self.name = name
        self.value = value
        self.deployment = deployment

    def __repr__(self):
        return 'CalibrationEvent(deployment: {deployment} name: {name} value: {value})'.format(**self.__dict__)


class DeploymentEvent(object):
    """
    Class to represent a deployment event
    """
    ntp_epoch = datetime(1900, 1, 1)

    def __init__(self, tag, refdes, deployment, start, stop, lat, lon, depth, name):
        self.tag = tag
        self.refdes = refdes
        self.deployment = deployment
        self.start = start
        self.stop = stop
        self.ntp_start = (self.start - self.ntp_epoch).total_seconds()
        self.ntp_stop = (self.stop - self.ntp_epoch).total_seconds()
        self.lat = lat
        self.lon = lon
        self.depth = depth
        self.name = name

    def __repr__(self):
        return ('DeploymentEvent(tag: {tag} refdes: {refdes} '
                'deployment: {deployment} start: {start} stop: {stop} '
                'lat: {lat} lon: {lon} depth:{depth} name: {name})').format(**self.__dict__)


class AssetEvents(object):
    """
    Container for all events for a particular reference designator
    """
    def __init__(self, refdes, events, request_id=None):
        self.request_id = request_id
        self.refdes = refdes
        self.events = events
        self.tags = self._extract_tags()
        self.deps = self._extract_deployments()
        self.cals = self._extract_calibrations()

    def get_location_data(self, deployment):
        """
        Returns the latitude, longitude and depth for this deployment or None if not found
        """
        lat = lon = depth = None
        dep = self.deps.get(deployment)
        if dep is not None:
            lat = dep.lat
            lon = dep.lon
            depth = dep.depth
        return lat, lon, depth

    def get_cal(self, name, deployment):
        """
        Given a calibration name and deployment number, return the calibration value
        """
        if isinstance(name, Number):
            return name, {'constant': name}

        return self.cals.get(name, {}).get(deployment)

    def get_tiled_cal(self, name, deployment, times):
        """
        Given a calibration name, deployment number and times vector, return the time-vectorized value
        """
        if isinstance(name, Number):
            return name, {'constant': name}

        cal = self.get_cal(name, deployment)

        if cal is None:
            message = '<%s> Unable to build cc %r: no cc exists for deployment: %d'
            log.error(message, self.request_id, name, deployment)
            return None, None

        cal = np.array(cal.value)
        shape = times.shape + cal.shape
        cc = np.empty(shape)
        cc[:] = cal

        st = times[0]
        et = times[-1]
        startdt = ntp_to_datestring(st)
        enddt = ntp_to_datestring(et)

        cc_meta = {
            'sources': cal,
            'data_begin': st,
            'data_end': et,
            'beginDT': startdt,
            'endDT': enddt,
            'type': 'CC',
        }
        return cc, cc_meta

    def _extract_tags(self):
        """
        Extract all tag events from event list, return as { assetid -> tag }
        """
        tags = {}
        for event in self.events:
            if event.get('@class') == '.TagEvent':
                tag = event.get('tag')
                asset_id = self.get_assetid(event)
                if tag and asset_id:
                    tags[asset_id] = tag
        return tags

    def _extract_deployments(self):
        """
        Extract all deployment events from event list, return as { number -> DeploymentEvent }
        """
        deps = {}
        for event in self.events:
            if event.get('@class') == '.DeploymentEvent':
                refdes = self.get_refdes(event)
                if refdes != self.refdes:
                    continue

                start = self.datetime_from_msecs(event.get('startDate'))
                stop = self.datetime_from_msecs(event.get('endDate'))
                number = event.get('deploymentNumber', 0)
                depth = event.get('depth', 0)
                name = event.get('locationName')
                lon_lat = event.get('locationLonLat')
                if lon_lat is None:
                    lon, lat = None, None
                else:
                    lon, lat = lon_lat
                tag = self.tags.get(self.get_assetid(event))
                if tag is not None:
                    deps[number] = DeploymentEvent(tag, refdes, number, start, stop, lat, lon, depth, name)
        return deps

    def _extract_calibrations(self):
        """
        Extract all calibration events from event list, return as { name -> { number -> CalibrationValue } }
        """
        cals = {}

        for event in self.events:
            if event.get('@class') == '.CalibrationEvent':
                start = self.datetime_from_msecs(event.get('startDate'))
                cc = event.get('calibrationCoefficient', [])
                tag = self.tags.get(self.get_assetid(event))
                if tag is not None:
                    for dep_number in self.deps:
                        dep = self.deps[dep_number]
                        if start == dep.start and tag == dep.tag:
                            for each in cc:
                                name = each['name']
                                value = each['values']
                                cals.setdefault(name, {})[dep_number] = CalibrationValue(dep, name, value)
        return cals

    @staticmethod
    def get_assetid(event):
        return event.get('asset', {}).get('assetId')

    @staticmethod
    def get_refdes(event):
        refdes = event.get('referenceDesignator', {})
        subsite = refdes.get('subsite')
        node = refdes.get('node')
        sensor = refdes.get('sensor')
        if subsite and node and sensor:
            return '-'.join((subsite, node, sensor))
        elif subsite and node:
            return '-'.join((subsite, node))
        return subsite

    @staticmethod
    def datetime_from_msecs(msecs):
        if msecs is None or msecs < 0:
            dt = datetime.utcfromtimestamp(0)
        else:
            dt = datetime.utcfromtimestamp(msecs / 1000.0)
        return dt


class AssetManagement(object):
    def __init__(self, host, port=12573, request_id=None):
        self.base_url = 'http://{host}:{port}'.format(host=host, port=port)
        self.request_id = request_id

    def get_events(self, subsite, node, sensor):
        return self._get_events(subsite, node, sensor)

    def get_events_async(self, subsite, node, sensor):
        return executor.submit(self._get_events, subsite, node, sensor)

    def get_events_url(self, subsite, node, sensor):
        return '/'.join((self.base_url, 'assets', 'byReferenceDesignator', subsite, node, sensor, 'events'))

    @cached(cache)
    def _get_events(self, subsite, node, sensor):
        refdes = '-'.join((subsite, node, sensor))
        qurl = self.get_events_url(subsite, node, sensor)
        log.debug('<%s> AM query: %r', self.request_id, qurl)
        try:
            response = requests.get(qurl)
            return AssetEvents(refdes, response.json())
        except (JSONDecodeError, ValueError) as e:
            log.warn('<%s> Received invalid response from Asset Management for %s-%s-%s: %s',
                     self.request_id, subsite, node, sensor, e)
            return AssetEvents(refdes, [])
