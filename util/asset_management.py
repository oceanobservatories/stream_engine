import logging
from numbers import Number

import requests
import numpy as np

from datetime import datetime
from cachetools import TTLCache, cached
from concurrent.futures import ThreadPoolExecutor
from simplejson import JSONDecodeError

from util.common import ntp_to_datestring
from util.numpy_broadcast_to import broadcast_to

log = logging.getLogger(__name__)
cache = TTLCache(100, 3600)
executor = ThreadPoolExecutor(max_workers=4)

LATITUDE_NAMES = ['CC_lat', 'CC_latitude']
LONGITUDE_NAMES = ['CC_lon', 'CC_longitude']


class TimeBoundsException(Exception):
    pass


class Deployment(object):
    """
    Class to represent a deployment event
    """
    ntp_epoch = datetime(1900, 1, 1)

    def __init__(self, am_dictionary):
        self.am_dictionary = am_dictionary
        self._times = None

    @property
    def ntp_start(self):
        start, _ = self.get_times()
        return (start - self.ntp_epoch).total_seconds()

    @property
    def ntp_stop(self):
        _, stop = self.get_times()
        if stop is None:
            stop = datetime.utcnow()
        return (stop - self.ntp_epoch).total_seconds()

    def get_number(self):
        return self.am_dictionary.get('deploymentNumber', 0)

    def get_times(self):
        if self._times is None:
            self._times = self._extract_times(self.am_dictionary)
        return self._times

    def get_location(self):
        return self.am_dictionary.get('location')

    def _get_sensor(self):
        return self.am_dictionary.get('sensor', {})

    def _get_calibration(self):
        return self._get_sensor().get('calibration', [])

    def get_cal_values(self):
        dstart, dstop = self.get_times()
        d = {}
        for each in self._get_calibration():
            name = each.get('name')
            data = each.get('calData')
            for each in data:
                val, cstart, cstop = self._extract_from_cal(each)
                try:
                    cstart, cstop = self._valid_times(dstart, dstop, cstart, cstop)
                    d.setdefault(name, []).append((val, cstart, cstop))
                except TimeBoundsException:
                    continue
        return d

    def _valid_times(self, dstart, dstop, cstart, cstop):
        # start times must always be defined
        if not all((cstart, dstart)):
            raise TimeBoundsException

        # deployment stop defined and cal start after - not applicable this deployment
        if dstop is not None and cstart > dstop:
            raise TimeBoundsException

        # cal stop defined and deployment start after - not applicable this deployment
        if cstop is not None and dstart > cstop:
            raise TimeBoundsException

        # trim calibration times to deployment bounds
        if cstart < dstart:
            cstart = dstart

        # both stops are unbounded, do nothing
        if cstop is None and dstop is None:
            pass
        # cal stop is unbounded but deployment stop defined, trim
        elif cstop is None and dstop:
            cstop = dstop
        # both stops are defined but cal exceed deployment, trim
        elif cstop > dstop:
            cstop = dstop

        return cstart, cstop

    def _extract_from_cal(self, data):
        val = data.get('value')
        start, stop = self._extract_times(data)
        return val, start, stop

    @staticmethod
    def _extract_times(dictionary):
        start = dictionary.get('eventStartTime')
        stop = dictionary.get('eventStopTime')

        if isinstance(start, Number):
            start = datetime.utcfromtimestamp(start / 1000.0)

        if isinstance(stop, Number):
            stop = datetime.utcfromtimestamp(stop / 1000.0)

        return start, stop


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


class AssetEvents(object):
    """
    Container for all events for a particular reference designator
    """
    def __init__(self, refdes, events, request_id=None):
        self.request_id = request_id
        self.refdes = refdes
        self.events = events
        self.deps = {}
        self.cals = {}
        self.locations = {}
        self.parse_events()

    def parse_events(self):
        for event in self.events:
            deployment = Deployment(event)
            number = deployment.get_number()
            self.deps[number] = deployment
            self.cals[number] = deployment.get_cal_values()
            self.locations[number] = deployment.get_location()

    def get_location_data(self, deployment):
        """
        Returns the latitude, longitude and depth for this deployment or None if not found
        """
        lat = lon = depth = None
        loc = self.locations.get(deployment)
        if loc is not None:
            lat = loc.get('latitude')
            lon = loc.get('longitude')
            depth = loc.get('depth')
        return lat, lon, depth

    def get_cal(self, name, deployment):
        """
        Given a calibration name and deployment number, return the calibration value
        """
        if isinstance(name, Number):
            return name, {'constant': name}

        return self.cals.get(deployment, {}).get(name)

    def get_tiled_cal(self, name, deployment, times):
        """
        Given a calibration name, deployment number and times vector, return the time-vectorized value
        """
        if isinstance(name, Number):
            return name, {'constant': name}

        if name in LATITUDE_NAMES:
            lat, _, _ = self.get_location_data(deployment)
            cal = [(lat, 0, 0)]

        elif name in LONGITUDE_NAMES:
            _, lon, _ = self.get_location_data(deployment)
            cal = [(lon, 0, 0)]

        else:
            cal = self.get_cal(name, deployment)

        if cal is None:
            message = '<%s> Unable to build cc %r: no cc exists for deployment: %d'
            log.error(message, self.request_id, name, deployment)
            return None, None

        if len(cal) == 1:
            value, _, _ = cal[0]
            value = np.array(value)
            shape = times.shape + value.shape

            # SPECIAL CASE HANDLING FOR OPTAA
            # I would love to do this for ALL coefficients,
            # but the met algorithms write to the cal data (TODO: investigate)
            if name in ['CC_tcarray', 'CC_taarray']:
                cc = broadcast_to(value, shape)
            else:
                cc = np.empty(shape)
                cc[:] = value

            st = times[0]
            et = times[-1]
            startdt = ntp_to_datestring(st)
            enddt = ntp_to_datestring(et)

            cc_meta = {
                'sources': value,
                'data_begin': st,
                'data_end': et,
                'beginDT': startdt,
                'endDT': enddt,
                'type': 'CC',
            }
            return cc, cc_meta


class AssetManagement(object):
    def __init__(self, host, port=12587, request_id=None):
        self.base_url = 'http://{host}:{port}/asset/cal'.format(host=host, port=port)
        self.request_id = request_id

    def get_events(self, refdes):
        return self._get_events(refdes)

    def get_events_async(self, refdes):
        return executor.submit(self._get_events, refdes)

    @staticmethod
    def _get_refdes(subsite, node, sensor):
        return '-'.join((subsite, node, sensor))

    @cached(cache)
    def _get_events(self, refdes):
        params = dict(refdes=refdes)
        log.debug('<%s> AM query: %r %r', self.request_id, self.base_url, params)
        try:
            response = requests.get(self.base_url, params=params)
            return AssetEvents(refdes, response.json())
        except (JSONDecodeError, ValueError) as e:
            log.warn('<%s> Received invalid response from Asset Management for %s: %s',
                     self.request_id, refdes, e)
            return AssetEvents(refdes, [])
