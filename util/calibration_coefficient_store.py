import logging
from collections import namedtuple
from numbers import Number

import numpy

from util.common import ntp_to_datestring


FramedCoefficient = namedtuple('FramedCoefficient', ['start', 'stop', 'value', 'deployment'])


log = logging.getLogger(__name__)


class CalibrationCoefficientStore(object):
    def __init__(self, coefficients, request_id=None):
        self.coefficients = coefficients
        self.request_id = request_id

    def get(self, name, deployment, times):
        if isinstance(name, Number):
            return name, {'constant': name}

        st = times[0]
        et = times[-1]
        startdt = ntp_to_datestring(st)
        enddt = ntp_to_datestring(et)

        frames = [FramedCoefficient(**f) for f in self.coefficients.get(name, [])]
        frames.sort()
        frames = [f for f in frames if deployment in [0, f.deployment] and any(self.in_range(f, times))]

        if not frames:
            message = '<%s> Unable to build cc %r: no cc exists for deployment: %d start: %s stop: %s'
            log.error(message, self.request_id, name, deployment, startdt, enddt)
            return None, None

        first_value = frames[0].value

        if isinstance(first_value, (list, tuple)):
            shape = times.shape + numpy.array(first_value).shape
        else:
            shape = times.shape
        cc = numpy.empty(shape)
        cc[:] = numpy.NAN

        values = []
        try:
            for frame in frames:
                values.append({'CC_begin': frame[0], 'CC_stop': frame[1], 'value': frame[2], 'deployment': frame[3],
                               'CC_beginDT': ntp_to_datestring(frame[0]), 'CC_stopDT': ntp_to_datestring(frame[1])})
                mask = self.in_range(frame, times)
                cc[mask] = frame[2]
        except ValueError:
            log.exception('<%s> Exception while building cc %r', self.request_id, name)
            return None, None

        cc_meta = {
            'sources': values,
            'data_begin': st,
            'data_end': et,
            'beginDT': startdt,
            'endDT': enddt,
            'type': 'CC',
        }
        return cc, cc_meta

    @staticmethod
    def in_range(frame, times):
        """
        Returns boolean masking array for times in range.

          frame is a tuple such that frame[0] is the inclusive start time and
          frame[1] is the exclusive stop time.  None for any of these indices
          indicates unbounded.

          times is a numpy array of ntp times.

          returns a bool numpy array the same shape as times
        """
        if frame.start is None and frame.stop is None:
            mask = numpy.ones(times.shape, dtype=bool)
        elif frame.start is None:
            mask = (times < frame.stop)
        elif frame.stop is None:
            mask = (times >= frame.start)
        elif frame.start == frame.stop:
            mask = (times == frame.start)
        else:
            mask = numpy.logical_and(times >= frame.start, times < frame.stop)
        return mask
