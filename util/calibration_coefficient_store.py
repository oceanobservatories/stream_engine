import logging

log = logging.getLogger(__name__)


class CalibrationCoefficientStore(object):
    def __init__(self, request_id=None):
        self.coefficients = {}
        self.request_id = request_id

    def add(self, cals):
        for cal in cals:
            self.coefficients.setdefault(cal.name, []).append(cal)
