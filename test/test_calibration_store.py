import os
import unittest

import numpy as np
import requests_mock

from util.asset_management import AssetManagement
from util.calibration_coefficient_store import CalibrationCoefficientStore


TEST_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(TEST_DIR, 'data')
events_response = open(os.path.join(DATA_DIR, 'events.json')).read()


class CalibrationStoreTest(unittest.TestCase):

    def get_calibrations(self):
        am = AssetManagement(None)
        with requests_mock.Mocker() as m:
            call_args = ('CE04OSPS', 'SF01B', '2A-CTDPFA107')
            m.get(am.get_events_url(*call_args), text=events_response)
            return am.get_events(*call_args).cals

    @unittest.skip('not yet implemented')
    def test_optaa_coeffs(self):
        """
        With the OPTAA, we have multiple coefficients which are large arrays, the largest of which
        are approximately 2400 element 2-D arrays. When tiled out to the length of the time dimension,
        these arrays use huge amounts of memory.

        Special handling has been added to the coefficient store to return an array which is backed by
        a view of the actual coefficient data but appears to be that data tiled out over the time dimension.

        This test exists to verify that functionality. By asking for 5,000,000 data points we can verify
        that we can produce a virtual array which would occupy more memory than we physically have...

        50,000,000 * 83 * 24 * 8 = 796,800,000,000 (796GiB)
        :return:
        """
        tcarray = np.random.random((83,24))
        cals = {'CC_tcarray': tcarray}
        coefficients = {k: [{'start': 1, 'stop': 100, 'value': cals[k], 'deployment': 3}] for k in cals}
        cstore = CalibrationCoefficientStore(coefficients)
        time = np.linspace(1, 100, num=50000000, endpoint=False)

        cal, meta = cstore.get('CC_tcarray', 3, time)
        np.testing.assert_array_equal(cal[4999999], tcarray)
        np.testing.assert_array_equal(cal.base, tcarray)

