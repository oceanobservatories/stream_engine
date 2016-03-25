import json
import logging
import os
import unittest

import numpy as np
import requests_mock

from util.asset_management import AssetManagement

logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)

TEST_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(TEST_DIR, 'data')
events_response = open(os.path.join(DATA_DIR, 'CE04OSPS-SF01B-2A-CTDPFA107_events.json')).read()


class AssetManagementTest(unittest.TestCase):
    def setUp(self):
        self.amhost = 'localhost'
        self.refdes = 'CE04OSPS-SF01B-2A-CTDPFA107'

    def test_create_am(self):
        return AssetManagement(self.amhost)

    @unittest.skip('Used to fetch event data')
    def test_get(self):
        targets = [
            ('CP05MOAS', 'GL388', '04-DOSTAM000'),
            ('CP05MOAS', 'GL388', '03-CTDGVM000'),
            ('CP05MOAS', 'GL388', '00-ENG000000'),
            ('CP02PMUO', 'WFP01', '05-PARADK000'),
            ('CP02PMUO', 'WFP01', '03-CTDPFK000'),
            ('CE04OSPS', 'SF01B', '4A-NUTNRA102'),
            ('CE04OSPS', 'SF01B', '2A-CTDPFA107'),
            ('RS01SLBS', 'LJ01A', '05-HPIESA101'),
            ('GI01SUMO', 'SBD11', '06-METBKA000'),
            ('GI01SUMO', 'RID16', '04-VELPTA000'),
            ('CE05MOAS', 'GL319', '05-CTDGVM000'),
            ('CE05MOAS', 'GL319', '01-PARADM000'),
        ]
        am = self.test_create_am()
        futures = []
        for subsite, node, sensor in targets:
            refdes = '-'.join((subsite, node, sensor))
            futures.append((refdes, am.get_events_async(refdes)))

        for refdes, f in futures:
            events = f.result()
            json.dump(events.events, open('%s_events.json' % refdes, 'w'), indent=4, separators=(',', ': '))

    def test_get_calibrations(self):
        am = self.test_create_am()
        with requests_mock.Mocker() as m:
            m.get(am.base_url, text=events_response)
            return am.get_events(self.refdes).cals

    def test_get_calibrations_async(self):
        am = self.test_create_am()
        with requests_mock.Mocker() as m:
            m.get(am.base_url, text=events_response)
            return am.get_events_async(self.refdes).result().cals

    def test_get_events_async(self):
        am = self.test_create_am()
        with requests_mock.Mocker() as m:
            m.get(am.base_url, text=events_response)
            future = am.get_events_async(self.refdes)
            return future.result()

    def test_expected_calibrations(self):
        cals = self.test_get_calibrations_async()

        expected = [
            # DOFST, deployment 1
            (1, 'CC_voltage_offset', 0.502),
            (1, 'CC_frequency_offset', 0.502),
            (1, 'CC_oxygen_signal_slope', -0.5229),
            (1, 'CC_residual_temperature_correction_factor_a', -0.0031307),
            (1, 'CC_residual_temperature_correction_factor_b', 0.00015977),
            (1, 'CC_residual_temperature_correction_factor_c', -0.0000030854),
            (1, 'CC_residual_temperature_correction_factor_e', 0.036),
            # (1, 'CC_latitude', 44.37414),
            # (1, 'CC_longitude', -124.9565267),
            # CTD, deployment 1
            # (1, 'CC_lat', 44.37414),
            # (1, 'CC_lon', -124.9565267),
            (1, 'CC_a0', 0.001246087),
            (1, 'CC_a1', 0.0002744662),
            (1, 'CC_a2', -0.000001053524),
            (1, 'CC_a3', 0.000000178513),
            (1, 'CC_cpcor', -0.0000000957),
            (1, 'CC_ctcor', 0.00000325),
            (1, 'CC_g', -1.002191),
            (1, 'CC_h', 0.1438346),
            (1, 'CC_i', -0.0002759555),
            (1, 'CC_j', 0.00004000654),
            (1, 'CC_pa0', 0.1062501),
            (1, 'CC_pa1', 0.001546543),
            (1, 'CC_pa2', 6.464906E-12),
            (1, 'CC_ptempa0', -66.05836),
            (1, 'CC_ptempa1', 52.6279),
            (1, 'CC_ptempa2', -0.4963498),
            (1, 'CC_ptca0', 525508.6),
            (1, 'CC_ptca1', 5.603161),
            (1, 'CC_ptca2', -0.1110844),
            (1, 'CC_ptcb0', 25.04787),
            (1, 'CC_ptcb1', -0.000025),
            (1, 'CC_ptcb2', 0),
            # DOFST, deployment 2
            (2, 'CC_voltage_offset', -0.5199),
            (2, 'CC_frequency_offset', -0.5199),
            (2, 'CC_oxygen_signal_slope', 0.5327),
            (2, 'CC_residual_temperature_correction_factor_a', -0.0024522),
            (2, 'CC_residual_temperature_correction_factor_b', 0.00012028),
            (2, 'CC_residual_temperature_correction_factor_c', -0.0000024619),
            (2, 'CC_residual_temperature_correction_factor_e', 0.036),
            # (2, 'CC_latitude', 44.37414),
            # (2, 'CC_longitude', -124.9565267),
            # CTD, deployment 2
            # (2, 'CC_lat', 44.37414),
            # (2, 'CC_lon', -124.9565267),
            (2, 'CC_a0', 0.001252507),
            (2, 'CC_a1', 0.000277772),
            (2, 'CC_a2', -0.000001743991),
            (2, 'CC_a3', 0.000000204535),
            (2, 'CC_cpcor', -0.0000000957),
            (2, 'CC_ctcor', 0.00000325),
            (2, 'CC_g', -0.9733758),
            (2, 'CC_h', 0.1472746),
            (2, 'CC_i', -0.0001213067),
            (2, 'CC_j', 0.00003036137),
            (2, 'CC_pa0', -0.05980422),
            (2, 'CC_pa1', 0.001717275),
            (2, 'CC_pa2', 8.37E-11),
            (2, 'CC_ptempa0', 169.6927),
            (2, 'CC_ptempa1', -70.72418),
            (2, 'CC_ptempa2', -1.08706),
            (2, 'CC_ptca0', 524732.5),
            (2, 'CC_ptca1', -6.679858),
            (2, 'CC_ptca2', 0.2275047),
            (2, 'CC_ptcb0', 24.8405),
            (2, 'CC_ptcb1', 0.0001),
            (2, 'CC_ptcb2', 0)
        ]

        for dep, name, value in expected:
            cal, _, _ = cals.get(dep, {}).get(name)[0]
            self.assertIsNotNone(cal, msg='Missing cal value for %s %s' % (name, dep))

            np.testing.assert_almost_equal(value, cal, decimal=5)

    def test_get_cal(self):
        events = self.test_get_events_async()
        cal, meta = events.get_tiled_cal('CC_pa2', 2, np.array([1, 2]))
        np.testing.assert_almost_equal(cal, np.array([8.37E-11, 8.37E-11]))

    def test_get_cal_lat(self):
        events = self.test_get_events_async()
        cal, meta = events.get_tiled_cal('CC_latitude', 2, np.array([1, 2]))
        np.testing.assert_almost_equal(cal, np.array([44.37415, 44.37415]))

        cal, meta = events.get_tiled_cal('CC_lat', 2, np.array([1, 2]))
        np.testing.assert_almost_equal(cal, np.array([44.37415, 44.37415]))

    def test_get_cal_lon(self):
        events = self.test_get_events_async()
        cal, meta = events.get_tiled_cal('CC_longitude', 2, np.array([1, 2]))
        np.testing.assert_almost_equal(cal, np.array([-124.95648, -124.95648]))

        cal, meta = events.get_tiled_cal('CC_lon', 2, np.array([1, 2]))
        np.testing.assert_almost_equal(cal, np.array([-124.95648, -124.95648]))
