import csv
import json
import logging
import os
import unittest
from bisect import bisect
from fnmatch import fnmatch
from glob import glob

import javaobj
import psycopg2
import requests_mock
import numpy as np
import time

from datetime import datetime

from xlrd import XLRDError

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
        self.conn = psycopg2.connect(host='localhost', database='metadata', user='awips')

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
            json.dump(events.events, open('%s_events.json' % refdes, 'w'))

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
            cal = cals.get(dep, {}).get(name)
            self.assertIsNotNone(cal, msg='Missing cal value for %s %s' % (name, dep))

            np.testing.assert_almost_equal(value, cal[0][0], decimal=5)

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

    def assert_cal_times(self, calibrations, asset, dstart, dstop):
        if not calibrations:
            return

        # verify that one of the calibrations is valid for this deployment
        cal_dict = {}
        for cal in calibrations:
            name = cal['name']
            # print name, len(cal['calData'])
            for each in cal['calData']:
                cstart = each['eventStartTime']

                cal_asset = each['assetUid']
                self.assertEqual(asset, cal_asset)
                # print name, cal_asset, cstart
                cal_dict.setdefault(name, []).append(cstart)

        # assert that there are an equal number of cals for each deployment
        lengths = {len(x) for x in cal_dict.values()}
        if len(lengths) != 1:
            print 'missing cal values? asset: %s cals: %s' % (asset, cal_dict)
        # self.assertEqual(len(lengths), 1, msg='missing cal values? asset: %s cals: %s' % (asset, cal_dict))
        import pprint
        pprint.pprint(cal_dict)

    def assert_events(self, events):
        self.assertEqual(len(events), 1)
        now = time.time() * 1000
        deployment = events[0]
        dstart = deployment['eventStartTime']
        dstop = deployment['eventStopTime']
        sensor = deployment['sensor']
        asset = sensor['uid']
        calibrations = sensor['calibration']
        if not dstop:
            dstop = now

        self.assert_cal_times(calibrations, asset, dstart, dstop)

    def test_all_cals(self):
        import requests
        base_url = 'http://localhost:12587/events/deployment/inv'
        subsites = requests.get(base_url).json()
        for subsite in subsites:
            nodes = requests.get('%s/%s' % (base_url, subsite)).json()
            for node in nodes:
                sensors = requests.get('%s/%s/%s' % (base_url, subsite, node)).json()
                for sensor in sensors:
                    deployments = requests.get('%s/%s/%s/%s' % (base_url, subsite, node, sensor)).json()
                    for deployment in deployments:
                        events = requests.get('%s/%s/%s/%s/%d' % (base_url, subsite, node, sensor, deployment)).json()
                        print subsite, node, sensor, deployment
                        self.assert_events(events)
            break
            # print subsite, node, sensor, deployment, events
        assert subsites is False

    def make_array(self, dimensions, values):
        if dimensions == [1] and len(values) == 1:
            return values[0]

        a = np.array(values)
        return list(np.reshape(a, dimensions))

    def assert_cal_value(self, uid, start, name, value):
        # print repr(uid), start, repr(name), repr(value)
        cur = self.conn.cursor()
        cur.execute(
            'select name, dimensions, values, eventstarttime from xcalibration xc, xcalibrationdata xcd, xevent xe where xc.calid = xcd.calid and xe.eventid = xcd.eventid and assetuid=%s and name=%s',
            (uid, name))
        rows = cur.fetchall()
        values = []
        for name, dims, vals, t in rows:
            try:
                if dims is not None and vals is not None:
                    dims = javaobj.loads(dims)
                    vals = javaobj.loads(vals)
                    a = self.make_array(dims, vals)
                    t /= 1000.0
                    values.append((t, a))
            except IOError:
                print name, repr(dims), repr(vals)

        if not values:
            rval = 'missing', name, uid, start, datetime.utcfromtimestamp(start).isoformat(), value, values
            print rval
            return rval

        values.sort()
        times = [v[0] for v in values]
        index = bisect(times, start)

        if index <= 0:
            rval = 'notfound', name, uid, start, datetime.utcfromtimestamp(start).isoformat(), value, values
            print rval
            return rval

        t, expected = values[index - 1]
        try:
            np.testing.assert_array_almost_equal(expected, value)
        except AssertionError:
            rval = 'mismatch', name, uid, start, datetime.utcfromtimestamp(start).isoformat(), value, values
            print rval
            return rval

    def test_new_spreadsheets(self):
        import pandas as pd
        unix_epoch = datetime(1970, 1, 1)
        results = []
        for filename in glob('/Users/petercable/newam/*Cal*'):
            sheets = pd.read_excel(filename, sheetname=None)
            for df in sheets.values():
                try:
                    df = df[
                        ['Sensor.uid', 'startDateTime', 'Calibration Cofficient Name', 'Calibration Cofficient Value']]
                    df = df.dropna(how='all')
                    for each in df.itertuples(index=False):
                        uid, start, name, value = each
                        if (isinstance(uid, float) and np.isnan(uid) or
                                    isinstance(name, float) and np.isnan(name) or
                                    isinstance(value, float) and np.isnan(value)):
                            continue

                        if isinstance(value, basestring):
                            try:
                                value = json.loads(value)
                            except ValueError:
                                continue
                        start = (start - unix_epoch).total_seconds()
                        result = self.assert_cal_value(uid, start, name, value)
                        if result:
                            results.append(result)
                except (XLRDError, KeyError):
                    pass

        print len(results)
        with open('new_results.csv', 'w') as fh:
            writer = csv.writer(fh)
            writer.writerows(results)

    def test_old_spreadsheets(self):
        import pandas as pd
        skip = ['CC_lat', 'CC_lon', 'CC_latitude', 'CC_longitude', 'Inductive ID', 'Induction ID', 'CC_depth']
        unix_epoch = datetime(1970, 1, 1)
        results = []
        for filename in glob('/Users/petercable/src/asset-management/deployment/Omaha_Cal*xlsx'):
            print filename
            sheets = pd.read_excel(filename, sheetname=None)
            if 'Moorings' in sheets and 'Asset_Cal_Info' in sheets:
                moorings = sheets['Moorings']
                moorings = moorings[['Mooring OOIBARCODE', 'Deployment Number', 'Anchor Launch Date', 'Anchor Launch Time']]
                moorings = moorings.dropna(how='any')
                deployments = {}

                for row in moorings.itertuples(index=False):
                    barcode, number, date, rowtime = row
                    print barcode, number, date, rowtime,

                    if date == rowtime:
                        dt = date
                    else:
                        if isinstance(time, datetime):
                            rowtime = rowtime.time()
                        dt = datetime.combine(date.date(), rowtime)

                    print dt
                    deployments[(barcode, int(number))] = dt

                all_moorings = list({x[0] for x in deployments})

                df = sheets['Asset_Cal_Info']
                try:
                    df = df[
                        ['Mooring OOIBARCODE', 'Sensor OOIBARCODE', 'Deployment Number',
                         'Calibration Cofficient Name', 'Calibration Cofficient Value']]
                    df = df.dropna(how='all')
                    for each in df.itertuples(index=False):
                        mooring_uid, uid, deployment, name, value = each

                        if name in skip:
                            continue

                        if mooring_uid is None or isinstance(mooring_uid, float) and np.isnan(mooring_uid):
                            if len(all_moorings) == 1:
                                mooring_uid = all_moorings[0]

                        start = deployments.get((uid, deployment))
                        if start is None:
                            start = deployments.get((mooring_uid, deployment))
                        if start is None:
                            print 'deployment not found', each, deployments
                            continue

                        if (isinstance(uid, float) and np.isnan(uid) or
                                    isinstance(name, float) and np.isnan(name) or
                                    isinstance(value, float) and np.isnan(value)):
                            continue

                        if isinstance(value, basestring):
                            try:
                                value = json.loads(value)
                            except ValueError:
                                continue
                        start = (start - unix_epoch).total_seconds()

                        result = self.assert_cal_value(uid, start, name, value)
                        if result:
                            results.append(result)
                except (XLRDError, KeyError) as e:
                    print e

        print len(results)
        with open('old_results.csv', 'w') as fh:
            writer = csv.writer(fh)
            writer.writerow(('reason', 'name', 'uid', 'start', 'start_ts', 'value', 'retrieved'))
            writer.writerows(results)