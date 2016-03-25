import json
import logging
import os
import unittest

import mock
import numpy as np
import xarray as xr
from ion_functions.data.ctd_functions import ctd_sbe16plus_tempwat, ctd_pracsal

from preload_database.database import initialize_connection, open_connection, PreloadDatabaseMode
from preload_database.model.preload import Parameter
from util.advlogging import jdefault
from util.asset_management import AssetEvents
from util.common import StreamKey
from util.stream_dataset import StreamDataset

TEST_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(TEST_DIR, 'data')
initialize_connection(PreloadDatabaseMode.POPULATED_MEMORY)
open_connection()

logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)


class StreamDatasetTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.nutnr_sk = StreamKey('CE04OSPS', 'SF01B', '4A-NUTNRA102', 'streamed', 'nutnr_a_sample')
        cls.ctdpf_sk = StreamKey('CE04OSPS', 'SF01B', '2A-CTDPFA107', 'streamed', 'ctdpf_sbe43_sample')
        cls.nutnr_fn = 'nutnr_a_sample.nc'
        cls.ctdpf_fn = 'ctdpf_sbe43_sample.nc'
        cls.ctd_events = json.load(open(os.path.join(DATA_DIR, 'CE04OSPS-SF01B-2A-CTDPFA107_events.json')))
        cls.nut_events = json.load(open(os.path.join(DATA_DIR, 'CE04OSPS-SF01B-4A-NUTNRA102_events.json')))
        cls.ctd_events = AssetEvents(cls.ctdpf_sk.as_three_part_refdes(),
                                     json.load(open(os.path.join(DATA_DIR, 'CE04OSPS-SF01B-2A-CTDPFA107_events.json'))))
        cls.nut_events = AssetEvents(cls.nutnr_sk.as_three_part_refdes(),
                                     json.load(open(os.path.join(DATA_DIR, 'CE04OSPS-SF01B-4A-NUTNRA102_events.json'))))

    def assert_parameters_in_datasets(self, datasets, parameters):
        for dataset in datasets.itervalues():
            for parameter in parameters:
                self.assertIn(parameter, dataset)

    def test_calculate_internal_single_deployment(self):
        ctd_ds = xr.open_dataset(os.path.join(DATA_DIR, self.ctdpf_fn), decode_times=False)
        ctd_ds = ctd_ds[['obs', 'time', 'deployment', 'temperature', 'pressure',
                         'pressure_temp', 'conductivity', 'ext_volt0']]

        ctd_stream_dataset = StreamDataset(self.ctdpf_sk, {}, [], 'UNIT')
        ctd_stream_dataset.events = self.ctd_events
        ctd_stream_dataset._insert_dataset(ctd_ds)
        ctd_stream_dataset.calculate_internal()

        for deployment in ctd_stream_dataset.datasets:
            ds = ctd_stream_dataset.datasets[deployment]
            tempwat = ctd_sbe16plus_tempwat(ds.temperature,
                                            ctd_stream_dataset.events.get_cal('CC_a0', deployment).value,
                                            ctd_stream_dataset.events.get_cal('CC_a1', deployment).value,
                                            ctd_stream_dataset.events.get_cal('CC_a2', deployment).value,
                                            ctd_stream_dataset.events.get_cal('CC_a3', deployment).value)
            np.testing.assert_array_equal(ds.seawater_temperature, tempwat)

            pracsal = ctd_pracsal(ds.seawater_conductivity,
                                  ds.seawater_temperature,
                                  ds.seawater_pressure)
            np.testing.assert_array_equal(ds.practical_salinity, pracsal)

    def test_calculate_internal_multiple_deployments(self):
        ctd_ds = xr.open_dataset(os.path.join(DATA_DIR, self.ctdpf_fn), decode_times=False)
        ctd_ds = ctd_ds[['obs', 'time', 'deployment', 'temperature', 'pressure',
                         'pressure_temp', 'conductivity', 'ext_volt0']]

        # remap times to make this two separate deployments
        dep1_start = self.ctd_events.deps[1].ntp_start
        dep2_stop = self.ctd_events.deps[2].ntp_start + 864000
        ctd_ds.time.values = np.linspace(dep1_start+1, dep2_stop-1, num=ctd_ds.time.shape[0])

        ctd_stream_dataset = StreamDataset(self.ctdpf_sk, {}, [], 'UNIT')
        ctd_stream_dataset.events = self.ctd_events
        ctd_stream_dataset._insert_dataset(ctd_ds)
        ctd_stream_dataset.calculate_internal()

        for deployment in ctd_stream_dataset.datasets:
            ds = ctd_stream_dataset.datasets[deployment]
            tempwat = ctd_sbe16plus_tempwat(ds.temperature,
                                            ctd_stream_dataset.events.get_cal('CC_a0', deployment).value,
                                            ctd_stream_dataset.events.get_cal('CC_a1', deployment).value,
                                            ctd_stream_dataset.events.get_cal('CC_a2', deployment).value,
                                            ctd_stream_dataset.events.get_cal('CC_a3', deployment).value)
            np.testing.assert_array_equal(ds.seawater_temperature, tempwat)

            pracsal = ctd_pracsal(ds.seawater_conductivity,
                                  ds.seawater_temperature,
                                  ds.seawater_pressure)
            np.testing.assert_array_equal(ds.practical_salinity, pracsal)

    def test_calculate_external_single_deployment(self):
        ctd_ds = xr.open_dataset(os.path.join(DATA_DIR, self.ctdpf_fn), decode_times=False)
        nut_ds = xr.open_dataset(os.path.join(DATA_DIR, self.nutnr_fn), decode_times=False)

        ctd_ds = ctd_ds[['obs', 'time', 'deployment', 'temperature', 'pressure',
                         'pressure_temp', 'conductivity', 'ext_volt0']]
        nut_ds = nut_ds[['obs', 'time', 'deployment', 'spectral_channels',
                         'frame_type', 'nutnr_dark_value_used_for_fit']]

        ctd_stream_dataset = StreamDataset(self.ctdpf_sk, {}, [], 'UNIT')
        ctd_stream_dataset.events = self.ctd_events
        ctd_stream_dataset._insert_dataset(ctd_ds)
        ctd_stream_dataset.calculate_internal()

        nut_stream_dataset = StreamDataset(self.nutnr_sk, {}, [self.ctdpf_sk], 'UNIT')
        nut_stream_dataset.events = self.nut_events
        nut_stream_dataset._insert_dataset(nut_ds)
        nut_stream_dataset.calculate_internal()

        nut_stream_dataset.interpolate_needed({self.ctdpf_sk: ctd_stream_dataset})
        nut_stream_dataset.calculate_external()

        expected_params = ['ctdpf_sbe43_sample-seawater_temperature',
                           'ctdpf_sbe43_sample-practical_salinity',
                           'temp_sal_corrected_nitrate']
        self.assert_parameters_in_datasets(nut_stream_dataset.datasets, expected_params)

    def test_calculate_external_multiple_deployments(self):
        ctd_ds = xr.open_dataset(os.path.join(DATA_DIR, self.ctdpf_fn), decode_times=False)
        nut_ds = xr.open_dataset(os.path.join(DATA_DIR, self.nutnr_fn), decode_times=False)

        ctd_ds = ctd_ds[['obs', 'time', 'deployment', 'temperature', 'pressure',
                         'pressure_temp', 'conductivity', 'ext_volt0']]
        nut_ds = nut_ds[['obs', 'time', 'deployment', 'spectral_channels',
                         'frame_type', 'nutnr_dark_value_used_for_fit']]

        # remap times to make this two separate deployments
        dep1_start = self.ctd_events.deps[1].ntp_start
        dep2_stop = self.ctd_events.deps[2].ntp_start + 864000
        ctd_ds.time.values = np.linspace(dep1_start + 1, dep2_stop - 1, num=ctd_ds.time.shape[0])
        nut_ds.time.values = np.linspace(dep1_start + 1, dep2_stop - 1, num=nut_ds.time.shape[0])

        ctd_stream_dataset = StreamDataset(self.ctdpf_sk, {}, [], 'UNIT')
        ctd_stream_dataset.events = self.ctd_events
        ctd_stream_dataset._insert_dataset(ctd_ds)
        ctd_stream_dataset.calculate_internal()

        nut_stream_dataset = StreamDataset(self.nutnr_sk, {}, [self.ctdpf_sk], 'UNIT')
        nut_stream_dataset.events = self.nut_events
        nut_stream_dataset._insert_dataset(nut_ds)
        nut_stream_dataset.calculate_internal()

        nut_stream_dataset.interpolate_needed({self.ctdpf_sk: ctd_stream_dataset})
        nut_stream_dataset.calculate_external()

        expected_params = ['ctdpf_sbe43_sample-seawater_temperature',
                           'ctdpf_sbe43_sample-practical_salinity',
                           'temp_sal_corrected_nitrate']
        self.assert_parameters_in_datasets(nut_stream_dataset.datasets, expected_params)

    def test_log_algorithm_inputs(self):
        def mock_write(self):
            return json.dumps(self.m_qdata, default=jdefault)

        uflags = {'advancedStreamEngineLogging': True, 'userName': 'test'}
        ctd_ds = xr.open_dataset(os.path.join(DATA_DIR, self.ctdpf_fn), decode_times=False)
        ctd_ds = ctd_ds[['obs', 'time', 'deployment', 'temperature', 'pressure',
                         'pressure_temp', 'conductivity', 'ext_volt0']]

        ctd_stream_dataset = StreamDataset(self.ctdpf_sk, uflags, [], 'UNIT')
        ctd_stream_dataset.events = self.ctd_events
        ctd_stream_dataset._insert_dataset(ctd_ds)

        parameter = Parameter.query.get(911)
        with mock.patch('util.stream_dataset.ParameterReport.write', new=mock_write):
            result = ctd_stream_dataset._log_algorithm_inputs(parameter, {}, np.array([1, 2, 3]), self.ctdpf_sk, ctd_ds)
            self.assertIsNotNone(result)

    def test_log_algorithm_inputs_no_result(self):
        def mock_write(self):
            return json.dumps(self.m_qdata, default=jdefault)

        uflags = {'advancedStreamEngineLogging': True, 'userName': 'test'}
        ctd_ds = xr.open_dataset(os.path.join(DATA_DIR, self.ctdpf_fn), decode_times=False)
        ctd_ds = ctd_ds[['obs', 'time', 'deployment', 'temperature', 'pressure',
                         'pressure_temp', 'conductivity', 'ext_volt0']]

        ctd_stream_dataset = StreamDataset(self.ctdpf_sk, uflags, [], 'UNIT')
        ctd_stream_dataset.events = self.ctd_events
        ctd_stream_dataset._insert_dataset(ctd_ds)

        parameter = Parameter.query.get(911)
        with mock.patch('util.stream_dataset.ParameterReport.write', new=mock_write):
            result = ctd_stream_dataset._log_algorithm_inputs(parameter, {}, None, self.ctdpf_sk, ctd_ds)
            self.assertIsNotNone(result)
