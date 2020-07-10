import os
import mock
import json
import logging
import unittest
import requests_mock
import xarray as xr
import pandas as pd

from copy import deepcopy
from util.asset_management import AssetEvents
from util.qartod_service import qartodTestServiceAPI, QartodTestRecord
from util.common import StreamKey, TimeRange, QartodFlags, QARTOD_PRIMARY, QARTOD_SECONDARY
from util.stream_request import StreamRequest
from util.stream_dataset import StreamDataset


TEST_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(TEST_DIR, 'data')


logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)


NUTNR_QARTOD_RECORD_1 = QartodTestRecord(id=1,
                                         refDes={"subsite": "CE04OSPS", "node": "SF01B", "sensor": "4A-NUTNRA102",
                                                 "full": True},
                                         stream='nutnr_a_sample',
                                         parameters='{"inp": "pressure"}',
                                         qcConfig='{"qartod": {"gross_range_test": {"suspect_span": [28.0, 33.8], '
                                                '"fail_span": [0.0, 42.0]}}}')

NUTNR_QARTOD_RECORD_2 = QartodTestRecord(id=2,
                                         refDes={"subsite": "CE04OSPS", "node": "SF01B", "sensor": "4A-NUTNRA102",
                                                 "full": True},
                                         stream='nutnr_a_sample',
                                         parameters='{"inp": "salinity_corrected_nitrate"}',
                                         qcConfig='{"qartod": {"gross_range_test": {"suspect_span": [28.0, 33.8], '
                                                '"fail_span": [0.0, 42.0]}}}')

CTDBP_QARTOD_RECORD_1 = QartodTestRecord(id=3,
                                         refDes={"subsite": "CE04OSPS", "node": "SF01B", "sensor": "2A-CTDPFA107",
                                                 "full": True},
                                         stream='ctdpf_sbe43_sample',
                                         parameters='{"inp": "pressure"}',
                                         qcConfig='{"qartod": {"gross_range_test": {"suspect_span": [28.0, 33.8], '
                                                '"fail_span": [0.0, 42.0]}}}')

CTDBP_QARTOD_RECORD_2 = QartodTestRecord(id=4,
                                         refDes={"subsite": "CE04OSPS", "node": "SF01B", "sensor": "2A-CTDPFA107",
                                                 "full": True},
                                         stream='ctdpf_sbe43_sample',
                                         parameters='{"inp": "temperature"}',
                                         qcConfig='{"qartod": {"gross_range_test": {"suspect_span": [28.0, 33.8], '
                                                '"fail_span": [0.0, 42.0]}}}')

CTDBP_QARTOD_RECORD_3 = QartodTestRecord(id=5,
                                         refDes={"node": "LJ01D", "subsite": "CE02SHBP", "full": True,
                                                 "sensor": "06-CTDBPN106"},
                                         stream='ctdbp_no_sample',
                                         parameters='{"inp": "practical_salinity"}',
                                         qcConfig='{"qartod": {"gross_range_test": {"suspect_span": [28.0, 33.8], '
                                                '"fail_span": [0.0, 42.0]}}}')

metadata = pd.read_csv(os.path.join(DATA_DIR, 'stream_metadata.csv'))


def get_available_time_range(sk):
    rows = metadata[(metadata.subsite == sk.subsite) &
                    (metadata.node == sk.node) &
                    (metadata.sensor == sk.sensor) &
                    (metadata.method == sk.method) &
                    (metadata.stream == sk.stream.name)]
    for index, row in rows.iterrows():
        return TimeRange(row['first'], row['last'] + 1)


def get_stream_metadata():
    return [row[1:6] for row in metadata.itertuples()]


@mock.patch('util.metadata_service.stream.get_available_time_range', new=get_available_time_range)
@mock.patch('util.metadata_service.stream._get_stream_metadata', new=get_stream_metadata)
class QartodTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ctd_sk = StreamKey('CE04OSPS', 'SF01B', '2A-CTDPFA107', 'streamed', 'ctdpf_sbe43_sample')
        cls.nut_sk = StreamKey('CE04OSPS', 'SF01B', '4A-NUTNRA102', 'streamed', 'nutnr_a_sample')

        cls.ctd_events = AssetEvents(cls.ctd_sk.as_three_part_refdes(),
                                     json.load(open(os.path.join(DATA_DIR, 'CE04OSPS-SF01B-2A-CTDPFA107_events.json'))))
        cls.nut_events = AssetEvents(cls.nut_sk.as_three_part_refdes(),
                                     json.load(open(os.path.join(DATA_DIR, 'CE04OSPS-SF01B-4A-NUTNRA102_events.json'))))

        cls.base_params = ['time', 'deployment', 'provenance']

        cls.qartod_find_url = '/'.join((qartodTestServiceAPI.base_url, 'find'))

    @staticmethod
    def get_modified_ctdbp_record(**kwargs):
        record = deepcopy(CTDBP_QARTOD_RECORD_3)
        record.__dict__.update(kwargs)
        return record

    def check_ctdbp_record_expansion(self, record, expected):
        parameters = ['ctdbp_no_seawater_conductivity', 'ctdbp_no_seawater_pressure', 'practical_salinity',
                      'seawater_temperature']
        result = qartodTestServiceAPI.expand_qartod_record(record, 'CE02SHBP', 'LJ01D', '06-CTDBPN106',
                                                           'ctdbp_no_sample', parameters)
        self.assertItemsEqual(expected, result)

    def create_nut_sr(self):
        nutnr_fn = 'nutnr_a_sample.nc'
        ctdpf_fn = 'ctdpf_sbe43_sample.nc'
        qc = json.load(open(os.path.join(DATA_DIR, 'qc.json')))

        tr = TimeRange(3.65342400e+09, 3.65351040e+09)
        sr = StreamRequest(self.nut_sk, [18], tr, {}, qc_parameters=qc, request_id='UNIT')
        nutnr_ds = xr.open_dataset(os.path.join(DATA_DIR, nutnr_fn), decode_times=False)
        ctdpf_ds = xr.open_dataset(os.path.join(DATA_DIR, ctdpf_fn), decode_times=False)

        nutnr_ds = nutnr_ds[self.base_params + [p.name for p in sr.stream_parameters[self.nut_sk]]]
        ctdpf_ds = ctdpf_ds[self.base_params + [p.name for p in sr.stream_parameters[self.ctd_sk]]]

        sr.datasets[self.ctd_sk] = StreamDataset(self.ctd_sk, sr.uflags, [self.nut_sk], sr.request_id)
        sr.datasets[self.nut_sk] = StreamDataset(self.nut_sk, sr.uflags, [self.ctd_sk], sr.request_id)
        sr.datasets[self.ctd_sk].events = self.ctd_events
        sr.datasets[self.nut_sk].events = self.nut_events
        sr.datasets[self.ctd_sk]._insert_dataset(ctdpf_ds)
        sr.datasets[self.nut_sk]._insert_dataset(nutnr_ds)
        return sr

    @requests_mock.mock()
    def calculate_nut_sr(self, m):
        """
        Calculate a dataset and run QARTOD tests on it. Return the dataset for examination.
        """
        # Mock calls to EDEX to fetch QARTOD test records
        # requests_mock.response chokes when trying to serialize QartodTestRecord objects, so pass the serialized JSON
        # via the 'text' argument for request mocking instead of using the 'json' argument
        nut_json = "[%s, %s]" % (NUTNR_QARTOD_RECORD_1, NUTNR_QARTOD_RECORD_2)
        nut_path = self.qartod_find_url + "?sensor=4A-NUTNRA102"
        m.get(nut_path, text=nut_json)
        ctd_json = "[%s, %s]" % (CTDBP_QARTOD_RECORD_1, CTDBP_QARTOD_RECORD_2)
        ctd_path = self.qartod_find_url + "?sensor=2A-CTDPFA107"
        m.get(ctd_path, text=ctd_json)

        sr = self.create_nut_sr()
        sr.calculate_derived_products()
        sr.import_extra_externals()
        sr.execute_qartod_qc()
        sr.insert_provenance()
        return sr

    def test_expand_qartod_record_for_null_subsite(self):
        record = self.get_modified_ctdbp_record(subsite=None)
        expected = [CTDBP_QARTOD_RECORD_3]
        self.check_ctdbp_record_expansion(record, expected)

    def test_expand_qartod_record_for_null_node(self):
        record = self.get_modified_ctdbp_record(node=None)
        expected = [CTDBP_QARTOD_RECORD_3]
        self.check_ctdbp_record_expansion(record, expected)

    def test_expand_qartod_record_for_null_sensor(self):
        record = self.get_modified_ctdbp_record(sensor=None)
        expected = [CTDBP_QARTOD_RECORD_3]
        self.check_ctdbp_record_expansion(record, expected)

    def test_expand_qartod_record_for_null_stream(self):
        record = self.get_modified_ctdbp_record(stream=None)
        expected = [CTDBP_QARTOD_RECORD_3]
        self.check_ctdbp_record_expansion(record, expected)

    def test_expand_qartod_record_for_parameters_null_in_json(self):
        record = self.get_modified_ctdbp_record(parameters='{"inp": null}')

        record_a = self.get_modified_ctdbp_record(parameters='{"inp": "ctdbp_no_seawater_conductivity"}')
        record_b = self.get_modified_ctdbp_record(parameters='{"inp": "ctdbp_no_seawater_pressure"}')
        record_c = self.get_modified_ctdbp_record(parameters='{"inp": "practical_salinity"}')
        record_d = self.get_modified_ctdbp_record(parameters='{"inp": "seawater_temperature"}')
        expected = [record_a, record_b, record_c, record_d]

        self.check_ctdbp_record_expansion(record, expected)

    def test_expand_qartod_record_for_null_parameters(self):
        record = self.get_modified_ctdbp_record(parameters=None)

        record_a = self.get_modified_ctdbp_record(parameters='{"inp": "ctdbp_no_seawater_conductivity"}')
        record_b = self.get_modified_ctdbp_record(parameters='{"inp": "ctdbp_no_seawater_pressure"}')
        record_c = self.get_modified_ctdbp_record(parameters='{"inp": "practical_salinity"}')
        record_d = self.get_modified_ctdbp_record(parameters='{"inp": "seawater_temperature"}')
        expected = [record_a, record_b, record_c, record_d]

        self.check_ctdbp_record_expansion(record, expected)

    def test_expand_qartod_record_for_multiple_nulls(self):
        record = self.get_modified_ctdbp_record(subsite=None, node=None, sensor=None, stream=None, parameters='{"inp": null}')

        record_a = self.get_modified_ctdbp_record(parameters='{"inp": "ctdbp_no_seawater_conductivity"}')
        record_b = self.get_modified_ctdbp_record(parameters='{"inp": "ctdbp_no_seawater_pressure"}')
        record_c = self.get_modified_ctdbp_record(parameters='{"inp": "practical_salinity"}')
        record_d = self.get_modified_ctdbp_record(parameters='{"inp": "seawater_temperature"}')
        expected = [record_a, record_b, record_c, record_d]

        self.check_ctdbp_record_expansion(record, expected)

    @requests_mock.mock()
    def test_find_qartod_tests_handles_error_status(self, m):
        m.get(self.qartod_find_url, status_code=500, json={'status_code': 500, 'message': 'Internal Server Error'})
        result = qartodTestServiceAPI.find_qartod_tests('CE02SHBP', 'LJ01D', '06-CTDBPN106', 'ctdbp_no_sample',
                                                        'practical_salinity')
        self.assertListEqual(result, [])

    @requests_mock.mock()
    def test_find_qartod_tests_handles_empty_list(self, m):
        m.get(self.qartod_find_url, json=[])
        result = qartodTestServiceAPI.find_qartod_tests('CE02SHBP', 'LJ01D', '06-CTDBPN106', 'ctdbp_no_sample',
                                                        'practical_salinity')
        self.assertListEqual(result, [])

    @requests_mock.mock()
    def test_find_qartod_tests_handles_malformed_response(self, m):
        m.get(self.qartod_find_url, text='This should have been JSON...')
        result = qartodTestServiceAPI.find_qartod_tests('CE02SHBP', 'LJ01D', '06-CTDBPN106', 'ctdbp_no_sample',
                                                        'practical_salinity')
        self.assertListEqual(result, [])

    def test_primary_qartod_flags_in_dataset(self):
        sr = self.calculate_nut_sr()
        self.assertIn('salinity_corrected_nitrate' + QARTOD_PRIMARY, sr.datasets[self.nut_sk].datasets[2])
        self.assertIn('pressure' + QARTOD_PRIMARY, sr.datasets[self.nut_sk].datasets[2])
        self.assertIn('pressure' + QARTOD_PRIMARY, sr.datasets[self.ctd_sk].datasets[2])
        self.assertIn('temperature' + QARTOD_PRIMARY, sr.datasets[self.ctd_sk].datasets[2])

    def test_secondary_qartod_flags_in_dataset(self):
        sr = self.calculate_nut_sr()
        self.assertIn('salinity_corrected_nitrate' + QARTOD_SECONDARY, sr.datasets[self.nut_sk].datasets[2])
        self.assertIn('pressure' + QARTOD_SECONDARY, sr.datasets[self.nut_sk].datasets[2])
        self.assertIn('pressure' + QARTOD_SECONDARY, sr.datasets[self.ctd_sk].datasets[2])
        self.assertIn('temperature' + QARTOD_SECONDARY, sr.datasets[self.ctd_sk].datasets[2])

    def test_qartod_flag_attribute_long_names(self):
        sr = self.calculate_nut_sr()
        nitrate_var = 'salinity_corrected_nitrate' + QARTOD_PRIMARY
        pressure_var = 'pressure' + QARTOD_SECONDARY
        log.warn(sr.datasets[self.nut_sk].datasets[2])
        self.assertEqual('Nitrate Concentration - Temp and Sal Corrected QARTOD Summary Flag', 
                         sr.datasets[self.nut_sk].datasets[2][nitrate_var].attrs['long_name'])
        self.assertEqual('Seawater Pressure Measurement Individual QARTOD Flags', 
                         sr.datasets[self.ctd_sk].datasets[2][pressure_var].attrs['long_name'])

    def test_primary_qartod_flag_attribute_flag_values(self):
        sr = self.calculate_nut_sr()
        nitrate_var = 'salinity_corrected_nitrate' + QARTOD_PRIMARY
        self.assertListEqual(QartodFlags.getValidQCFlags(),
                             sr.datasets[self.nut_sk].datasets[2][nitrate_var].attrs['flag_values'].tolist())

    def test_primary_qartod_flag_attribute_flag_meanings(self):
        sr = self.calculate_nut_sr()
        nitrate_var = 'salinity_corrected_nitrate' + QARTOD_PRIMARY
        self.assertEqual(' '.join(QartodFlags.getQCFlagMeanings()),
                         sr.datasets[self.nut_sk].datasets[2][nitrate_var].attrs['flag_meanings'])

    def test_qartod_flag_variable_attributes_tests_executed(self):
        sr = self.calculate_nut_sr()
        nitrate_var = 'salinity_corrected_nitrate' + QARTOD_SECONDARY
        self.assertEqual('gross_range_test', sr.datasets[self.nut_sk].datasets[2][nitrate_var].attrs['tests_executed'])
