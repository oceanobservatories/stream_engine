import global_test_setup

import json
import logging
import os
import unittest
import httplib
import ast

import mock
import numpy as np
import pandas as pd
import xarray as xr

from preload_database.database import initialize_connection, open_connection, PreloadDatabaseMode
from preload_database.model.preload import Parameter
from util.asset_management import AssetEvents
from util.common import StreamKey, TimeRange, StreamEngineException, InvalidParameterException
from util.csvresponse import CsvGenerator
from util.jsonresponse import JsonResponse
from util.netcdf_generator import NetcdfGenerator
from util.stream_dataset import StreamDataset
from util.stream_request import StreamRequest
from util.calc import execute_stream_request, validate

TEST_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(TEST_DIR, 'data')
initialize_connection(PreloadDatabaseMode.POPULATED_MEMORY)
open_connection()

logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)
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
class StreamRequestTest(unittest.TestCase):
    metadata = []
    base_params = ['time', 'deployment', 'provenance']
    call_cnt = 0

    @classmethod
    def setUpClass(cls):
        cls.ctd_sk = StreamKey('CE04OSPS', 'SF01B', '2A-CTDPFA107', 'streamed', 'ctdpf_sbe43_sample')
        cls.nut_sk = StreamKey('CE04OSPS', 'SF01B', '4A-NUTNRA102', 'streamed', 'nutnr_a_sample')
        cls.echo_sk = StreamKey('RS01SLBS', 'LJ01A', '05-HPIESA101', 'streamed', 'echo_sounding')
        cls.hourly_sk = StreamKey('GI01SUMO', 'SBD11', '06-METBKA000', 'recovered_host', 'metbk_hourly')
        cls.met_sk = StreamKey('GI01SUMO', 'SBD11', '06-METBKA000', 'recovered_host', 'metbk_a_dcl_instrument_recovered')
        cls.vel_sk = StreamKey('GI01SUMO', 'RID16', '04-VELPTA000', 'recovered_host', 'velpt_ab_dcl_instrument_recovered')

        cls.ctd_events = AssetEvents(cls.ctd_sk.as_three_part_refdes(),
                                     json.load(open(os.path.join(DATA_DIR, 'CE04OSPS-SF01B-2A-CTDPFA107_events.json'))))
        cls.nut_events = AssetEvents(cls.nut_sk.as_three_part_refdes(),
                                     json.load(open(os.path.join(DATA_DIR, 'CE04OSPS-SF01B-4A-NUTNRA102_events.json'))))
        cls.hpies_events = AssetEvents(cls.echo_sk.as_three_part_refdes(),
                                       json.load(open(os.path.join(DATA_DIR, 'RS01SLBS-LJ01A-05-HPIESA101_events.json'))))
        cls.met_events = AssetEvents(cls.met_sk.as_three_part_refdes(),
                                     json.load(open(os.path.join(DATA_DIR, 'GI01SUMO-SBD11-06-METBKA000_events.json'))))
        cls.vel_events = AssetEvents(cls.vel_sk.as_three_part_refdes(),
                                     json.load(open(os.path.join(DATA_DIR, 'GI01SUMO-RID16-04-VELPTA000_events.json'))))

    def assert_parameters_in_datasets(self, datasets, parameters):
        for dataset in datasets.itervalues():
            for parameter in parameters:
                self.assertIn(parameter, dataset)

    def test_basic_stream_request(self):
        sk = StreamKey('CP05MOAS', 'GL388', '03-CTDGVM000', 'recovered_host', 'ctdgv_m_glider_instrument_recovered')
        tr = TimeRange(3.622409e+09, 3.627058e+09)
        sr = StreamRequest(sk, [1527], {}, tr, {}, request_id='UNIT')

    def test_glider_include_preswat_gps(self):
        do_sk = StreamKey('CP05MOAS', 'GL388', '04-DOSTAM000', 'recovered_host', 'dosta_abcdjm_glider_recovered')
        ctd_sk = StreamKey('CP05MOAS', 'GL388', '03-CTDGVM000', 'recovered_host', 'ctdgv_m_glider_instrument_recovered')
        gps_sk = StreamKey('CP05MOAS', 'GL388', '00-ENG000000', 'recovered_host', 'glider_gps_position')
        tr = TimeRange(3.622409e+09, 3.627058e+09)
        sr = StreamRequest(do_sk, [], {}, tr, {}, request_id='UNIT')

        # we expect to fetch the PRESWAT from the ctd glider stream and LAT/LON from the gps position stream
        self.assertEqual(set(sr.stream_parameters), {do_sk, ctd_sk, gps_sk})

    def test_wfp_include_preswat(self):
        par_sk = StreamKey('CP02PMUO', 'WFP01', '05-PARADK000', 'recovered_wfp',
                           'parad_k__stc_imodem_instrument_recovered')
        ctd_sk = StreamKey('CP02PMUO', 'WFP01', '03-CTDPFK000', 'recovered_wfp', 'ctdpf_ckl_wfp_instrument_recovered')
        tr = TimeRange(3594211324.0, 3653837045.0)
        sr = StreamRequest(par_sk, [], {}, tr, {}, request_id='UNIT')

        # we expect to fetch the PRESWAT from the co-located CTD
        self.assertEqual(set(sr.stream_parameters), {par_sk, ctd_sk})

    def test_no_stream_key(self):
        with self.assertRaises(StreamEngineException):
            StreamRequest(None, None, None, None, None)

    def test_empty_stream_key(self):
        with self.assertRaises(StreamEngineException):
            StreamRequest(None, None, None, None, None)

    def test_bad_stream_key(self):
        with self.assertRaises(StreamEngineException):
            StreamRequest('bogus', None, None, None, None)

    def test_need_internal(self):
        sk = StreamKey('RS03AXBS', 'LJ03A', '12-CTDPFB301', 'streamed', 'ctdpf_optode_sample')
        tr = TimeRange(3617736678.149051, 3661524609.0570827)
        sr = StreamRequest(sk, [911], {}, tr, {}, request_id='UNIT')
        # if internal only, no external stream should exist in stream_parameters
        self.assertEqual(set(sr.stream_parameters), {sk})

    def test_need_external(self):
        # nutnr_a_sample requests PD908 and PD911
        tr = TimeRange(3617736678.149051, 3661524609.0570827)
        sr = StreamRequest(self.nut_sk, [], {}, tr, {}, request_id='UNIT')

        self.assertEqual(set(sr.stream_parameters), {self.ctd_sk, self.nut_sk})

    def test_need_dpi(self):
        # OPTAA specifies that it needs dpi_PRACSAL_L2
        # first, an OPTAA with a colocated SBE43
        sk = StreamKey('RS03AXPS', 'SF03A', '3B-OPTAAD301', 'streamed', 'optaa_sample')
        sk2 = StreamKey('RS03AXPS', 'SF03A', '2A-CTDPFA302', 'streamed', 'ctdpf_sbe43_sample')
        tr = TimeRange(3617736678.149051, 3661524609.0570827)
        sr = StreamRequest(sk, [], {}, tr, {}, request_id='UNIT')

        self.assertEqual(set(sr.stream_parameters), {sk, sk2})

        # TODO: uncomment when OPTAA data products restored in preload
        # # now, an OPTAA with a colocated CTDPF/optode
        # sk = StreamKey('RS03AXBS', 'LJ03A', '11-OPTAAC303', 'streamed', 'optaa_sample')
        # sk2 = StreamKey('RS03AXBS', 'LJ03A', '12-CTDPFB301', 'streamed', 'ctdpf_optode_sample')
        # tr = TimeRange(3617736678.149051, 3661524609.0570827)
        # sr = StreamRequest(sk, [], {}, tr, {}, request_id='UNIT')
        #
        # self.assertEqual(set(sr.stream_parameters), {sk, sk2})

    def test_virtual(self):
        sk1 = StreamKey('GI01SUMO', 'SBD11', '06-METBKA000', 'recovered_host', 'metbk_hourly')
        sk2 = StreamKey('GI01SUMO', 'SBD11', '06-METBKA000', 'recovered_host', 'metbk_a_dcl_instrument_recovered')
        sk3 = StreamKey('GI01SUMO', 'RID16', '04-VELPTA000', 'recovered_host', 'velpt_ab_dcl_instrument_recovered')
        tr = TimeRange(3617736678.149051, 3661524609.0570827)
        sr = StreamRequest(sk1, [], {}, tr, {}, request_id='UNIT')
        self.assertEqual(set(sr.stream_parameters), {sk1, sk2, sk3})

    def create_nut_sr(self):
        nutnr_fn = 'nutnr_a_sample.nc'
        ctdpf_fn = 'ctdpf_sbe43_sample.nc'
        qc = json.load(open(os.path.join(DATA_DIR, 'qc.json')))

        tr = TimeRange(3.65342400e+09, 3.65351040e+09)
        sr = StreamRequest(self.nut_sk, [2443], tr, {}, qc_parameters=qc, request_id='UNIT')
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

    def test_calculate(self):
        sr = self.create_nut_sr()
        sr.calculate_derived_products()
        return sr

    def test_netcdf_raw_files(self):
        disk_path = os.path.join(DATA_DIR, 'test_out')
        _, json_data = self._test_netcdf(disk_path)
        # Process results
        json_decoded = json.loads(json_data)
        self.assertIn('code', json_decoded)
        self.assertIn('message', json_decoded)
        self.assertIsInstance(json_decoded['code'], int)
        self.assertIsInstance(json_decoded['message'], unicode)
        self.assertEqual(json_decoded['code'], httplib.OK)
        file_paths = ast.literal_eval(json_decoded['message'])
        for file_path in file_paths:
            self.assertTrue(os.path.isfile(file_path))

    def test_netcdf_zip(self):
        disk_path = None
        stream_name, zip_data = self._test_netcdf(disk_path)
        # Process results
        file_path = os.path.join(DATA_DIR, 'test_out', '%s.zip' % stream_name)
        with open(file_path, 'w') as f:
            f.write(zip_data)

    def _test_netcdf(self, disk_path):
        sr = self.test_calculate()
        return sr.stream_key.stream.name, NetcdfGenerator(sr, False, disk_path).write()

    def test_csv(self):
        sr = self.test_calculate()
        csv = CsvGenerator(sr, ',').to_csv()
        self.assertTrue(csv)

    def test_qc(self):
        sr = self.test_calculate()
        expected_parameters = ['temp_sal_corrected_nitrate_qc_executed',
                               'temp_sal_corrected_nitrate_qc_results']
        self.assert_parameters_in_datasets(sr.datasets[self.nut_sk].datasets, expected_parameters)

    def test_metbk_hourly_needs(self):
        hourly_sk = StreamKey('CP01CNSM', 'SBD11', '06-METBKA000', 'telemetered', 'metbk_hourly')
        met_sk = StreamKey('CP01CNSM', 'SBD11', '06-METBKA000', 'telemetered', 'metbk_a_dcl_instrument')
        vel_sk = StreamKey('CP01CNSM', 'RID26', '04-VELPTA000', 'telemetered', 'velpt_ab_dcl_instrument')
        tr = TimeRange(0, 99999999)
        sr = StreamRequest(hourly_sk, [], {}, tr, {}, request_id='UNIT')
        self.assertEqual(set(sr.stream_parameters), {hourly_sk, met_sk, vel_sk})

    def create_metbk_hourly_sr(self):
        metbk_fn = 'metbk_a_dcl_instrument_recovered.nc'
        metbk_ds = xr.open_dataset(os.path.join(DATA_DIR, metbk_fn), decode_times=False)
        vel_fn = 'velpt_ab_dcl_instrument_recovered.nc'
        vel_ds = xr.open_dataset(os.path.join(DATA_DIR, vel_fn), decode_times=False)

        # both of these datasets are labeled deployment 3 but the times are squarely in deployment 1. Fix.
        metbk_ds.deployment.values[:] = 1
        vel_ds.deployment.values[:] = 1

        tr = TimeRange(metbk_ds.time.values[0], metbk_ds.time.values[-1])

        sr = StreamRequest(self.hourly_sk, [], tr, {}, request_id='UNIT')

        metbk_ds = metbk_ds[self.base_params + [p.name for p in sr.stream_parameters[self.met_sk]]]
        vel_ds = vel_ds[self.base_params + [p.name for p in sr.stream_parameters[self.vel_sk]]]

        sr.datasets[self.met_sk] = StreamDataset(self.met_sk, sr.uflags, [self.hourly_sk, self.vel_sk], sr.request_id)
        sr.datasets[self.hourly_sk] = StreamDataset(self.hourly_sk, sr.uflags, [self.met_sk, self.vel_sk], sr.request_id)
        sr.datasets[self.vel_sk] = StreamDataset(self.vel_sk, sr.uflags, [self.hourly_sk, self.met_sk], sr.request_id)

        sr.datasets[self.hourly_sk].events = self.met_events
        sr.datasets[self.met_sk].events = self.met_events
        sr.datasets[self.vel_sk].events = self.vel_events

        sr.datasets[self.met_sk]._insert_dataset(metbk_ds)
        sr.datasets[self.vel_sk]._insert_dataset(vel_ds)
        return sr

    def test_metbk_hourly(self):
        hourly_sk = StreamKey('GI01SUMO', 'SBD11', '06-METBKA000', 'recovered_host', 'metbk_hourly')
        sr = self.create_metbk_hourly_sr()
        sr.calculate_derived_products()
        expected_params = [p.name for p in hourly_sk.stream.parameters] + ['obs', 'time', 'deployment', 'lat', 'lon']
        self.assertListEqual(sorted(expected_params), sorted(sr.datasets[hourly_sk].datasets[1]))

    def create_echo_sounding_sr(self, parameters=None):
        parameters = [] if parameters is None else parameters
        echo_fn = 'echo_sounding.nc'
        echo_ds = xr.open_dataset(os.path.join(DATA_DIR, echo_fn), decode_times=False)
        # somehow the times in this dataset are corrupted. Remap to valid times spanning both deployments
        dep1_start = self.hpies_events.deps[1].ntp_start
        dep2_end = self.hpies_events.deps[2].ntp_start + 864000
        echo_ds.time.values = np.linspace(dep1_start + 1, dep2_end - 1, num=echo_ds.time.shape[0])

        tr = TimeRange(dep1_start, dep2_end)
        sr = StreamRequest(self.echo_sk, parameters, tr, {}, request_id='UNIT')
        sr.datasets[self.echo_sk] = StreamDataset(self.echo_sk, sr.uflags, [], sr.request_id)
        sr.datasets[self.echo_sk].events = self.hpies_events
        sr.datasets[self.echo_sk]._insert_dataset(echo_ds)
        return sr

    @unittest.skip('missing asset data')
    def test_function_map_scalar(self):
        echo_sk = StreamKey('RS01SLBS', 'LJ01A', '05-HPIESA101', 'streamed', 'echo_sounding')
        sr = self.create_echo_sounding_sr()
        sr.calculate_derived_products()
        sr._add_location()

        expected = {'hpies_travel_time1_L1', 'hpies_travel_time2_L1', 'hpies_travel_time3_L1', 'hpies_travel_time4_L1',
                    'hpies_bliley_temperature_L1', 'hpies_pressure_L1'}
        missing = expected.difference(sr.datasets[echo_sk].datasets[1])
        self.assertSetEqual(missing, set())
        missing = expected.difference(sr.datasets[echo_sk].datasets[2])
        self.assertSetEqual(missing, set())

    @unittest.skip('missing asset data')
    def test_add_location(self):
        echo_sk = StreamKey('RS01SLBS', 'LJ01A', '05-HPIESA101', 'streamed', 'echo_sounding')
        sr = self.create_echo_sounding_sr()
        sr.calculate_derived_products()
        sr._add_location()
        ds = sr.datasets[echo_sk]

        for deployment, lat, lon in [(1, 44.52225, -125.38051), (2, 44.52230, -125.38041)]:
            lats = np.unique(ds.datasets[deployment].lat.values)
            lons = np.unique(ds.datasets[deployment].lon.values)
            np.testing.assert_almost_equal(lat, lats, decimal=5)
            np.testing.assert_almost_equal(lon, lons, decimal=5)

    def test_add_externals(self):
        nutnr_sk = StreamKey('CE04OSPS', 'SF01B', '4A-NUTNRA102', 'streamed', 'nutnr_a_sample')
        ctdpf_sk = StreamKey('CE04OSPS', 'SF01B', '2A-CTDPFA107', 'streamed', 'ctdpf_sbe43_sample')
        sr = self.create_nut_sr()
        sr.calculate_derived_products()
        sr.import_extra_externals()

        self.assertIn('ctdpf_sbe43_sample-seawater_pressure', sr.datasets[nutnr_sk].datasets[2])
        self.assertNotIn('ctdpf_sbe43_sample-seawater_pressure', sr.datasets[ctdpf_sk].datasets[2])

        data = json.loads(JsonResponse(sr).json())
        for each in data:
            self.assertIn('int_ctd_pressure', each)

    def get_events(self, stream_key):
        return AssetEvents(stream_key.as_three_part_refdes(), {})

    def test_add_externals_glider(self):
        gps_fn = 'deployment0003_CE05MOAS-GL319-00-ENG000000-recovered_host-glider_gps_position.nc'
        par_fn = 'deployment0003_CE05MOAS-GL319-01-PARADM000-recovered_host-parad_m_glider_recovered.nc'
        ctd_fn = 'deployment0003_CE05MOAS-GL319-05-CTDGVM000-recovered_host-ctdgv_m_glider_instrument_recovered.nc'

        gps_sk = StreamKey('CE05MOAS', 'GL319', '00-ENG000000', 'recovered_host', 'glider_gps_position')
        par_sk = StreamKey('CE05MOAS', 'GL319', '01-PARADM000', 'recovered_host', 'parad_m_glider_recovered')
        ctd_sk = StreamKey('CE05MOAS', 'GL319', '05-CTDGVM000', 'recovered_host', 'ctdgv_m_glider_instrument_recovered')

        # Fetch the source data
        gps_ds = xr.open_dataset(os.path.join(DATA_DIR, gps_fn), decode_times=False)
        par_ds = xr.open_dataset(os.path.join(DATA_DIR, par_fn), decode_times=False)
        ctd_ds = xr.open_dataset(os.path.join(DATA_DIR, ctd_fn), decode_times=False)

        # Create the stream request
        tr = TimeRange(par_ds.time.values[0], par_ds.time.values[-1])
        sr = StreamRequest(par_sk, [], tr, {}, request_id='UNIT')

        # Filter the source data to just the data the stream request says we need
        gps_ds = gps_ds[self.base_params + [p.name for p in sr.stream_parameters[gps_sk]]]
        par_ds = par_ds[self.base_params + [p.name for p in sr.stream_parameters[par_sk]]]
        ctd_ds = ctd_ds[self.base_params + [p.name for p in sr.stream_parameters[ctd_sk]]]

        # Create the StreamDataset objects
        sr.datasets[gps_sk] = StreamDataset(gps_sk, sr.uflags, [par_sk, ctd_sk], sr.request_id)
        sr.datasets[par_sk] = StreamDataset(par_sk, sr.uflags, [gps_sk, ctd_sk], sr.request_id)
        sr.datasets[ctd_sk] = StreamDataset(ctd_sk, sr.uflags, [par_sk, gps_sk], sr.request_id)

        sr.datasets[gps_sk].events = self.get_events(gps_sk)
        sr.datasets[par_sk].events = self.get_events(par_sk)
        sr.datasets[ctd_sk].events = self.get_events(ctd_sk)

        # Insert the source data
        sr.datasets[gps_sk]._insert_dataset(gps_ds)
        sr.datasets[par_sk]._insert_dataset(par_ds)
        sr.datasets[ctd_sk]._insert_dataset(ctd_ds)

        sr.calculate_derived_products()
        sr.import_extra_externals()

        self.assertIn('ctdgv_m_glider_instrument_recovered-sci_water_pressure_dbar', sr.datasets[par_sk].datasets[3])
        self.assertNotIn('ctdgv_m_glider_instrument_recovered-sci_water_pressure_dbar', sr.datasets[ctd_sk].datasets[3])

        data = json.loads(JsonResponse(sr).json())
        for each in data:
            self.assertIn('int_ctd_pressure', each)
            self.assertIn('lat', each)
            self.assertIn('lon', each)

    def test_glider_rename_netcdf_lat_lon(self):
        ctd_sk = StreamKey('CE05MOAS', 'GL319', '05-CTDGVM000', 'recovered_host', 'ctdgv_m_glider_instrument_recovered')
        ctd_fn = 'deployment0003_CE05MOAS-GL319-05-CTDGVM000-recovered_host-ctdgv_m_glider_instrument_recovered.nc'
        ctd_ds = xr.open_dataset(os.path.join(DATA_DIR, ctd_fn), decode_times=False)

        self.assertIn('glider_gps_position-m_gps_lat', ctd_ds)
        self.assertIn('glider_gps_position-m_gps_lon', ctd_ds)

        modified = NetcdfGenerator._rename_glider_lat_lon(ctd_sk, ctd_ds)
        self.assertNotIn('glider_gps_position-m_gps_lat', modified)
        self.assertNotIn('glider_gps_position-m_gps_lon', modified)
        self.assertIn('lat', modified)
        self.assertIn('lon', modified)

    def test_execute_stream_request_multiple_streams(self):
        input_data = json.load(open(os.path.join(DATA_DIR, 'multiple_stream_request.json')))

        ctdpf_fn = 'ctdpf_sbe43_sample.nc'
        echo_fn = 'echo_sounding.nc'
        ctd_ds = xr.open_dataset(os.path.join(DATA_DIR, ctdpf_fn), decode_times=False)
        echo_ds = xr.open_dataset(os.path.join(DATA_DIR, echo_fn), decode_times=False)

        ctd_ds = ctd_ds.isel(obs=np.arange(0, 100))
        echo_ds = echo_ds.isel(obs=np.arange(0, 100))

        def mock_fetch_raw_data(self):
            if StreamRequestTest.call_cnt % 2 == 0:
                self.datasets[self.stream_key] = StreamDataset(self.stream_key, self.uflags, [], self.request_id)
                self.datasets[self.stream_key]._insert_dataset(ctd_ds)
                self.datasets[self.stream_key].events = AssetEvents('test', [])

            else:
                self.datasets[self.stream_key] = StreamDataset(self.stream_key, self.uflags, [], self.request_id)
                self.datasets[self.stream_key]._insert_dataset(echo_ds)
                self.datasets[self.stream_key].events = AssetEvents('test', [])

            StreamRequestTest.call_cnt += 1

        def mock_collapse_times(self):
            pass

        with mock.patch('util.stream_request.StreamRequest.fetch_raw_data', new=mock_fetch_raw_data):
            with mock.patch('util.stream_request.StreamRequest._collapse_times', new=mock_collapse_times):
                sr = execute_stream_request(validate(input_data), True)
                self.assertEqual(len(sr.external_includes), 1)

                sr = execute_stream_request(validate(input_data))
                self.assertIn(self.echo_sk, sr.external_includes)
                expected = {Parameter.query.get(2575)}
                self.assertEqual(expected, sr.external_includes[self.echo_sk])

                self.assertIn(self.nut_sk, sr.external_includes)
                expected = Parameter.query.get(2327)
                self.assertIn(expected, sr.external_includes[self.nut_sk])
                expected = Parameter.query.get(2328)
                self.assertIn(expected, sr.external_includes[self.nut_sk])
                expected = Parameter.query.get(2329)
                self.assertIn(expected, sr.external_includes[self.nut_sk])

    def test_execute_stream_request_multiple_streams_invalid_input(self):
        input_data = json.load(open(os.path.join(DATA_DIR, 'multiple_stream_request_no_parameter.json')))

        with self.assertRaises(InvalidParameterException):
            validate(input_data)

        input_data = json.load(open(os.path.join(DATA_DIR, 'multiple_stream_request_bad_parameter.json')))

        with self.assertRaises(InvalidParameterException):
            validate(input_data)

    def test_two_sr(self):
        nut_sr = self.create_nut_sr()
        nut_sr.calculate_derived_products()
        nut_sr.import_extra_externals()

        echo_sr = self.create_echo_sounding_sr(parameters=[3786])
        echo_sr.calculate_derived_products()
        echo_sr.import_extra_externals()

        nut_sr.interpolate_from_stream_request(echo_sr)

        self.assertIn(self.echo_sk, nut_sr.external_includes)
        expected = {Parameter.query.get(3786)}
        self.assertEqual(expected, nut_sr.external_includes[self.echo_sk])

        expected_name = 'echo_sounding-hpies_temperature'

        for dataset in nut_sr.datasets[self.nut_sk].datasets.itervalues():
            self.assertIn(expected_name, dataset)

        data = json.loads(JsonResponse(nut_sr).json())
        for each in data:
            self.assertIn(expected_name, each)
