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
from ion_functions.data.ctd_functions import ctd_sbe16plus_tempwat, ctd_pracsal

from preload_database.database import initialize_connection, open_connection, PreloadDatabaseMode
from util.common import StreamKey, TimeRange, StreamEngineException
from util.csvresponse import CsvGenerator
from util.jsonresponse import JsonResponse
from util.netcdf_generator import NetcdfGenerator
from util.stream_dataset import StreamDataset
from util.stream_request import StreamRequest

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


@mock.patch('util.stream_request.get_available_time_range', new=get_available_time_range)
@mock.patch('util.cass._get_stream_metadata', new=get_stream_metadata)
class StreamRequestTest(unittest.TestCase):
    metadata = []
    base_params = ['time', 'deployment', 'provenance']

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
        sk = StreamKey('CE04OSPS', 'SF01B', '4A-NUTNRA102', 'streamed', 'nutnr_a_sample')
        sk2 = StreamKey('CE04OSPS', 'SF01B', '2A-CTDPFA107', 'streamed', 'ctdpf_sbe43_sample')
        tr = TimeRange(3617736678.149051, 3661524609.0570827)
        sr = StreamRequest(sk, [], {}, tr, {}, request_id='UNIT')

        self.assertEqual(set(sr.stream_parameters), {sk, sk2})

    def test_need_dpi(self):
        # OPTAA specifies that it needs dpi_PRACSAL_L2
        # first, an OPTAA with a colocated SBE43
        sk = StreamKey('RS03AXPS', 'SF03A', '3B-OPTAAD301', 'streamed', 'optaa_sample')
        sk2 = StreamKey('RS03AXPS', 'SF03A', '2A-CTDPFA302', 'streamed', 'ctdpf_sbe43_sample')
        tr = TimeRange(3617736678.149051, 3661524609.0570827)
        sr = StreamRequest(sk, [], {}, tr, {}, request_id='UNIT')

        self.assertEqual(set(sr.stream_parameters), {sk, sk2})

        # now, an OPTAA with a colocated CTDPF/optode
        sk = StreamKey('RS03AXBS', 'LJ03A', '11-OPTAAC303', 'streamed', 'optaa_sample')
        sk2 = StreamKey('RS03AXBS', 'LJ03A', '12-CTDPFB301', 'streamed', 'ctdpf_optode_sample')
        tr = TimeRange(3617736678.149051, 3661524609.0570827)
        sr = StreamRequest(sk, [], {}, tr, {}, request_id='UNIT')

        self.assertEqual(set(sr.stream_parameters), {sk, sk2})

    def test_virtual(self):
        sk1 = StreamKey('GI01SUMO', 'SBD11', '06-METBKA000', 'recovered_host', 'metbk_hourly')
        sk2 = StreamKey('GI01SUMO', 'SBD11', '06-METBKA000', 'recovered_host', 'metbk_a_dcl_instrument_recovered')
        sk3 = StreamKey('GI01SUMO', 'RID16', '04-VELPTA000', 'recovered_host', 'velpt_ab_dcl_instrument_recovered')
        tr = TimeRange(3617736678.149051, 3661524609.0570827)
        sr = StreamRequest(sk1, [], {}, tr, {}, request_id='UNIT')
        self.assertEqual(set(sr.stream_parameters), {sk1, sk2, sk3})

    def test_calculate(self):
        nutnr_sk = StreamKey('CE04OSPS', 'SF01B', '4A-NUTNRA102', 'streamed', 'nutnr_a_sample')
        ctdpf_sk = StreamKey('CE04OSPS', 'SF01B', '2A-CTDPFA107', 'streamed', 'ctdpf_sbe43_sample')
        nutnr_fn = 'nutnr_a_sample.nc'
        ctdpf_fn = 'ctdpf_sbe43_sample.nc'

        cals = json.load(open(os.path.join(DATA_DIR, 'cals.json')))

        tr = TimeRange(3.65342400e+09, 3.65351040e+09)
        coefficients = {k: [{'start': tr.start-1, 'stop': tr.stop+1, 'value': cals[k], 'deployment': 1}] for k in cals}
        sr = StreamRequest(nutnr_sk, [2443], coefficients, tr, {}, request_id='UNIT')
        nutnr_ds = xr.open_dataset(os.path.join(DATA_DIR, nutnr_fn), decode_times=False)
        ctdpf_ds = xr.open_dataset(os.path.join(DATA_DIR, ctdpf_fn), decode_times=False)

        nutnr_ds = nutnr_ds[self.base_params + [p.name for p in sr.stream_parameters[nutnr_sk]]]
        ctdpf_ds = ctdpf_ds[self.base_params + [p.name for p in sr.stream_parameters[ctdpf_sk]]]

        sr.datasets[ctdpf_sk] = StreamDataset(ctdpf_sk, sr.coefficients, sr.uflags, [nutnr_sk], sr.request_id)
        sr.datasets[nutnr_sk] = StreamDataset(nutnr_sk, sr.coefficients, sr.uflags, [ctdpf_sk], sr.request_id)
        sr.datasets[ctdpf_sk]._insert_dataset(ctdpf_ds)
        sr.datasets[nutnr_sk]._insert_dataset(nutnr_ds)

        sr.calculate_derived_products()

        ds = sr.datasets[ctdpf_sk]
        tempwat = ctd_sbe16plus_tempwat(ds.datasets[0].temperature,
                                        cals['CC_a0'], cals['CC_a1'],
                                        cals['CC_a2'], cals['CC_a3'])
        np.testing.assert_array_equal(ds.datasets[0].seawater_temperature, tempwat)

        pracsal = ctd_pracsal(ds.datasets[0].seawater_conductivity,
                              ds.datasets[0].seawater_temperature,
                              ds.datasets[0].seawater_pressure)
        np.testing.assert_array_equal(ds.datasets[0].practical_salinity, pracsal)

        response = json.loads(JsonResponse(sr).json())
        self.assertEqual(len(response), len(nutnr_ds.time.values))

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
        nutnr_sk = StreamKey('CE04OSPS', 'SF01B', '4A-NUTNRA102', 'streamed', 'nutnr_a_sample')
        ctdpf_sk = StreamKey('CE04OSPS', 'SF01B', '2A-CTDPFA107', 'streamed', 'ctdpf_sbe43_sample')
        nutnr_fn = 'nutnr_a_sample.nc'
        ctdpf_fn = 'ctdpf_sbe43_sample.nc'

        cals = json.load(open(os.path.join(DATA_DIR, 'cals.json')))

        tr = TimeRange(3.65342400e+09, 3.65351040e+09)
        coefficients = {k: [{'start': tr.start-1, 'stop': tr.stop+1, 'value': cals[k], 'deployment': 1}] for k in cals}
        sr = StreamRequest(nutnr_sk, [2443], coefficients, tr, {}, request_id='UNIT')
        nutnr_ds = xr.open_dataset(os.path.join(DATA_DIR, nutnr_fn), decode_times=False)
        ctdpf_ds = xr.open_dataset(os.path.join(DATA_DIR, ctdpf_fn), decode_times=False)

        nutnr_ds = nutnr_ds[self.base_params + [p.name for p in sr.stream_parameters[nutnr_sk]]]
        ctdpf_ds = ctdpf_ds[self.base_params + [p.name for p in sr.stream_parameters[ctdpf_sk]]]

        sr.datasets[ctdpf_sk] = StreamDataset(ctdpf_sk, sr.coefficients, sr.uflags, [nutnr_sk], sr.request_id)
        sr.datasets[nutnr_sk] = StreamDataset(nutnr_sk, sr.coefficients, sr.uflags, [ctdpf_sk], sr.request_id)
        sr.datasets[ctdpf_sk]._insert_dataset(ctdpf_ds)
        sr.datasets[nutnr_sk]._insert_dataset(nutnr_ds)

        sr.calculate_derived_products()
        return sr.stream_key.stream.name, NetcdfGenerator(sr, False, disk_path).write()

    def test_csv(self):
        nutnr_sk = StreamKey('CE04OSPS', 'SF01B', '4A-NUTNRA102', 'streamed', 'nutnr_a_sample')
        ctdpf_sk = StreamKey('CE04OSPS', 'SF01B', '2A-CTDPFA107', 'streamed', 'ctdpf_sbe43_sample')
        nutnr_fn = 'nutnr_a_sample.nc'
        ctdpf_fn = 'ctdpf_sbe43_sample.nc'

        cals = json.load(open(os.path.join(DATA_DIR, 'cals.json')))

        tr = TimeRange(3.65342400e+09, 3.65351040e+09)
        coefficients = {k: [{'start': tr.start-1, 'stop': tr.stop+1, 'value': cals[k], 'deployment': 1}] for k in cals}
        sr = StreamRequest(nutnr_sk, [2443], coefficients, tr, {}, request_id='UNIT')
        nutnr_ds = xr.open_dataset(os.path.join(DATA_DIR, nutnr_fn), decode_times=False)
        ctdpf_ds = xr.open_dataset(os.path.join(DATA_DIR, ctdpf_fn), decode_times=False)

        nutnr_ds = nutnr_ds[self.base_params + [p.name for p in sr.stream_parameters[nutnr_sk]]]
        ctdpf_ds = ctdpf_ds[self.base_params + [p.name for p in sr.stream_parameters[ctdpf_sk]]]

        sr.datasets[ctdpf_sk] = StreamDataset(ctdpf_sk, sr.coefficients, sr.uflags, [nutnr_sk], sr.request_id)
        sr.datasets[nutnr_sk] = StreamDataset(nutnr_sk, sr.coefficients, sr.uflags, [ctdpf_sk], sr.request_id)
        sr.datasets[ctdpf_sk]._insert_dataset(ctdpf_ds)
        sr.datasets[nutnr_sk]._insert_dataset(nutnr_ds)

        sr.calculate_derived_products()
        csv = CsvGenerator(sr, ',').to_csv()
        self.assertTrue(csv)

    def test_qc(self):
        nutnr_sk = StreamKey('CE04OSPS', 'SF01B', '4A-NUTNRA102', 'streamed', 'nutnr_a_sample')
        ctdpf_sk = StreamKey('CE04OSPS', 'SF01B', '2A-CTDPFA107', 'streamed', 'ctdpf_sbe43_sample')
        nutnr_fn = 'nutnr_a_sample.nc'
        ctdpf_fn = 'ctdpf_sbe43_sample.nc'

        cals = json.load(open(os.path.join(DATA_DIR, 'cals.json')))
        qc = json.load(open(os.path.join(DATA_DIR, 'qc.json')))

        tr = TimeRange(3.65342400e+09, 3.65351040e+09)
        coefficients = {k: [{'start': tr.start-1, 'stop': tr.stop+1, 'value': cals[k], 'deployment': 1}] for k in cals}
        sr = StreamRequest(nutnr_sk, [2443], coefficients, tr, {}, qc_parameters=qc, request_id='UNIT')

        nutnr_ds = xr.open_dataset(os.path.join(DATA_DIR, nutnr_fn), decode_times=False)
        ctdpf_ds = xr.open_dataset(os.path.join(DATA_DIR, ctdpf_fn), decode_times=False)
        ctdpf_ds = ctdpf_ds[self.base_params + [p.name for p in sr.stream_parameters[ctdpf_sk]]]
        nutnr_ds = nutnr_ds[self.base_params + [p.name for p in sr.stream_parameters[nutnr_sk]]]

        sr.datasets[ctdpf_sk] = StreamDataset(ctdpf_sk, sr.coefficients, sr.uflags, [nutnr_sk], sr.request_id)
        sr.datasets[nutnr_sk] = StreamDataset(nutnr_sk, sr.coefficients, sr.uflags, [ctdpf_sk], sr.request_id)
        sr.datasets[ctdpf_sk]._insert_dataset(ctdpf_ds)
        sr.datasets[nutnr_sk]._insert_dataset(nutnr_ds)

        sr.calculate_derived_products()

        expected_parameters = ['temp_sal_corrected_nitrate_qc_executed',
                               'temp_sal_corrected_nitrate_qc_results']
        self.assert_parameters_in_datasets(sr.datasets[nutnr_sk].datasets, expected_parameters)

    def test_metbk_hourly_needs(self):
        hourly_sk = StreamKey('CP01CNSM', 'SBD11', '06-METBKA000', 'telemetered', 'metbk_hourly')
        met_sk = StreamKey('CP01CNSM', 'SBD11', '06-METBKA000', 'telemetered', 'metbk_a_dcl_instrument')
        vel_sk = StreamKey('CP01CNSM', 'RID26', '04-VELPTA000', 'telemetered', 'velpt_ab_dcl_instrument')
        tr = TimeRange(0, 99999999)
        sr = StreamRequest(hourly_sk, [], {}, tr, {}, request_id='UNIT')
        self.assertEqual(set(sr.stream_parameters), {hourly_sk, met_sk, vel_sk})

    def test_metbk_hourly(self):
        cals = {
            'CC_lat': 40.13678333,
            'CC_lon': -70.76978333,
            'CC_depth_of_conductivity_and_temperature_measurements_m': 1.0668,
            'CC_height_of_air_humidity_measurement_m': 4.2926,
            'CC_height_of_air_temperature_measurement_m': 4.2926,
            'CC_height_of_windspeed_sensor_above_sealevel_m': 4.7498,
            'CC_jcool': 1,
            'CC_jwarm': 1,
            'CC_zinvpbl': 600,
        }

        metbk_fn = 'metbk_a_dcl_instrument_recovered.nc'
        metbk_ds = xr.open_dataset(os.path.join(DATA_DIR, metbk_fn), decode_times=False)
        vel_fn = 'velpt_ab_dcl_instrument_recovered.nc'
        vel_ds = xr.open_dataset(os.path.join(DATA_DIR, vel_fn), decode_times=False)

        hourly_sk = StreamKey('GI01SUMO', 'SBD11', '06-METBKA000', 'recovered_host', 'metbk_hourly')
        source_sk = StreamKey('GI01SUMO', 'SBD11', '06-METBKA000', 'recovered_host', 'metbk_a_dcl_instrument_recovered')
        vel_sk = StreamKey('GI01SUMO', 'RID16', '04-VELPTA000', 'recovered_host', 'velpt_ab_dcl_instrument_recovered')

        tr = TimeRange(metbk_ds.time.values[0], metbk_ds.time.values[-1])
        coefficients = {k: [{'start': tr.start-1000, 'stop': tr.stop+1000, 'value': cals[k], 'deployment': 3}] for k in cals}
        sr = StreamRequest(hourly_sk, [], coefficients, tr, {}, request_id='UNIT')

        metbk_ds = metbk_ds[self.base_params + [p.name for p in sr.stream_parameters[source_sk]]]
        vel_ds = vel_ds[self.base_params + [p.name for p in sr.stream_parameters[vel_sk]]]

        sr.datasets[source_sk] = StreamDataset(source_sk, sr.coefficients, sr.uflags, [hourly_sk, vel_sk], sr.request_id)
        sr.datasets[hourly_sk] = StreamDataset(hourly_sk, sr.coefficients, sr.uflags, [source_sk, vel_sk], sr.request_id)
        sr.datasets[vel_sk] = StreamDataset(vel_sk, sr.coefficients, sr.uflags, [hourly_sk, vel_sk], sr.request_id)

        sr.datasets[source_sk]._insert_dataset(metbk_ds)
        sr.datasets[vel_sk]._insert_dataset(vel_ds)

        sr.calculate_derived_products()

        expected_params = [p.name for p in hourly_sk.stream.parameters] + ['obs', 'time', 'deployment', 'lat', 'lon']
        self.assertListEqual(sorted(expected_params), sorted(sr.datasets[hourly_sk].datasets[3]))

    def test_function_map_scalar(self):
        echo_fn = 'echo_sounding.nc'
        echo_ds = xr.open_dataset(os.path.join(DATA_DIR, echo_fn), decode_times=False)
        echo_sk = StreamKey('RS01SLBS', 'LJ01A', '05-HPIESA101', 'streamed', 'echo_sounding')
        tr = TimeRange(0, 99999999)
        sr = StreamRequest(echo_sk, [], {}, tr, {}, request_id='UNIT')
        sr.datasets[echo_sk] = StreamDataset(echo_sk, sr.coefficients, sr.uflags, [], sr.request_id)
        sr.datasets[echo_sk]._insert_dataset(echo_ds)
        sr.calculate_derived_products()
        sr._add_location()

        expected = {'hpies_travel_time1_L1', 'hpies_travel_time2_L1', 'hpies_travel_time3_L1', 'hpies_travel_time4_L1',
                    'hpies_bliley_temperature_L1', 'hpies_pressure_L1'}
        missing = expected.difference(sr.datasets[echo_sk].datasets[0])
        self.assertSetEqual(missing, set())

    def test_add_location(self):
        echo_fn = 'echo_sounding.nc'
        echo_ds = xr.open_dataset(os.path.join(DATA_DIR, echo_fn), decode_times=False)
        echo_ds.deployment.values[:20] = 1
        echo_ds.deployment.values[20:] = 2
        echo_sk = StreamKey('RS01SLBS', 'LJ01A', '05-HPIESA101', 'streamed', 'echo_sounding')
        location_info = {echo_sk.as_three_part_refdes(): [{'deployment': 1, 'lat': 1, 'lon': 5},
                                                          {'deployment': 2, 'lat': 2, 'lon': 6}]}
        tr = TimeRange(0, 99999999)
        sr = StreamRequest(echo_sk, [], {}, tr, {}, location_information=location_info, request_id='UNIT')
        sr.datasets[echo_sk] = StreamDataset(echo_sk, sr.coefficients, sr.uflags, [], sr.request_id)
        sr.datasets[echo_sk]._insert_dataset(echo_ds)

        sr.calculate_derived_products()
        sr._add_location()

        ds = sr.datasets[echo_sk]

        for deployment, lat, lon in [(1, 1.0, 5.0), (2, 2.0, 6.0)]:
            lats = set(np.unique(ds.datasets[deployment].lat.values))
            lons = set(np.unique(ds.datasets[deployment].lon.values))
            self.assertSetEqual(lats, {lat})
            self.assertSetEqual(lons, {lon})

    def test_add_externals(self):
        nutnr_sk = StreamKey('CE04OSPS', 'SF01B', '4A-NUTNRA102', 'streamed', 'nutnr_a_sample')
        ctdpf_sk = StreamKey('CE04OSPS', 'SF01B', '2A-CTDPFA107', 'streamed', 'ctdpf_sbe43_sample')
        nutnr_fn = 'nutnr_a_sample.nc'
        ctdpf_fn = 'ctdpf_sbe43_sample.nc'

        cals = json.load(open(os.path.join(DATA_DIR, 'cals.json')))

        tr = TimeRange(3.65342400e+09, 3.65351040e+09)
        coefficients = {k: [{'start': tr.start-1, 'stop': tr.stop+1, 'value': cals[k], 'deployment': 1}] for k in cals}
        sr = StreamRequest(nutnr_sk, [2443], coefficients, tr, {}, request_id='UNIT')

        nutnr_ds = xr.open_dataset(os.path.join(DATA_DIR, nutnr_fn), decode_times=False)
        ctdpf_ds = xr.open_dataset(os.path.join(DATA_DIR, ctdpf_fn), decode_times=False)
        ctdpf_ds = ctdpf_ds[self.base_params + [p.name for p in sr.stream_parameters[ctdpf_sk]]]
        nutnr_ds = nutnr_ds[self.base_params + [p.name for p in sr.stream_parameters[nutnr_sk]]]

        sr.datasets[ctdpf_sk] = StreamDataset(ctdpf_sk, sr.coefficients, sr.uflags, [nutnr_sk], sr.request_id)
        sr.datasets[nutnr_sk] = StreamDataset(nutnr_sk, sr.coefficients, sr.uflags, [ctdpf_sk], sr.request_id)
        sr.datasets[ctdpf_sk]._insert_dataset(ctdpf_ds)
        sr.datasets[nutnr_sk]._insert_dataset(nutnr_ds)

        sr.calculate_derived_products()
        sr.import_extra_externals()

        self.assertIn('ctdpf_sbe43_sample-seawater_pressure', sr.datasets[nutnr_sk].datasets[0])
        self.assertNotIn('ctdpf_sbe43_sample-seawater_pressure', sr.datasets[ctdpf_sk].datasets[0])

        data = json.loads(JsonResponse(sr).json())
        for each in data:
            self.assertIn('int_ctd_pressure', each)

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
        sr = StreamRequest(par_sk, [], {}, tr, {}, request_id='UNIT')

        # Filter the source data to just the data the stream request says we need
        gps_ds = gps_ds[self.base_params + [p.name for p in sr.stream_parameters[gps_sk]]]
        par_ds = par_ds[self.base_params + [p.name for p in sr.stream_parameters[par_sk]]]
        ctd_ds = ctd_ds[self.base_params + [p.name for p in sr.stream_parameters[ctd_sk]]]

        # Create the StreamDataset objects
        sr.datasets[gps_sk] = StreamDataset(gps_sk, sr.coefficients, sr.uflags, [par_sk, ctd_sk], sr.request_id)
        sr.datasets[par_sk] = StreamDataset(par_sk, sr.coefficients, sr.uflags, [gps_sk, ctd_sk], sr.request_id)
        sr.datasets[ctd_sk] = StreamDataset(ctd_sk, sr.coefficients, sr.uflags, [par_sk, gps_sk], sr.request_id)

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
