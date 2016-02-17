import csv
import json
import os
import unittest
import logging
import mock
import pandas as pd
import xray
import numpy as np
from ion_functions.data.ctd_functions import ctd_sbe16plus_tempwat, ctd_pracsal

from preload_database.database import initialize_connection, open_connection, PreloadDatabaseMode
from util.common import StreamKey, TimeRange, StreamEngineException
from util.jsonresponse import JsonResponse
from util.stream_request import StreamRequest

TEST_DIR = os.path.dirname(__file__)
initialize_connection(PreloadDatabaseMode.POPULATED_MEMORY)
open_connection()

logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)
metadata = pd.read_csv(os.path.join(TEST_DIR, 'data', 'stream_metadata.csv'))


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

    def test_basic_stream_request(self):
        sk = StreamKey('CP05MOAS', 'GL388', '03-CTDGVM000', 'recovered_host', 'ctdgv_m_glider_instrument_recovered')
        tr = TimeRange(3.622409e+09, 3.627058e+09)
        sr = StreamRequest(sk, [1527], {}, tr, {})

    def test_glider_include_preswat_gps(self):
        do_sk = StreamKey('CP05MOAS', 'GL388', '04-DOSTAM000', 'recovered_host', 'dosta_abcdjm_glider_recovered')
        ctd_sk = StreamKey('CP05MOAS', 'GL388', '03-CTDGVM000', 'recovered_host', 'ctdgv_m_glider_instrument_recovered')
        gps_sk = StreamKey('CP05MOAS', 'GL388', '00-ENG000000', 'recovered_host', 'glider_gps_position')
        tr = TimeRange(3.622409e+09, 3.627058e+09)
        sr = StreamRequest(do_sk, [], {}, tr, {})

        # we expect to fetch the PRESWAT from the ctd glider stream and LAT/LON from the gps position stream
        self.assertEqual(set(sr.stream_parameters), {do_sk, ctd_sk, gps_sk})

    def test_wfp_include_preswat(self):
        par_sk = StreamKey('CP02PMUO', 'WFP01', '05-PARADK000', 'recovered_wfp',
                           'parad_k__stc_imodem_instrument_recovered')
        ctd_sk = StreamKey('CP02PMUO', 'WFP01', '03-CTDPFK000', 'recovered_wfp', 'ctdpf_ckl_wfp_instrument_recovered')
        tr = TimeRange(3594211324.0, 3653837045.0)
        sr = StreamRequest(par_sk, [], {}, tr, {})

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
        sr = StreamRequest(sk, [911], {}, tr, {})
        # if internal only, no external stream should exist in stream_parameters
        self.assertEqual(set(sr.stream_parameters), {sk})

    def test_need_external(self):
        # nutnr_a_sample requests PD908 and PD911
        sk = StreamKey('CE04OSPS', 'SF01B', '4A-NUTNRA102', 'streamed', 'nutnr_a_sample')
        sk2 = StreamKey('CE04OSPS', 'SF01B', '2A-CTDPFA107', 'streamed', 'ctdpf_sbe43_sample')
        tr = TimeRange(3617736678.149051, 3661524609.0570827)
        sr = StreamRequest(sk, [], {}, tr, {})

        self.assertEqual(set(sr.stream_parameters), {sk, sk2})

    def test_need_dpi(self):
        # OPTAA specifies that it needs dpi_PRACSAL_L2
        # first, an OPTAA with a colocated SBE43
        sk = StreamKey('RS03AXPS', 'SF03A', '3B-OPTAAD301', 'streamed', 'optaa_sample')
        sk2 = StreamKey('RS03AXPS', 'SF03A', '2A-CTDPFA302', 'streamed', 'ctdpf_sbe43_sample')
        tr = TimeRange(3617736678.149051, 3661524609.0570827)
        sr = StreamRequest(sk, [], {}, tr, {})

        self.assertEqual(set(sr.stream_parameters), {sk, sk2})

        # now, an OPTAA with a colocated CTDPF/optode
        sk = StreamKey('RS03AXBS', 'LJ03A', '11-OPTAAC303', 'streamed', 'optaa_sample')
        sk2 = StreamKey('RS03AXBS', 'LJ03A', '12-CTDPFB301', 'streamed', 'ctdpf_optode_sample')
        tr = TimeRange(3617736678.149051, 3661524609.0570827)
        sr = StreamRequest(sk, [], {}, tr, {})

        self.assertEqual(set(sr.stream_parameters), {sk, sk2})

    def test_virtual(self):
        sk = StreamKey('CP01CNSM', 'SBD11', '06-METBKA000', 'recovered_host', 'metbk_hourly')
        sk1 = StreamKey('CP01CNSM', 'SBD11', '06-METBKA000', 'recovered_host', 'metbk_a_dcl_instrument_recovered')
        tr = TimeRange(3617736678.149051, 3661524609.0570827)
        sr = StreamRequest(sk, [], {}, tr, {})
        self.assertEqual(set(sr.stream_parameters), {sk, sk1})

    def test_calculate(self):
        nutnr_sk = StreamKey('CE04OSPS', 'SF01B', '4A-NUTNRA102', 'streamed', 'nutnr_a_sample')
        ctdpf_sk = StreamKey('CE04OSPS', 'SF01B', '2A-CTDPFA107', 'streamed', 'ctdpf_sbe43_sample')
        nutnr_fn = 'nutnr_a_sample.nc'
        ctdpf_fn = 'ctdpf_sbe43_sample.nc'

        cals = json.load(open(os.path.join(TEST_DIR, 'data', 'cals.json')))

        tr = TimeRange(3.65342400e+09, 3.65351040e+09)
        coefficients = {k: [{'start': tr.start-1, 'stop': tr.stop+1, 'value': cals[k], 'deployment': 1}] for k in cals}
        sr = StreamRequest(nutnr_sk, [2443], coefficients, tr, {}, request_id='UNIT')
        nutnr_ds = xray.open_dataset(os.path.join(TEST_DIR, 'data', nutnr_fn), decode_times=False)
        ctdpf_ds = xray.open_dataset(os.path.join(TEST_DIR, 'data', ctdpf_fn), decode_times=False)

        sr.datasets[ctdpf_sk] = ctdpf_ds
        sr.datasets[nutnr_sk] = nutnr_ds
        sr.calculate_derived_products()

        ds = sr.datasets[ctdpf_sk]
        tempwat = ctd_sbe16plus_tempwat(ds.temperature,
                                        cals['CC_a0'], cals['CC_a1'],
                                        cals['CC_a2'], cals['CC_a3'])
        np.testing.assert_array_equal(ds.seawater_temperature, tempwat)

        pracsal = ctd_pracsal(ds.seawater_conductivity, ds.seawater_temperature, ds.seawater_pressure)
        np.testing.assert_array_equal(ds.practical_salinity, pracsal)

        response = json.loads(JsonResponse(sr).json())
        self.assertEqual(len(response), len(nutnr_ds.time.values))

    def test_qc(self):
        nutnr_sk = StreamKey('CE04OSPS', 'SF01B', '4A-NUTNRA102', 'streamed', 'nutnr_a_sample')
        ctdpf_sk = StreamKey('CE04OSPS', 'SF01B', '2A-CTDPFA107', 'streamed', 'ctdpf_sbe43_sample')
        nutnr_fn = 'nutnr_a_sample.nc'
        ctdpf_fn = 'ctdpf_sbe43_sample.nc'

        cals = json.load(open(os.path.join(TEST_DIR, 'data', 'cals.json')))
        qc = json.load(open(os.path.join(TEST_DIR, 'data', 'qc.json')))

        tr = TimeRange(3.65342400e+09, 3.65351040e+09)
        coefficients = {k: [{'start': tr.start-1, 'stop': tr.stop+1, 'value': cals[k], 'deployment': 1}] for k in cals}
        sr = StreamRequest(nutnr_sk, [2443], coefficients, tr, {}, qc_parameters=qc, request_id='UNIT')
        nutnr_ds = xray.open_dataset(os.path.join(TEST_DIR, 'data', nutnr_fn), decode_times=False)
        ctdpf_ds = xray.open_dataset(os.path.join(TEST_DIR, 'data', ctdpf_fn), decode_times=False)

        sr.datasets[ctdpf_sk] = ctdpf_ds
        sr.datasets[nutnr_sk] = nutnr_ds
        sr.calculate_derived_products()

        ds = sr.datasets[ctdpf_sk]
        tempwat = ctd_sbe16plus_tempwat(ds.temperature,
                                        cals['CC_a0'], cals['CC_a1'],
                                        cals['CC_a2'], cals['CC_a3'])
        np.testing.assert_array_equal(ds.seawater_temperature, tempwat)

        pracsal = ctd_pracsal(ds.seawater_conductivity, ds.seawater_temperature, ds.seawater_pressure)
        np.testing.assert_array_equal(ds.practical_salinity, pracsal)

        response = json.loads(JsonResponse(sr).json())
        self.assertEqual(len(response), len(nutnr_ds.time.values))

    @unittest.skip('dump csv')
    def test_all_streams(self):
        # this is used to dump the results of attempting to resolve all needs for all streams
        # given the data in stream_metadata.csv
        results = []
        for _, subsite, node, sensor, method, stream, _, first, last in metadata.itertuples():
            sk = StreamKey(subsite, node, sensor, method, stream)
            if sk.stream.needs:
                tr = TimeRange(first, last)
                sr = StreamRequest(sk, [], {}, tr, {})
                if len(sr.stream_parameters) > 1:
                    for key in set(sr.stream_parameters) - {sk}:
                        results.append(sk.as_tuple() +
                                       (str(sk.stream.needs),) +
                                       key.as_tuple() +
                                       (', '.join(['%s(%d)' % (p.name, p.id) for p in sr.stream_parameters[key]]),))
                else:
                    results.append(sk.as_tuple() + (str(sk.stream.needs),) + tuple([''] * 6))

        with open('out.csv', 'w') as fh:
            writer = csv.writer(fh)
            writer.writerow('subsite node sensor method stream params subsite node sensor method stream params'.split())
            writer.writerows(results)
