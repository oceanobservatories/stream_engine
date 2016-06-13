import json
import logging
import os
import unittest

import mock
import pandas as pd
import xarray as xr

from preload_database.database import initialize_connection, open_connection, PreloadDatabaseMode
from util.common import StreamKey, TimeRange
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
class BotptVirtualTest(unittest.TestCase):
    metadata = []
    base_params = ['time', 'deployment', 'provenance']

    def test_botpt_15s(self):
        botpt_sk = StreamKey('RS03ECAL', 'MJ03E', '06-BOTPTA302', 'streamed', 'botpt_nano_sample')
        botpt_15s_sk = StreamKey('RS03ECAL', 'MJ03E', '06-BOTPTA302', 'streamed', 'botpt_nano_sample_15s')
        botpt_fn = 'deployment0001_RS03ECAL-MJ03E-06-BOTPTA302-streamed-botpt_nano_sample.nc'

        cals = json.load(open(os.path.join(DATA_DIR, 'cals.json')))

        tr = TimeRange(3674160000.0, 3674181600.1)
        coefficients = {k: [{'start': tr.start - 1, 'stop': tr.stop + 1, 'value': cals[k], 'deployment': 1}] for k in
                        cals}
        sr = StreamRequest(botpt_15s_sk, [], coefficients, tr, {}, request_id='UNIT')
        botps_ds = xr.open_dataset(os.path.join(DATA_DIR, botpt_fn), decode_times=False)

        botps_ds = botps_ds[self.base_params + [p.name for p in sr.stream_parameters[botpt_sk]]]

        sr.datasets[botpt_sk] = StreamDataset(botpt_sk, sr.coefficients, sr.uflags, [], sr.request_id)
        sr.datasets[botpt_15s_sk] = StreamDataset(botpt_15s_sk, sr.coefficients, sr.uflags, [botpt_sk], sr.request_id)
        sr.datasets[botpt_sk]._insert_dataset(botps_ds)

        sr.calculate_derived_products()

        result = sr.datasets[botpt_15s_sk].datasets[1]
        self.assertIn('botsflu_time15s', result)
        self.assertIn('botsflu_meanpres', result)
        self.assertIn('botsflu_meandepth', result)
        self.assertIn('botsflu_5minrate', result)
        self.assertIn('botsflu_10minrate', result)
        self.assertIn('botsflu_predtide', result)

    def test_botpt_24hr(self):
        botpt_sk = StreamKey('RS03ECAL', 'MJ03E', '06-BOTPTA302', 'streamed', 'botpt_nano_sample')
        botpt_24hr_sk = StreamKey('RS03ECAL', 'MJ03E', '06-BOTPTA302', 'streamed', 'botpt_nano_sample_24hr')
        botpt_fn = 'deployment0001_RS03ECAL-MJ03E-06-BOTPTA302-streamed-botpt_nano_sample.nc'

        cals = json.load(open(os.path.join(DATA_DIR, 'cals.json')))

        tr = TimeRange(3674160000.0, 3674181600.1)
        coefficients = {k: [{'start': tr.start - 1, 'stop': tr.stop + 1, 'value': cals[k], 'deployment': 1}] for k in
                        cals}
        sr = StreamRequest(botpt_24hr_sk, [], coefficients, tr, {}, request_id='UNIT')
        botps_ds = xr.open_dataset(os.path.join(DATA_DIR, botpt_fn), decode_times=False)

        botps_ds = botps_ds[self.base_params + [p.name for p in sr.stream_parameters[botpt_sk]]]

        sr.datasets[botpt_sk] = StreamDataset(botpt_sk, sr.coefficients, sr.uflags, [], sr.request_id)
        sr.datasets[botpt_24hr_sk] = StreamDataset(botpt_24hr_sk, sr.coefficients, sr.uflags, [botpt_sk], sr.request_id)
        sr.datasets[botpt_sk]._insert_dataset(botps_ds)

        sr.calculate_derived_products()

        result = sr.datasets[botpt_24hr_sk].datasets[1]
        self.assertIn('botsflu_time24h', result)  # TODO - input is currently defined as TIME15S, should be TIME?
        self.assertIn('botsflu_daydepth', result)
        self.assertIn('botsflu_4wkrate', result)
        self.assertIn('botsflu_8wkrate', result)
