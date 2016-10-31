import msgpack

import global_test_setup

import logging
import os

from unittest import TestCase
import xarray as xr
import numpy as np

from preload_database.database import initialize_connection, PreloadDatabaseMode, open_connection
from preload_database.model.preload import Stream, Parameter
from util.common import StreamKey
from util.datamodel import to_xray_dataset, _get_fill_value, _replace_values

TEST_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(TEST_DIR, 'data')
initialize_connection(PreloadDatabaseMode.POPULATED_MEMORY)
open_connection()

logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)


class DataModelTest(TestCase):
    def find_int64_vars(self, ds):
        found = set()
        for var in ds.data_vars:
            if ds[var].dtype == np.dtype('int64'):
                found.add(var)
        return found

    def test_no_int64(self):
        echo_fn = 'echo_sounding.nc'
        echo_sk = StreamKey('RS01SLBS', 'LJ01A', '05-HPIESA101', 'streamed', 'echo_sounding')
        echo_ds = xr.open_dataset(os.path.join(DATA_DIR, echo_fn), decode_times=False)

        # turn the dataset back into a dataframe, then into rows
        echo_df = echo_ds.to_dataframe()
        cols = echo_df.columns
        rows = list(echo_df.itertuples(index=False))

        ds = to_xray_dataset(cols, rows, echo_sk, None)

        # first, verify there were 64-bit vars in the original dataset
        found = self.find_int64_vars(echo_ds)
        self.assertNotEqual(found, set())

        # second, verify there are no 64-bit vars in the output dataset
        found = self.find_int64_vars(ds)
        self.assertEqual(found, set())

    def test_shared_dimensions(self):
        adcp_fn = 'deployment0000_RS03AXBS-LJ03A-10-ADCPTE301-streamed-adcp_velocity_beam.nc'
        adcp_sk = StreamKey('RS03AXBS', 'LJ03A', '10-ADCPTE301', 'streamed', 'adcp_velocity_beam')
        adcp_ds = xr.open_dataset(os.path.join(DATA_DIR, adcp_fn), decode_times=False)

        # grab the stream from preload
        stream = Stream.query.filter(Stream.name == 'adcp_velocity_beam').first()
        params = [p.name for p in stream.parameters if not p.is_function]

        # transform into row data suitable for to_xray_dataset
        rows = []

        for i in adcp_ds.obs.values:
            row = []
            for col in params:
                data = adcp_ds[col].values[i]
                if isinstance(data, np.ndarray) and data.shape:
                    if 'velocity_beam' in col:
                        data[np.isnan(data)] = -32768
                        data = data.astype('int64')
                    data = msgpack.packb(list(data))
                row.append(data)
            rows.append(row)

        # create the dataset
        ds = to_xray_dataset(params, rows, adcp_sk, None)
        # verify only two dimensions exists, bin and obs
        self.assertEqual(set(ds.dims), {'bin', 'obs'})

    def test_get_fill_value(self):
        for param in Parameter.query:
            fill = _get_fill_value(param)
            self.assertIsNotNone(fill)
            if isinstance(fill, basestring):
                self.assertEqual(fill, '')

    def test_replace_values_string_arrays(self):
        data_slice = np.array(['\x91\xb6 No GPS Data Available', '\xc0', '\x91\xac NO RDA DATA',
                               '\x92\xac NO RDA DATA\xac NO RDA DATA', '\x91\xac NO RDA DATA',
                               '\xc0', '\xc0', '\xc0', '\xc0', '\xc0'])
        value_encoding = 'string'
        fill_value = ''
        is_array = True
        name = 'test'
        rval = _replace_values(data_slice, value_encoding, fill_value, is_array, name)
        self.assertEqual(rval.shape, (10, 2))
        self.assertEqual(rval[0][0], ' No GPS Data Available')
        self.assertEqual(rval[0][1], '')
        self.assertEqual(rval[1][0], '')
        self.assertEqual(rval[1][1], '')
        self.assertEqual(rval[2][0], ' NO RDA DATA')
        self.assertEqual(rval[2][1], '')
        self.assertEqual(rval[3][0], ' NO RDA DATA')
        self.assertEqual(rval[3][1], ' NO RDA DATA')
        self.assertEqual(rval[4][0], ' NO RDA DATA')
        self.assertEqual(rval[4][1], '')
        self.assertEqual(rval[5][0], '')
        self.assertEqual(rval[5][1], '')