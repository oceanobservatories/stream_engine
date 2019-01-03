import logging
import os
import shutil
import tempfile
import unittest

import numpy as np
import xarray as xr
from ooi_data.postgres.model import MetadataBase

from preload_database.database import create_engine_from_url, create_scoped_session
from util.aggregation import aggregate_netcdf_group

logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)

engine = create_engine_from_url(None)
session = create_scoped_session(engine)
MetadataBase.query = session.query_property()

TEST_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(TEST_DIR, 'data')


class AggregationTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_aggregate_netcdf_group_simple(self):
        f1 = os.path.join(self.tempdir, 'f1.nc')
        f2 = os.path.join(self.tempdir, 'f2.nc')
        ds = xr.Dataset()
        ds['time'] = (['obs'], [1, 2, 4], {})
        ds['x'] = (['obs'], [1, 2, 3], {})
        ds.to_netcdf(f1)
        ds['time'] = (['obs'], [3, 5, 6], {})
        ds.to_netcdf(f2)

        aggregate_netcdf_group(self.tempdir, self.tempdir, [f1, f2], 'test')
        expected = os.path.join(self.tempdir, 'test_19000101T000001-19000101T000006.nc')
        self.assertTrue(os.path.exists(expected))

        with xr.open_dataset(expected) as ds:
            self.assertIn('x', ds)
            self.assertIn('time', ds)
            self.assertIn('obs', ds)
            self.assertTrue((np.diff(ds.obs.values) > 0).all())
            self.assertTrue((np.diff(ds.time.values) > 0).all())

    def test_aggregate_netcdf_handles_mismatched_coordinates(self):
        f1 = os.path.join(self.tempdir, 'f1.nc')
        f2 = os.path.join(self.tempdir, 'f2.nc')
        ds = xr.Dataset()
        ds['time'] = (['obs'], [1, 2, 4], {})
        ds['x'] = (['obs'], [1, 2, 3], {})
        ds.coords['obs'] = ('obs', [1, 2, 3])
        ds.coords['lat'] = ('obs', [1, 2, 3])
        ds.coords['lon'] = ('obs', [1, 2, 3])
        ds.to_netcdf(f1)
        ds['time'] = (['obs'], [3, 5, 6], {})
        ds.coords['depth'] = ('obs', [1, 2, 3])
        ds.to_netcdf(f2)

        aggregate_netcdf_group(self.tempdir, self.tempdir, [f1, f2], 'test')
        expected = os.path.join(self.tempdir, 'test_19000101T000001-19000101T000006.nc')
        self.assertTrue(os.path.exists(expected))

        with xr.open_dataset(expected) as ds:
            self.assertIn('x', ds)
            self.assertIn('time', ds)
            self.assertIn('obs', ds.coords)
            self.assertIn('lat', ds.coords)
            self.assertIn('lon', ds.coords)
            self.assertNotIn('depth', ds.coords)
            self.assertIn('depth', ds)

    def test_aggregate_netcdf_handles_mismatched_coordinates_in_first_dataset(self):
        f1 = os.path.join(self.tempdir, 'f1.nc')
        f2 = os.path.join(self.tempdir, 'f2.nc')
        ds = xr.Dataset()
        ds['time'] = (['obs'], [1, 2, 4], {})
        ds['x'] = (['obs'], [1, 2, 3], {})
        ds.coords['obs'] = ('obs', [1, 2, 3])
        ds.coords['lat'] = ('obs', [1, 2, 3])
        ds.coords['lon'] = ('obs', [1, 2, 3])
        ds.coords['depth'] = ('obs', [1, 2, 3])
        ds.to_netcdf(f1)
        ds['time'] = (['obs'], [3, 5, 6], {})
        del ds.coords['depth']
        ds.to_netcdf(f2)

        aggregate_netcdf_group(self.tempdir, self.tempdir, [f1, f2], 'test')
        expected = os.path.join(self.tempdir, 'test_19000101T000001-19000101T000006.nc')
        self.assertTrue(os.path.exists(expected))

        with xr.open_dataset(expected) as ds:
            self.assertIn('x', ds)
            self.assertIn('time', ds)
            self.assertIn('obs', ds.coords)
            self.assertIn('lat', ds.coords)
            self.assertIn('lon', ds.coords)
            self.assertNotIn('depth', ds.coords)
            self.assertIn('depth', ds)

    def test_aggregate_netcdf_group_missing(self):
        f1 = os.path.join(self.tempdir, 'f1.nc')
        f2 = os.path.join(self.tempdir, 'f2.nc')
        ds = xr.Dataset()
        ds['time'] = (['obs'], [1, 2, 4], {})
        ds['x'] = (['obs'], [1, 2, 3], {})
        ds.to_netcdf(f1)
        ds['time'] = (['obs'], [3, 5, 6], {})
        del ds['x']
        ds.to_netcdf(f2)

        aggregate_netcdf_group(self.tempdir, self.tempdir, [f1, f2], 'test')
        expected = os.path.join(self.tempdir, 'test_19000101T000001-19000101T000006.nc')
        self.assertTrue(os.path.exists(expected))

        with xr.open_dataset(expected) as ds:
            self.assertIn('x', ds)
            np.testing.assert_equal(ds.x.values, [1, 2, -9999999, 3, -9999999, -9999999])

    def test_aggregate_netcdf_group_2d(self):
        f1 = os.path.join(self.tempdir, 'f1.nc')
        f2 = os.path.join(self.tempdir, 'f2.nc')
        ds = xr.Dataset()
        ds['time'] = (['obs'], [1, 2, 4], {})
        ds['x'] = (['obs', 'dim1'], [[1, 2], [3, 4], [5, 6]], {})
        ds.to_netcdf(f1)
        ds['time'] = (['obs'], [3, 5, 6], {})
        ds.to_netcdf(f2)

        aggregate_netcdf_group(self.tempdir, self.tempdir, [f1, f2], 'test')
        expected = os.path.join(self.tempdir, 'test_19000101T000001-19000101T000006.nc')
        self.assertTrue(os.path.exists(expected))

        with xr.open_dataset(expected) as ds:
            self.assertIn('x', ds)
            self.assertIn('time', ds)
            self.assertIn('obs', ds)
            self.assertTrue((np.diff(ds.obs.values) > 0).all())
            self.assertTrue((np.diff(ds.time.values) > 0).all())

    def test_aggregate_netcdf_group_2d_missing(self):
        f1 = os.path.join(self.tempdir, 'f1.nc')
        f2 = os.path.join(self.tempdir, 'f2.nc')
        ds = xr.Dataset()
        ds['time'] = (['obs'], [1, 2, 4], {})
        ds['x'] = (['obs', 'dim1'], [[1, 2], [3, 4], [5, 6]], {})
        ds.to_netcdf(f1)
        ds['time'] = (['obs'], [3, 5, 6], {})
        del ds['x']
        ds.to_netcdf(f2)

        aggregate_netcdf_group(self.tempdir, self.tempdir, [f1, f2], 'test')
        expected = os.path.join(self.tempdir, 'test_19000101T000001-19000101T000006.nc')
        self.assertTrue(os.path.exists(expected))

        with xr.open_dataset(expected) as ds:
            self.assertIn('x', ds)
            np.testing.assert_equal(ds.x.values, [[1, 2], [3, 4], [-9999999, -9999999],
                                                  [5, 6], [-9999999, -9999999], [-9999999, -9999999]])

    def test_aggregate_netcdf_group_2d_scalar_mixed(self):
        f1 = os.path.join(self.tempdir, 'f1.nc')
        f2 = os.path.join(self.tempdir, 'f2.nc')
        ds = xr.Dataset()
        ds['time'] = (['obs'], [1, 2, 4], {})
        ds['x'] = (['obs', 'dim1'], [[1, 2], [3, 4], [5, 6]], {})
        ds.to_netcdf(f1)
        ds['time'] = (['obs'], [3, 5, 6], {})
        ds['x'] = (['obs'], [1, 2, 3], {})
        ds.to_netcdf(f2)

        aggregate_netcdf_group(self.tempdir, self.tempdir, [f1, f2], 'test')
        expected = os.path.join(self.tempdir, 'test_19000101T000001-19000101T000006.nc')
        self.assertTrue(os.path.exists(expected))

        with xr.open_dataset(expected) as ds:
            self.assertIn('x', ds)
            np.testing.assert_equal(ds.x.values, [[1, 2], [3, 4], [1, -9999999],
                                                  [5, 6], [2, -9999999], [3, -9999999]])

    def test_aggregate_netcdf_group_2d_varying_size(self):
        f1 = os.path.join(self.tempdir, 'f1.nc')
        f2 = os.path.join(self.tempdir, 'f2.nc')
        ds = xr.Dataset()
        ds['time'] = (['obs'], [1, 2, 4], {})
        ds['x'] = (['obs', 'dim1'], [[1, 2], [3, 4], [5, 6]], {})
        ds.to_netcdf(f1)
        ds = xr.Dataset()
        ds['time'] = (['obs'], [3, 5, 6], {})
        ds['x'] = (['obs', 'dim1'], [[1, 2, 3], [4, 5, 6], [7, 8, 9]], {})
        ds.to_netcdf(f2)

        aggregate_netcdf_group(self.tempdir, self.tempdir, [f1, f2], 'test')
        expected = os.path.join(self.tempdir, 'test_19000101T000001-19000101T000006.nc')
        self.assertTrue(os.path.exists(expected))

        with xr.open_dataset(expected) as ds:
            self.assertIn('x', ds)
            np.testing.assert_equal(ds.x.values, [[1, 2, -9999999],
                                                  [3, 4, -9999999],
                                                  [1, 2, 3],
                                                  [5, 6, -9999999],
                                                  [4, 5, 6],
                                                  [7, 8, 9]])
            self.assertNotIn('TEMP_DIM_x_1', ds)

    def test_aggregate_missing_string(self):
        f1 = os.path.join(self.tempdir, 'f1.nc')
        f2 = os.path.join(self.tempdir, 'f2.nc')
        ds = xr.Dataset()
        ds['time'] = (['obs'], [1, 2, 4], {})
        ds['x'] = (['obs'], ['test', 'test', 'test'], {})
        ds.to_netcdf(f1)
        ds = xr.Dataset()
        ds['time'] = (['obs'], [3, 5, 6], {})
        ds['x'] = (['obs'], ['', '', ''], {})
        ds.to_netcdf(f2)

        aggregate_netcdf_group(self.tempdir, self.tempdir, [f1, f2], 'test')
        expected = os.path.join(self.tempdir, 'test_19000101T000001-19000101T000006.nc')
        self.assertTrue(os.path.exists(expected))

        with xr.open_dataset(expected) as ds:
            self.assertIn('x', ds)
            np.testing.assert_equal(ds.x.values, ['test',
                                                  'test',
                                                  '',
                                                  'test',
                                                  '',
                                                  ''])

    def test_read_2d_string_variable(self):
        a = np.array([['a'], ['b'], ['c']], 'str')
        f1 = os.path.join(self.tempdir, 'f1.nc')
        f2 = os.path.join(self.tempdir, 'f2.nc')
        ds = xr.Dataset()
        ds['time'] = (['obs'], [1, 2, 4], {})
        ds['x'] = (['obs', 'xdim'], a, {})
        ds.to_netcdf(f1)
        ds = xr.Dataset()
        ds['time'] = (['obs'], [3, 5, 6], {})
        ds['x'] = (['obs', 'xdim'], a, {})
        ds.to_netcdf(f2)

        aggregate_netcdf_group(self.tempdir, self.tempdir, [f1, f2], 'test')
        expected = os.path.join(self.tempdir, 'test_19000101T000001-19000101T000006.nc')
        self.assertTrue(os.path.exists(expected))

        with xr.open_dataset(expected) as ds:
            self.assertIn('x', ds)
            np.testing.assert_equal(ds.x.values, ['a', 'b', 'a', 'c', 'b', 'c'])

    def test_read_2d_empty_string_variable(self):
        a = np.array([[], [], []], 'str')
        f1 = os.path.join(self.tempdir, 'f1.nc')
        f2 = os.path.join(self.tempdir, 'f2.nc')
        ds = xr.Dataset()
        ds['time'] = (['obs'], [1, 2, 4], {})
        ds['x'] = (['obs', 'xdim'], a, {})
        ds.to_netcdf(f1)
        ds = xr.Dataset()
        ds['time'] = (['obs'], [3, 5, 6], {})
        ds['x'] = (['obs', 'xdim'], a, {})
        ds.to_netcdf(f2)

        aggregate_netcdf_group(self.tempdir, self.tempdir, [f1, f2], 'test')
        expected = os.path.join(self.tempdir, 'test_19000101T000001-19000101T000006.nc')
        self.assertTrue(os.path.exists(expected))

        with xr.open_dataset(expected) as ds:
            self.assertIn('x', ds)
            np.testing.assert_equal(ds.x.values, np.concatenate([a, a]))
