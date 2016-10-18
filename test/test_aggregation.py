import logging
import os
import shutil
import tempfile
import unittest

import numpy as np
import xarray as xr

from preload_database.database import initialize_connection, open_connection, PreloadDatabaseMode
from util.aggregation import aggregate_provenance_json, aggregate_netcdf_group

logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)

initialize_connection(PreloadDatabaseMode.POPULATED_MEMORY)
open_connection()

TEST_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(TEST_DIR, 'data')


class AggregationTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    # def test_aggregate_json(self):
    #     aggregate_provenance_json(self.jobdir)

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
        expected = os.path.join(self.tempdir, 'AGG_test_19000101T000001-19000101T000006.nc')
        self.assertTrue(os.path.exists(expected))

        with xr.open_dataset(expected) as ds:
            self.assertIn('x', ds)
            self.assertIn('time', ds)
            self.assertIn('obs', ds)
            self.assertTrue((np.diff(ds.obs.values) > 0).all())
            self.assertTrue((np.diff(ds.time.values) > 0).all())

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
        expected = os.path.join(self.tempdir, 'AGG_test_19000101T000001-19000101T000006.nc')
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
        expected = os.path.join(self.tempdir, 'AGG_test_19000101T000001-19000101T000006.nc')
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
        expected = os.path.join(self.tempdir, 'AGG_test_19000101T000001-19000101T000006.nc')
        self.assertTrue(os.path.exists(expected))

        with xr.open_dataset(expected) as ds:
            self.assertIn('x', ds)
            np.testing.assert_equal(ds.x.values, [[1, 2], [3, 4], [-9999999, -9999999],
                                                  [5, 6],[-9999999, -9999999], [-9999999, -9999999]])

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
        expected = os.path.join(self.tempdir, 'AGG_test_19000101T000001-19000101T000006.nc')
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
        expected = os.path.join(self.tempdir, 'AGG_test_19000101T000001-19000101T000006.nc')
        self.assertTrue(os.path.exists(expected))

        with xr.open_dataset(expected) as ds:
            self.assertIn('x', ds)
            np.testing.assert_equal(ds.x.values, [[1, 2, -9999999],
                                                  [3, 4, -9999999],
                                                  [1, 2, 3],
                                                  [5, 6, -9999999],
                                                  [4, 5, 6],
                                                  [7, 8, 9]])
