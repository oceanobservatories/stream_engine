import logging
import unittest

import numpy as np
from ooi_data.postgres.model import MetadataBase

from preload_database.database import create_engine_from_url, create_scoped_session
from util.netcdf_utils import max_shape, max_dtype

logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)

sqla_engine = create_engine_from_url(None)
session = create_scoped_session(sqla_engine)
MetadataBase.query = session.query_property()


class NetcdfUtilsTest(unittest.TestCase):
    def test_max_size(self):
        self.assertEqual(max_shape((), (1,)), (1,))
        self.assertEqual(max_shape((1, ), (1,)), (1,))
        self.assertEqual(max_shape((1,), (2,)), (2,))
        self.assertEqual(max_shape((2,), (1,)), (2,))
        self.assertEqual(max_shape((1, 2), (2,)), (2, 2))
        self.assertEqual(max_shape((1, 2), (2, 3)), (2, 3))

    def test_max_dtype(self):
        b1 = np.dtype(np.bool)
        i8 = np.dtype(np.int8)
        i16 = np.dtype(np.int16)
        i32 = np.dtype(np.int32)
        i64 = np.dtype(np.int64)

        u8 = np.dtype(np.uint8)
        u16 = np.dtype(np.uint16)
        u32 = np.dtype(np.uint32)
        u64 = np.dtype(np.uint64)

        f32 = np.dtype(np.float32)
        f64 = np.dtype(np.float64)

        string = np.dtype(np.str)

        # bool
        self.assertEqual(max_dtype(b1, i8), i8)
        self.assertEqual(max_dtype(b1, i16), i16)
        self.assertEqual(max_dtype(b1, i32), i32)
        self.assertEqual(max_dtype(b1, i64), i64)
        self.assertEqual(max_dtype(b1, f32), f32)
        self.assertEqual(max_dtype(b1, f64), f64)

        # two signed integers
        self.assertEqual(max_dtype(i8, i8), i8)
        self.assertEqual(max_dtype(i8, i16), i16)
        self.assertEqual(max_dtype(i32, i8), i32)
        self.assertEqual(max_dtype(i64, i32), i64)

        # mixed signed/unsigned
        self.assertEqual(max_dtype(u8, i8), i16)
        self.assertEqual(max_dtype(u8, i32), i32)
        self.assertEqual(max_dtype(u32, i32), i64)
        self.assertEqual(max_dtype(u16, i8), i32)
        self.assertEqual(max_dtype(i8, u64), f64)

        # mixed integer/float
        self.assertEqual(max_dtype(f32, i8), f32)
        self.assertEqual(max_dtype(f32, i16), f32)
        self.assertEqual(max_dtype(f32, i32), f64)
        self.assertEqual(max_dtype(i8, f32), f32)
        self.assertEqual(max_dtype(i32, f32), f64)
        self.assertEqual(max_dtype(f64, i8), f64)
        self.assertEqual(max_dtype(f64, u64), f64)

        # string plus anything
        self.assertEqual(max_dtype(string, f32), string)
        self.assertEqual(max_dtype(string, f64), string)
        self.assertEqual(max_dtype(string, i32), string)
        self.assertEqual(max_dtype(string, u32), string)
