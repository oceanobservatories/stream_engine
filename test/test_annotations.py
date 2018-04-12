import logging
import os
import unittest

import ntplib
import numpy as np
from ooi_data.postgres.model import MetadataBase

from preload_database.database import create_engine_from_url, create_scoped_session
from util.annotation import AnnotationServiceInterface, AnnotationStore, AnnotationRecord
from util.common import StreamKey, TimeRange

logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)

engine = create_engine_from_url(None)
session = create_scoped_session(engine)
MetadataBase.query = session.query_property()

TEST_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(TEST_DIR, 'data')


class AnnotationTest(unittest.TestCase):
    def setUp(self):
        self.amhost = 'localhost'
        self.refdes = 'CE04OSPS-SF01B-2A-CTDPFA107'
        self.sk = StreamKey('CP01CNSM', 'MFD37', '04-DOSTAD000', 'telemetered', 'dosta_abcdjm_dcl_instrument')
        # AnnotationStore will only add one AnnotationRecord with a given id - use this to increment id
        self.annotation_id_counter = 0

    def test_create_interface(self):
        return AnnotationServiceInterface(self.amhost)

    @unittest.skip('Requires running uframe')
    def test_get(self):
        anno_interface = self.test_create_interface()
        key = StreamKey('CE04OSPS', 'SF01B', '2A-CTDPFA107', 'test', 'test')
        time_range = TimeRange(3684009000, 3685000000)
        for anno in anno_interface.find_annotations(key, time_range):
            print anno._tuple

    def _create_exclusion_anno(self, streamkey, start, stop):
        # increment id
        self.annotation_id_counter += 1
        key = streamkey.as_dict()
        return AnnotationRecord(id=self.annotation_id_counter, beginDT=start, endDT=stop, subsite=key['subsite'],
                                node=key['node'], sensor=key['sensor'], method=key['method'], stream=key['stream'],
                                exclusionFlag=True)

    def _test_single_exclusion(self, streamkey, tstart, tstop, astart, astop, expected):
        return self._test_multiple_exclusions(streamkey, tstart, tstop, [(astart, astop)], expected)

    def _test_multiple_exclusions(self, streamkey, tstart, tstop, annos, expected):
        # all times in whole seconds since 1970
        # adapt to expected formats
        times = np.arange(ntplib.system_to_ntp_time(tstart), ntplib.system_to_ntp_time(tstop + 1))
        store = AnnotationStore()
        store.add_annotations([self._create_exclusion_anno(streamkey, start*1000, stop*1000) for start, stop in annos])
        mask = store.get_exclusion_mask(streamkey, times)
        self.assertEqual(list(mask), expected)

    def test_exclude_all(self):
        self._test_single_exclusion(self.sk, 1, 5, 1, 5, [False, False, False, False, False])

    def test_exclude_single(self):
        self._test_single_exclusion(self.sk, 1, 5, 1, 4, [False, False, False, False, True])
        self._test_single_exclusion(self.sk, 1, 5, 2, 5, [True, False, False, False, False])
        self._test_single_exclusion(self.sk, 1, 5, 2, 4, [True, False, False, False, True])

    def test_exclude_multiple_non_overlapping(self):
        self._test_multiple_exclusions(self.sk, 1, 10, [(1, 2), (9, 10)],
                                       [False, False, True, True, True, True, True, True, False, False])
        self._test_multiple_exclusions(self.sk, 1, 10, [(3, 4), (6, 8)],
                                       [True, True, False, False, True, False, False, False, True, True])
        self._test_multiple_exclusions(self.sk, 1, 10, [(1, 2), (3, 4)],
                                       [False, False, False, False, True, True, True, True, True, True])

    def test_exclude_multiple_overlapping(self):
        self._test_multiple_exclusions(self.sk, 1, 10, [(1, 4), (3, 5)],
                                       [False, False, False, False, False, True, True, True, True, True])
        self._test_multiple_exclusions(self.sk, 1, 10, [(1, 4), (2, 3)],
                                       [False, False, False, False, True, True, True, True, True, True])

    def test_non_matching(self):
        self._test_single_exclusion(self.sk, 1, 5, 7, 10, [True] * 5)

    def test_bigger(self):
        self._test_single_exclusion(self.sk, 5, 10, 1, 20, [False] * 6)

    def test_bigger_one_side(self):
        self._test_single_exclusion(self.sk, 5, 10, 1, 8, [False, False, False, False, True, True])
        self._test_single_exclusion(self.sk, 5, 10, 7, 20, [True, True, False, False, False, False])
