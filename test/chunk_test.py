__author__ = 'Stephen Zakrewsky'

import numpy
from util import chunks
from util.chunks import Chunk
from util.calc import Average_Generator, Interpolation_Generator


class Test_StreamRequest(object):

    def __init__(self, times):
        self.times = times


class Test_Chunk_Generator(object):

    def __init__(self):
        self.chunks_list = [
            { 7: {'source': 'foo', 'data': numpy.array([2,4,8])},
              8: {'source': 'foo', 'data': numpy.array([1,2,3])},
              9: {'source': 'foo', 'data': numpy.array([[1,2],[1,2],[1,2]])},
             10: {'source': 'foo', 'data': numpy.array(['str1','str2','str3'])}},
            { 7: {'source': 'foo', 'data': numpy.array([9])},
              8: {'source': 'foo', 'data': numpy.array([4])},
              9: {'source': 'foo', 'data': numpy.array([[1,2]])},
             10: {'source': 'foo', 'data': numpy.array(['str1'])}},
            { 7: {'source': 'foo', 'data': numpy.array([13, 19])},
              8: {'source': 'foo', 'data': numpy.array([7,8])},
              9: {'source': 'foo', 'data': numpy.array([[1,2],[1,2]])},
             10: {'source': 'foo', 'data': numpy.array(['str1','str2'])}}
        ]
        self.empty_chunk = \
            { 7: {'source': 'foo', 'data': numpy.empty((0,), dtype=int)},
              8: {'source': 'foo', 'data': numpy.empty((0,), dtype=int)},
              9: {'source': 'foo', 'data': numpy.empty((0,2,), dtype=int)},
             10: {'source': 'foo', 'data': numpy.empty((0,), dtype='|S4')}}

    def chunks(self, r=None):
        for chunk in self.chunks_list:
            yield chunk


def test_axioms():
    cg = Test_Chunk_Generator()

    assert len(Chunk(cg.empty_chunk)) == 0
    assert not Chunk(cg.empty_chunk)

    assert len(Chunk(cg.chunks_list[0])) > 0
    assert Chunk(cg.chunks_list[0])

    test_chunk = \
        { 7: {'source': 'foo', 'data': numpy.array([2,4,8])},
          8: {'source': 'foo', 'data': numpy.array([1,2,3])},
          9: {'source': 'foo', 'data': numpy.array([[1,2],[1,2],[1,2]])},
         10: {'source': 'foo', 'data': numpy.array(['str1','str2','str3'])}}

    assert chunks.is_equal(test_chunk, cg.chunks_list[0])
    assert Chunk(test_chunk) == cg.chunks_list[0]
    assert not chunks.is_equal(test_chunk, cg.chunks_list[1])
    assert Chunk(test_chunk) != cg.chunks_list[1]


def test_concatenate():
    cg = Test_Chunk_Generator()

    test_chunk = \
        { 7: {'source': 'foo', 'data': numpy.array([2,4,8,9])},
          8: {'source': 'foo', 'data': numpy.array([1,2,3,4])},
          9: {'source': 'foo', 'data': numpy.array([[1,2],[1,2],[1,2],[1,2]])},
         10: {'source': 'foo', 'data': numpy.array(['str1','str2','str3','str1'])}}

    chunk = chunks.concatenate(cg.chunks_list[0], cg.chunks_list[1])
    assert chunk == test_chunk
    assert chunk == chunks.concatenate(cg.empty_chunk, chunk)
    assert chunk == chunks.concatenate(chunk, cg.empty_chunk)


def test_slice():
    cg = Test_Chunk_Generator()

    chunk = chunks.concatenate(cg.chunks_list[0], cg.chunks_list[1])
    chunk0_length = len(Chunk(cg.chunks_list[0]))
    concatenated_length = len(chunk)

    slice1 = chunk[:chunk0_length]
    slice2 = chunk[chunk0_length:]
    assert cg.chunks_list[0] == slice1
    assert cg.chunks_list[1] == slice2

    slice1 = chunk[:0]
    slice2 = chunk[0:]
    assert cg.empty_chunk == slice1
    assert chunk == slice2

    slice1 = chunk[:concatenated_length]
    slice2 = chunk[concatenated_length:]
    assert chunk == slice1
    assert cg.empty_chunk == slice2

    slice_end = chunk[-1]
    assert slice_end == \
           { 7: {'source': 'foo', 'data': numpy.array([9])},
             8: {'source': 'foo', 'data': numpy.array([4])},
             9: {'source': 'foo', 'data': numpy.array([[1,2]])},
            10: {'source': 'foo', 'data': numpy.array(['str1'])}}


def test_with_times():
    cg = Test_Chunk_Generator()

    avg_chunk = \
        { 7: {'source': 'foo', 'data': numpy.array([9,14,15])},
          8: {'source': 'foo', 'data': numpy.array([5.5, numpy.NAN, 8])},
          9: {'source': 'foo', 'data': numpy.array([[1,2],[-9223372036854775808,-9223372036854775808],[1,2]])},
         10: {'source': 'foo', 'data': numpy.array(['str1','nan','str2'])}}

    nan_chunk = \
        { 7: {'source': 'foo', 'data': numpy.array([1,2,3,4])},
          8: {'source': 'foo', 'data': numpy.array([-9223372036854775808,-9223372036854775808,-9223372036854775808,-9223372036854775808])},
          9: {'source': 'foo', 'data': numpy.array([[-9223372036854775808,-9223372036854775808],[-9223372036854775808,-9223372036854775808],[-9223372036854775808,-9223372036854775808],[-9223372036854775808,-9223372036854775808]])},
          10: {'source': 'foo', 'data': numpy.array(['nan','nan','nan','nan'])}}

    concat_chunk = \
        {7: {'source': 'foo', 'data': numpy.array([ 2,  4,  8,  9, 13, 19])},
         8: {'source': 'foo', 'data': numpy.array([1, 2, 3, 4, 7, 8])},
         9: {'source': 'foo', 'data': numpy.array([[1, 2], [1, 2], [1, 2], [1, 2], [1, 2], [1, 2]])},
         10: {'source': 'foo', 'data': numpy.array(['str1', 'str2', 'str3', 'str1', 'str1', 'str2'])}}

    chunk = chunks.concatenate(*cg.chunks_list)
    assert concat_chunk == chunk

    chunk_result = chunk.with_times([9,14,15], strategy='Average')
    assert avg_chunk == chunk_result

    chunk_result =  chunk.with_times([])
    assert cg.empty_chunk == chunk_result

    chunk_result = Chunk(cg.empty_chunk).with_times([1,2,3,4])
    assert nan_chunk == chunk_result

    int_chunk = \
        { 7: {'source': 'foo', 'data': numpy.array([1,3,20])},
          8: {'source': 'foo', 'data': numpy.array([1,1.5,8])},
          9: {'source': 'foo', 'data': numpy.array([[1,2],[1,2],[1,2]])},
         10: {'source': 'foo', 'data': numpy.array(['str1','str1','str2'])}}

    chunk_result = chunk.with_times([1,3,20])
    assert int_chunk == chunk_result


def test_average_generator():
    avg_chunks = [{ 7: {'source': 'foo', 'data': numpy.array([9,14])},
                    8: {'source': 'foo', 'data': numpy.array([5.5, numpy.NAN])},
                    9: {'source': 'foo', 'data': numpy.array([[1,2],[-9223372036854775808,-9223372036854775808]])},
                   10: {'source': 'foo', 'data': numpy.array(['str1','nan'])}},
                  { 7: {'source': 'foo', 'data': numpy.array([15])},
                    8: {'source': 'foo', 'data': numpy.array([8])},
                    9: {'source': 'foo', 'data': numpy.array([[1,2]])},
                   10: {'source': 'foo', 'data': numpy.array(['str2'])}}]

    ag = Average_Generator(Test_Chunk_Generator())
    i = 0
    for chunk in ag.chunks(Test_StreamRequest([9,14,15,20])):
        assert chunks.is_equal(chunk, avg_chunks[i], exact=False)
        i+=1


def test_interpolation_generator():
    int_chunks = [{ 7: {'source': 'foo', 'data': numpy.array([1,5])},
                    8: {'source': 'foo', 'data': numpy.array([1,2.25])},
                    9: {'source': 'foo', 'data': numpy.array([[1,2],[1,2]])},
                   10: {'source': 'foo', 'data': numpy.array(['str1','str2'])}},
                  { 7: {'source': 'foo', 'data': numpy.array([10,15])},
                    8: {'source': 'foo', 'data': numpy.array([4.75,(7. + (2./6.))])},
                    9: {'source': 'foo', 'data': numpy.array([[1,2],[1,2]])},
                   10: {'source': 'foo', 'data': numpy.array(['str1','str1'])}},
                  { 7: {'source': 'foo', 'data': numpy.array([20])},
                    8: {'source': 'foo', 'data': numpy.array([8])},
                    9: {'source': 'foo', 'data': numpy.array([[1,2]])},
                   10: {'source': 'foo', 'data': numpy.array(['str2'])}}]

    ag = Interpolation_Generator(Test_Chunk_Generator())
    i = 0
    for chunk in ag.chunks(Test_StreamRequest([1,5,10,15,20])):
        assert chunks.is_equal(chunk, int_chunks[i], exact=False)
        i+=1