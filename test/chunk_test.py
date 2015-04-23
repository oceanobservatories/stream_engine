__author__ = 'Stephen Zakrewsky'

import numpy
from util import chunks
from util.calc import Average_Generator, Interpolation_Generator

class Test_StreamRequest(object):

    def __init__(self, times):
        self.times = times

class Test_Chunk_Generator(object):

    def __init__(self, chunks_list=None):
        if chunks_list:
            self.chunks_list = chunks_list
        else:
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
            self.empty_chunk = {}
            for key in self.chunks_list[0]:
                self.empty_chunk[key] = {'source': self.chunks_list[0][key]['source'], 'data': self.chunks_list[0][key]['data'][:0]}

    def chunks(self, r=None):
        for chunk in self.chunks_list:
            yield chunk


def test_chunk_generator():
    cg = Test_Chunk_Generator()
    itr = cg.chunks()
    for chunk in cg.chunks_list:
        assert chunk == itr.next()


def test_is_empty():
    cg = Test_Chunk_Generator()

    assert len(cg.empty_chunk[7]['data']) == 0
    assert chunks.is_empty(cg.empty_chunk)

    assert len(cg.chunks_list[0][7]['data']) > 0
    assert not chunks.is_empty(cg.chunks_list[0])


def test_is_equals():
    chunk =  { 7: {'source': 'foo', 'data': numpy.array([2,4,8])},
                  8: {'source': 'foo', 'data': numpy.array([1,2,3])},
                  9: {'source': 'foo', 'data': numpy.array([[1,2],[1,2],[1,2]])},
                 10: {'source': 'foo', 'data': numpy.array(['str1','str2','str3'])}}

    cg = Test_Chunk_Generator()
    itr = cg.chunks()
    assert chunks.is_equal(chunk, itr.next())
    assert not chunks.is_equal(chunk, itr.next())


def test_copy():
    cg = Test_Chunk_Generator()
    chunk = chunks.copy(cg.chunks_list[0])
    assert chunks.is_equal(chunk, cg.chunks_list[0])
    assert chunk is not cg.chunks_list[0]


def test_join():
    joined_chunk = { 7: {'source': 'foo', 'data': numpy.array([2,4,8,9])},
                  8: {'source': 'foo', 'data': numpy.array([1,2,3,4])},
                  9: {'source': 'foo', 'data': numpy.array([[1,2],[1,2],[1,2],[1,2]])},
                 10: {'source': 'foo', 'data': numpy.array(['str1','str2','str3','str1'])}}

    cg = Test_Chunk_Generator()
    chunk = chunks.join(cg.chunks_list[0], cg.chunks_list[1])
    assert chunks.is_equal(chunk, joined_chunk)
    assert chunks.is_equal(chunk, chunks.join(cg.empty_chunk, chunk))
    assert chunks.is_equal(chunk, chunks.join(chunk,cg.empty_chunk))


def test_split():
    cg = Test_Chunk_Generator()
    joined_chunk = chunks.join(cg.chunks_list[0], cg.chunks_list[1])
    chunk0_length = len(cg.chunks_list[0][7]['data'])
    joined_length = len(joined_chunk[7]['data'])

    split_chunk1, split_chunk2 = chunks.split(joined_chunk, chunk0_length)
    assert chunks.is_equal(cg.chunks_list[0], split_chunk1)
    assert chunks.is_equal(cg.chunks_list[1], split_chunk2)

    split_chunk1, split_chunk2 = chunks.split(joined_chunk, 0)
    assert chunks.is_equal(cg.empty_chunk, split_chunk1)
    assert chunks.is_equal(joined_chunk, split_chunk2)

    split_chunk1, split_chunk2 = chunks.split(joined_chunk, joined_length)
    assert chunks.is_equal(joined_chunk, split_chunk1)
    assert chunks.is_equal(cg.empty_chunk, split_chunk2)


def test_average():
    cg = Test_Chunk_Generator()
    avg_chunk = { 7: {'source': 'foo', 'data': numpy.array([9,14,15])},
                  8: {'source': 'foo', 'data': numpy.array([5.5, numpy.NAN, 8])},
                  9: {'source': 'foo', 'data': numpy.array([[1,2],[-9223372036854775808,-9223372036854775808],[1,2]])},
                 10: {'source': 'foo', 'data': numpy.array(['str1','nan','str2'])}}

    # this one is weird.  Generally NAN != NAN, so can't really compare, except that
    # NAN is only valid for floats, so average is returning integers
    # as fill values for invalid data.
    nan_chunk = { 7: {'source': 'foo', 'data': numpy.array([1,2,3,4])},
                  8: {'source': 'foo', 'data': numpy.array([1,1,1,1])},
                  9: {'source': 'foo', 'data': numpy.array([[1,1],[1,1],[1,1],[1,1]])},
                 10: {'source': 'foo', 'data': numpy.array(['str1','str1','str1','str1'])}}
    for key in nan_chunk:
        if key != 7:
            nan_chunk[key]['data'][:] = numpy.NAN

    joined_chunk = chunks.join(*cg.chunks_list)
    chunk_result = chunks.average(chunks.copy(joined_chunk), [9,14,15])
    assert chunks.is_equal(avg_chunk, chunk_result)

    chunk_result =  chunks.average(chunks.copy(joined_chunk), [])
    assert chunks.is_equal(cg.empty_chunk, chunk_result)

    chunk_result = chunks.average(cg.empty_chunk, [1,2,3,4])
    print chunk_result
    print nan_chunk
    assert numpy.array_equal(chunk_result[7]['data'], [1,2,3,4])
    for key in chunk_result:
        if key != 7:
            assert numpy.array_equal(nan_chunk[key]['data'], chunk_result[key]['data'])


def test_interpolate():
    cg = Test_Chunk_Generator()
    int_chunk = { 7: {'source': 'foo', 'data': numpy.array([1,3,20])},
                  8: {'source': 'foo', 'data': numpy.array([1,1.5,8])},
                  9: {'source': 'foo', 'data': numpy.array([[1,2],[1,2],[1,2]])},
                 10: {'source': 'foo', 'data': numpy.array(['str1','str1','str2'])}}
    joined_chunk = chunks.join(*cg.chunks_list)
    chunk_result = chunks.interpolate(joined_chunk, [1,3,20])
    assert chunks.is_equal(int_chunk, chunk_result)


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