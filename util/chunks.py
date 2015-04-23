__author__ = 'Stephen Zakrewsky'

import numpy
from util import common
from scipy import stats

def split(chunk, i):
    """
    Split a chunk into two chunks at index i, returning both chunks as a tuple.
    If i is 0 or is greater than or equal to the length, one of the returned chunks
    will be equal to the input chunk and the other will be the empty chunk
    respectively.

    Note: the underlying numpy data in the returned chunks are views into the
    respective data in the input chunk.

    :param chunk:
    :param i: scalar
    :return: (chunk before i, chunk at i)
    """
    chunk1 = {}
    chunk2 = {}
    for key in chunk:
        chunk1[key] = {}
        chunk2[key] = {}
        chunk1[key]['source'] = chunk[key]['source']
        chunk2[key]['source'] = chunk[key]['source']
        chunk1[key]['data'] = chunk[key]['data'][:i]
        chunk2[key]['data'] = chunk[key]['data'][i:]
    return chunk1, chunk2


def join(*chunks):
    """
    Join a list of chunks together into one chunk. The chunks must be consistent and
    in the proper sequence otherwise the returned chunk will be nonsense.  This
    joins chunks in the order they are input.

    :param chunks: list of chunks
    :return: the joined chunk
    """
    chunk = {}
    for key in chunks[0]:
        chunk[key] = {'source': chunks[0][key]['source']}
        chunk[key]['data'] = numpy.concatenate([c[key]['data'] for c in chunks])
    return chunk


def is_equal(chunk1, chunk2, exact=True):
    """
    Tests whether two chunks are logically equal.

    :param chunk1:
    :param chunk2:
    :return: True or False
    """
    if chunk1 is chunk2:
        return True

    if set(chunk1) != set(chunk2):
        return False

    for key in chunk1:
        if chunk1[key]['source'] != chunk2[key]['source']:
            return False
        if chunk1[key]['data'] is not chunk2[key]['data']:
            try:
                # Check NAN -- this only works on numbers
                m1 = numpy.logical_not(numpy.isnan(chunk1[key]['data']))
                m2 = numpy.logical_not(numpy.isnan(chunk2[key]['data']))
                if not numpy.array_equal(m1,m2):
                    return False
            except TypeError:
                m1 = numpy.ones(chunk1[key]['data'].shape, dtype=bool)
                m2 = numpy.ones(chunk2[key]['data'].shape, dtype=bool)

            if exact:
                if not numpy.array_equal(chunk1[key]['data'][m1], chunk2[key]['data'][m2]):
                    return False
            else:
                try:
                    # this only works on numbers
                    if not numpy.allclose(chunk1[key]['data'][m1], chunk2[key]['data'][m2]):
                        return False
                except TypeError:
                    if not numpy.array_equal(chunk1[key]['data'][m1], chunk2[key]['data'][m2]):
                        return False

    return True


def is_empty(chunk):
    """
    Test whether the chunk is the empty chunk

    :param chunk:
    :return:
    """
    return len(chunk[7]['data']) == 0


def copy(chunk):
    """
    Deep copy a chunk.

    :param chunk:
    :return:
    """
    copied_chunk = {}
    for key in chunk:
        copied_chunk[key] = {'source': chunk[key]['source']}
        copied_chunk[key]['data'] = numpy.copy(chunk[key]['data'])
    return copied_chunk


def average(chunk, times):
    """
    Average the chunk into the times bins.  Times must be monotonically increasing.
    The bins are (inclusive,exclusive) until the last bin which includes everything
    after.

    If chunk is empty, it is returned expanded to the times using fill values.  If
    times is empty, the chunk is returned as the empty chunk.

    If the data cannot be averaged because of its dtype, the first one in the bin
    is used.  Empty empty bins use a fill value.

    Note: the input chunk is modified and returned.

    :param chunk:
    :param times: list
    :return:
    """
    if any([is_empty(chunk), not times]):
        chunk[7]['data'] = numpy.array(times)
        for key in chunk:
            if key != 7:
                chunk[key]['data'] = common.with_size_and_fill(chunk[key]['data'], len(times))
        return chunk

    bins = numpy.digitize(chunk[7]['data'], times) - 1

    for key in chunk:
        if key != 7:
            try:
                # expand times parameter to include trailing data because
                # binned_statistic uses the last time inclusively.  Add 1
                # because it doesn't like bin boundaries to be equal.
                b = times + [max(chunk[7]['data'][-1], times[-1]) + 1]
                # force range to be within the times parameter because by default
                # binned_statistic puts preceding and trailing data in the
                # first and last bin respectively.
                r = [(times[0], b[-1])]
                avg_data = stats.binned_statistic(chunk[7]['data'], chunk[key]['data'], 'mean', bins=b, range=r)[0]
            except:
                avg_data = common.with_size_and_fill(chunk[key]['data'], len(times))
                for i in range(len(times)):
                    bin_data = chunk[key]['data'][bins == i]
                    if len(bin_data) > 0:
                        avg_data[i] = bin_data[0]

            chunk[key]['data'] = avg_data
    chunk[7]['data'] = numpy.array(times)
    return chunk


def interpolate(chunk, times):
    """
    Interpolate the chunk into the time bins.  Both chunk and times must be
    non-empty.

    Note: the input chunk is modified and returned.

    :param chunk:
    :param times: list
    :return:
    """
    for key in chunk:
        if key != 7:
            t, d = common.stretch(numpy.copy(chunk[7]['data']), chunk[key]['data'], times)
            t, d = common.interpolate(t, d, times)
            chunk[key]['data'] = d
    chunk[7]['data'] = numpy.array(times)
    return chunk