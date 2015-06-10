__author__ = 'Stephen Zakrewsky'

import numpy as np
from util import common
from scipy import stats

class Chunk(object):

    def __init__(self, obj):
        if isinstance(obj, Chunk):
            self.__dict = obj.to_dict()
        elif isinstance(obj, dict):
            self.__dict = obj
        else:
            raise TypeError('obj required to be a Chunk or dict')

    def to_dict(self):
        return self.__dict

    def __repr__(self):
        return 'Chunk(%s)' % self.to_dict()

    def times(self):
        return self.__dict[7]['data']

    def __len__(self):
        return len(self.times())

    def __getitem__(self, i):
        """
        Slice a chunk.  The underlying numpy data in the returned chunk are
        views into their respective data in this chunk.

        :param i: scalar or slice
        :return: Chunk
        """
        if isinstance(i,int):
            if i < 0:
                i += len(self)
            i = slice(i,i+1)

        dict_ = {}
        for key in self.__dict:
            dict_[key] = {'source': self.__dict[key]['source']}
            dict_[key]['data'] = self.__dict[key]['data'][i]

        return Chunk(dict_)

    def __eq__(self, other):
        return is_equal(self, other, exact=True)

    def __ne__(self, other):
        return not is_equal(self, other, exact=True)

    def with_times(self, times, strategy='Interpolate'):
        """
        Return a new chunk with the times using the given strategy.
        :param times: strictly increasing list of ntp times
        :param strategy: optional

                'Interpolate' default
                    Interpolates the chunk to the times.

                'Average'
                    Averages the chunk into the times bins.  Times must be
                    strictly increasing.  The bins are (inclusive,exclusive)
                    until the last bin which includes everything after.

                    If the data cannot be averaged because of its dtype, the
                    first one in the bin is used.  Empty empty bins use a fill
                    value.

        :return: Chunk
        """
        if any([not self, len(times) == 0]):
            dict_ = {}
            for key in self.__dict:
                if key != 7:
                    dict_[key] = {'source': self.__dict[key]['source']}
                    dict_[key]['data'] = common.with_size_and_fill(self.__dict[key]['data'], len(times))
            dict_[7] = {'source': self.__dict[7]['source'], 'data': np.array(times)}
            return Chunk(dict_)

        if strategy == 'Average':
            return self.__average(times)
        else:
            return self.__interpolate(times)

    def __average(self, times):
        # Time bins are (inclusive,exclusive) until the last bin which includes
        # everything after.
        bins = np.digitize(self.times(), times) - 1
        # expand times parameter to include trailing data because
        # binned_statistic uses the last time as the upper bound, not as the
        # lower bound of the last bin.  Add 1 because it doesn't like bin
        # boundaries to be equal.
        b = times + [max(self.times()[-1], times[-1]) + 1]
        # force range to be within the times parameter because by default
        # binned_statistic puts preceding and trailing data in the first and
        # last bin respectively.
        r = [(times[0], b[-1])]

        dict_ = {}
        for key in self.__dict:
            if key != 7:
                d = self.__dict[key]['data']
                try:
                    avg_data = stats.binned_statistic(self.times(), d, 'mean', bins=b, range=r)[0]
                except:
                    # Data cannot be averaged because of its dtype.  Use the
                    # first one in the bin.  Use fill value for empty bins.
                    avg_data = common.with_size_and_fill(d, len(times))
                    for i in range(len(times)):
                        bin_data = d[bins == i]
                        if len(bin_data) > 0:
                            avg_data[i] = bin_data[0]
                dict_[key] = {'source': self.__dict[key]['source'], 'data': avg_data}
        dict_[7] = {'source': self.__dict[7]['source'], 'data': np.array(times)}
        return Chunk(dict_)

    def __interpolate(self, times):
        dict_ = {}
        for key in self.__dict:
            if key != 7:
                t, d = common.stretch(self.times(), self.__dict[key]['data'], times)
                t, d = common.interpolate(t, d, times)
                dict_[key] = {'source': self.__dict[key]['source'], 'data': d}
        dict_[7] = {'source': self.__dict[7]['source'], 'data': np.array(times)}
        return Chunk(dict_)


def is_equal(chunk1, chunk2, exact=True):
    """
    Tests whether two chunks are logically equal.  Supports NaN by making sure
    they occur in the same locations.  If exact is False (default is True), this
    will test float values with some degree of tolerance.

    :param chunk1: chunk like
    :param chunk2: chunk like
    :param exact: optional Boolean
    :return: Boolean
    """
    if chunk1 is chunk2:
        return True

    cd1 = Chunk(chunk1).to_dict()
    cd2 = Chunk(chunk2).to_dict()

    if cd1 is cd2:
        return True

    if set(cd1) != set(cd2):
        return False

    for key in cd1:
        if cd1[key]['source'] != cd2[key]['source']:
            return False

        d1 = cd1[key]['data']
        d2 = cd2[key]['data']
        if d1 is not d2:
            try:
                # Check NAN -- this only works on numbers
                m1 = np.logical_not(np.isnan(d1))
                m2 = np.logical_not(np.isnan(d2))
                if not np.array_equal(m1,m2):
                    return False
                d1 = d1[m1]
                d2 = d2[m2]
            except TypeError:
                pass

            if exact:
                if d1.dtype != d2.dtype and 'object' in (d1.dtype, d2.dtype) and d1.shape != d2.shape:
                    return d1.tolist() == d2.tolist()
                elif not np.array_equal(d1, d2):
                    return False
            else:
                try:
                    # this only works on numbers
                    if not np.allclose(d1, d2):
                        return False
                except TypeError:
                    if d1.dtype != d2.dtype and 'object' in (d1.dtype, d2.dtype) and d1.shape != d2.shape:
                        return d1.tolist() == d2.tolist()
                    elif not np.array_equal(d1, d2):
                        return False

    return True


def concatenate(*chunks):
    """
    Concatenate a list of chunks together into one chunk. The chunks must be
    consistent and in the proper sequence otherwise the returned chunk will be
    nonsense.  This concatenates chunks in the order they are input.

    :param chunks: list of chunk like objects
    :return: the joined chunk
    """
    chunks = [Chunk(c).to_dict() for c in chunks]
    dict_ = {}
    cd1 = chunks[0]
    for key in cd1:
        dict_[key] = {'source': cd1[key]['source']}
        try:
            dict_[key]['data'] = np.concatenate([cd[key]['data'] for cd in chunks])
        except ValueError:
            dict_[key]['data'] = np.array([d for cd in chunks for d in cd[key]['data'].tolist()])
    return Chunk(dict_)