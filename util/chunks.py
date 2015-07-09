__author__ = 'Stephen Zakrewsky'

import numpy as np
from util import common
from scipy import stats
import xray

class Chunk(object):

    def __init__(self, obj):
        if isinstance(obj, xray.Dataset):
            self.__dataset = obj
        elif isinstance(obj, Chunk):
            self.__dataset = obj.__dataset
        elif isinstance(obj, dict):
            d = {}
            for key in obj:
                if(key != 7):
                    dataArray = xray.DataArray(obj[key]['data'], coords={'dim_0': obj[7]['data']}, attrs={'source': obj[key]['source']})
                    d[key] = dataArray
            self.__dataset = xray.Dataset(d)
            self.__dataset.rename({'dim_0': 'time'}, inplace=True)
        else:
            raise TypeError('obj required to be xray.Dataset, Chunk or dict')

    def to_dataset(self):
        return self.__dataset

    def to_dict(self):
        dict_ = {}
        for key in self.__dataset:
            if key not in self.__dataset.dims.keys():
                dict_[key] = {'source': self.__dataset[key].attrs['source'], 'data': self.__dataset[key].values}
        dict_[7] = {'data': self.times()}
        return dict_

    def __repr__(self):
        return 'Chunk(%s)' % self.__dataset

    def times(self):
        return self.__dataset['time'].values

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

        return Chunk(self.__dataset.isel(time=i))

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
            for key in self.__dataset:
                if key not in self.__dataset.dims.keys():
                    dict_[key] = {'source': self.__dataset[key].attrs['source']}
                    dict_[key]['data'] = common.with_size_and_fill(self.__dataset[key].values, len(times))
            dict_[7] = {'data': np.array(times)}
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
        for key in self.__dataset:
            if key not in self.__dataset.dims.keys():
                d = self.__dataset[key].values
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
                dict_[key] = {'source': self.__dataset[key].attrs['source'], 'data': avg_data}
        dict_[7] = {'data': np.array(times)}
        return Chunk(dict_)

    def __interpolate(self, times):
        dict_ = {}
        for key in self.__dataset:
            if key not in self.__dataset.dims.keys():
                t, d = common.stretch(self.times(), self.__dataset[key].values, times)
                t, d = common.interpolate(t, d, times)
                dict_[key] = {'source': self.__dataset[key].attrs['source'], 'data': d}
        dict_[7] = {'data': np.array(times)}
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

    cd1 = Chunk(chunk1).to_dataset()
    cd2 = Chunk(chunk2).to_dataset()

    if cd1 is cd2:
        return True

    return cd1.identical(cd2)

def concatenate(*chunks):
    """
    Concatenate a list of chunks together into one chunk. The chunks must be
    consistent and in the proper sequence otherwise the returned chunk will be
    nonsense.  This concatenates chunks in the order they are input.

    :param chunks: list of chunk like objects
    :return: the joined chunk
    """
    chunks = [Chunk(c).to_dataset() for c in chunks]
    return Chunk(xray.concat(chunks, dim='time'))