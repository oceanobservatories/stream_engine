from collections import namedtuple

BinInfo = namedtuple('BinInfo', 'bin count first last')


class LocationMetadata(object):
    def __init__(self, bin_dict):
        # bin, count, first, last
        values = [BinInfo(*((i,) + bin_dict[i])) for i in bin_dict]
        # sort by start time
        values = sorted(values, key=lambda x: x.first)
        firsts = [b.first for b in values]
        lasts = [b.last for b in values]
        bin_list = [b.bin for b in values]
        counts = [b.count for b in values]
        if len(bin_list) > 0:
            self.total = sum(counts)
            self.start_time = min(firsts)
            self.end_time = max(lasts)
        else:
            self.total = 0
            self.start_time = 0
            self.end_time = 0
        self.bin_list = bin_list
        self.bin_information = bin_dict

    def __repr__(self):
        val = 'total: {:d} Bins -> '.format(self.total) + str(self.bin_list)
        return val

    def __str__(self):
        return self.__repr__()

    @property
    def secs(self):
        return self.end_time - self.start_time

    def particle_rate(self):
        if self.total == 0:
            return 0
        if self.secs == 0:
            return 1
        return float(self.total) / self.secs