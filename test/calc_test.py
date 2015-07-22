import numpy as np
from util.calc import interpolate_list

def test_interpolate_list():
    from_times = [0, 3, 4]
    from_values = [1, -999999999, 3]
    to_times = [1, 2, 3, 4, 5]
    to_values = [1.5, 2, 2.5, 3, 3]
    assert np.array_equal(interpolate_list(to_times, from_times, from_values),
                          to_values)