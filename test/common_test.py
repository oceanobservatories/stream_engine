import numpy as np
from util.common import isfillvalue

def test_isfillvalue():
    assert np.array_equal(isfillvalue([1, 2, 3, -999999999, 4]),
                          [False, False, False, True, False])
    assert np.array_equal(isfillvalue([1.2, np.NaN, np.NaN, 2.3, np.NaN]),
                          [False, True, True, False, True])
    assert np.array_equal(isfillvalue(['', 'abc']), [True, False])