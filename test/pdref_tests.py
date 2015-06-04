from parameter_util import PDRef

def test_PDRef_str_returns_str():
    pdref = PDRef(None, 1234)
    try:
        assert type(str(pdref)) == str
    except TypeError:
        assert False
