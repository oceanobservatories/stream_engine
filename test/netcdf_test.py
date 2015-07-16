from util.calc import NetCDF_Generator
import numpy as np
import numpy.ma as ma
import os
import preload_database.database
import netCDF4
import zipfile

preload_database.database.initialize_connection(preload_database.database.PreloadDatabaseMode.POPULATED_FILE)
preload_database.database.open_connection()

def to_obj(d):
    if type(d) in (list, dict):
        for i in d if type(d) == dict else range(len(d)):
            d[i] = to_obj(d[i]);
    if(type(d) == dict):
        return dict_to_obj(d)
    else:
        return d


class dict_to_obj(object):
    def __init__(self, d):
        self.__dict__ = d


class Generator(object):

    def __init__(self, chunk_data):
        self.chunk_data = chunk_data

    def chunks(self, r):
        for c in self.chunk_data:
            yield c


def test_NetCDF_Generator():

    request = to_obj({
        'stream_keys': [{
                'subsite': 'RS00ENGC',
                'node': 'XX00X',
                'sensor': '00-CTDBPA002',
                'method': 'streamed',
                'stream': {'name': 'ctdbp_no_sample'}
            }],
        'include_provenance': False
    })

    chunks = [{
        7: {
            'data': np.array([1,2,2]),
            'source': 'source'
        },
        'deployment': {
            'data': np.array([1,1,1]),
            'source': 'source'
        },
        'notaparm': {
            'data': np.array([[1,2],[1,2],[1,2]]),
            'source': 'source'
        },
        193: {
            'data': np.array([[1,2],[1,2],[1,2]]),
            'source': 'source'
        },
        195: {
            'data': np.array([1,2,2], dtype='object'),
            'source': 'source'
        },
        1963: {
            'data': np.array([1,2,2]),
            'source': 'derived'
        }
    },{
        7: {
            'data': np.array([3,4]),
            'source': 'source'
        },
        'deployment': {
            'data': np.array([1,1]),
            'source': 'source'
        },
        'notaparm': {
            'data': np.array([[1,2],[1,2]]),
            'source': 'source'
        },
        193: {
            'data': np.array([[1,2,3],[1,2,3]]),
            'source': 'source'
        },
        195: {
            'data': np.array([1,2]),
            'source': 'source'
        },
        1963: {
            'data': np.array(['foo','foo']),
            'source': 'derived'
        }
    },{
        7: {
            'data': np.array([5,6]),
            'source': 'source'
        },
        'deployment': {
            'data': np.array([2,2]),
            'source': 'source'
        },
        'notaparm': {
            'data': np.array([[1,2,3],[1,2,3]]),
            'source': 'source'
        },
        193: {
            'data': np.array([[1,2,3],[1,2,3]]),
            'source': 'source'
        },
        195: {
            'data': np.array([1,2]),
            'source': 'source'
        },
        1963: {
            'data': np.array(['foo','foo']),
            'source': 'derived'
        }
    }]

    try:
        file_output = NetCDF_Generator(Generator(chunks)).chunks(request)
    except Exception as e:
        raise AssertionError(e)

    ncfile = open('tmp.zip', 'w')
    ncfile.write(file_output)
    ncfile.close()

    zf = zipfile.ZipFile('tmp.zip', 'r')
    namelist = zf.namelist()
    assert len(namelist) == 4
    assert 'source_1' in namelist
    assert 'derived_1' in namelist
    assert 'source_2' in namelist
    assert 'derived_2' in namelist

    zf.extractall()
    source_group_1 = netCDF4.Dataset('source_1', 'r')
    derived_group_1 = netCDF4.Dataset('derived_1', 'r')
    source_group_2 = netCDF4.Dataset('source_2', 'r')
    derived_group_2 = netCDF4.Dataset('derived_2', 'r')

    assert ma.allequal(source_group_1.variables['deployment'], [1,1,1,1])
    assert ma.allequal(source_group_1.variables['notaparm'], [[1,2],[1,2],[1,2],[1,2]])
    np.testing.assert_array_equal(source_group_1.variables['temperature'], [[1,2],[1,2],[np.nan,np.nan],[np.nan,np.nan]])
    assert ma.allequal(source_group_1.variables['pressure'], [1,2,1,2])

    np.testing.assert_array_equal(derived_group_1.variables['ctdpf_ckl_seawater_density'], [1,2,np.nan,np.nan])

    assert ma.allequal(source_group_2.variables['deployment'], [2,2])
    assert ma.allequal(source_group_2.variables['notaparm'], [[1,2,3],[1,2,3]])
    assert ma.allequal(source_group_2.variables['temperature'], [[1,2,3],[1,2,3]])
    assert ma.allequal(source_group_2.variables['pressure'], [1,2])

    np.testing.assert_array_equal(derived_group_2.variables['ctdpf_ckl_seawater_density'], [['f','o','o'],['f','o','o']])

    os.remove('source_1')
    os.remove('derived_1')
    os.remove('source_2')
    os.remove('derived_2')
    os.remove('tmp.zip')