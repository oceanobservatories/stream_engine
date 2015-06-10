from util.calc import NetCDF_Generator
import numpy as np
import numpy.ma as ma
import os
import preload_database.database
import netCDF4

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

    request = to_obj({'stream_keys': [
        {
            'subsite': 'RS00ENGC',
            'node': 'XX00X',
            'sensor': '00-CTDBPA002',
            'method': 'streamed',
            'stream': {'name': 'ctdbp_no_sample'}
         }
    ]})

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

    ncfile = open('tmp.nc', 'w')
    ncfile.write(file_output)
    ncfile.close()

    ncfile = netCDF4.Dataset('tmp.nc', 'r')
    assert len(ncfile.groups) == 2
    assert 'source' in ncfile.groups
    assert 'derived' in ncfile.groups

    source_group = ncfile.groups['source']
    derived_group = ncfile.groups['derived']
    for i in (source_group, derived_group):
        assert len(i.groups) == 2
        assert '1' in i.groups
        assert '2' in i.groups

    source_group_1 = source_group.groups['1']
    source_group_2 = source_group.groups['2']
    derived_group_1 = derived_group.groups['1']
    derived_group_2 = derived_group.groups['2']

    assert ma.allequal(source_group_1.variables['deployment'], [1,1,1,1])
    assert ma.allequal(source_group_1.variables['notaparm'], [[1,2],[1,2],[1,2],[1,2]])
    assert ma.allequal(source_group_1.variables['temperature'], ma.array([[1,2],[1,2],[1,2],[1,2]], mask=[0,0,0,0,1,1,1,1]))
    assert ma.allequal(source_group_1.variables['pressure'], ma.array([1,2,1,2], mask=[1,1,0,0]))

    assert ma.allequal(derived_group_1.variables['ctdpf_ckl_seawater_density'], ma.array([1,2,1,2], mask=[0,0,1,1]))

    assert ma.allequal(source_group_2.variables['deployment'], [2,2])
    assert ma.allequal(source_group_2.variables['notaparm'], [[1,2,3],[1,2,3]])
    assert ma.allequal(source_group_2.variables['temperature'], [[1,2,3],[1,2,3]])
    assert ma.allequal(source_group_2.variables['pressure'], [1,2])

    assert ma.allequal(derived_group_2.variables['ctdpf_ckl_seawater_density'], ['foo', 'foo'])

    os.remove('tmp.nc')