from util.calc import NetCDF_Generator, StreamRequest
from util.common import StreamKey
import numpy as np
import numpy.ma as ma
import os
import preload_database.database
import netCDF4
import zipfile

preload_database.database.initialize_connection(preload_database.database.PreloadDatabaseMode.POPULATED_FILE)
preload_database.database.open_connection()

def test_NetCDF_Generator():

    stream_keys = [StreamKey.from_dict({
        'subsite': 'RS00ENGC',
        'node': 'XX00X',
        'sensor': '00-CTDBPA002',
        'method': 'streamed',
        'stream': 'ctdbp_no_sample'
    }), StreamKey.from_dict({
        'subsite': 'XX00XXXX',
        'node': 'XX00X',
        'sensor': '00-CTDPFW100',
        'method': 'recovered',
        'stream': 'ctdpf_ckl_wfp_instrument_recovered'
    }) ]

    request = StreamRequest(stream_keys, None, {}, None, needs_only=True)

    data = {
        7: {
            'RS00ENGC|XX00X|00-CTDBPA002|streamed|ctdbp_no_sample': {
                'data': np.array([1,2,2,3,4]),
                'source': 'source'
            },
            'XX00XXXX|XX00X|00-CTDPFW100|recovered|ctdpf_ckl_wfp_instrument_recovered': {
                'data': np.array([2,3]),
                'source': 'source'
            }
        },
        'deployment': {
            'RS00ENGC|XX00X|00-CTDBPA002|streamed|ctdbp_no_sample': {
                'data': np.array([1,1,1,1,1]),
                'source': 'source'
            },
            'XX00XXXX|XX00X|00-CTDPFW100|recovered|ctdpf_ckl_wfp_instrument_recovered': {
                'data': np.array([2,2]),
                'source': 'source'
            }
        },
        'notaparm': {
            'RS00ENGC|XX00X|00-CTDBPA002|streamed|ctdbp_no_sample': {
                'data': np.array([[1,2],[1,2],[1,2],[1,2],[1,2]]),
                'source': 'source'
            }
        },
        193: {
            'RS00ENGC|XX00X|00-CTDBPA002|streamed|ctdbp_no_sample': {
                'data': np.array([[1,2],[1,2],[1,2],[1,2],[1,2]]),
                'source': 'source'
            }
        },
        195: {
            'RS00ENGC|XX00X|00-CTDBPA002|streamed|ctdbp_no_sample': {
                'data': np.array([1,2,2,1,2], dtype='object'),
                'source': 'source'
            }
        },
        196: {
            'XX00XXXX|XX00X|00-CTDPFW100|recovered|ctdpf_ckl_wfp_instrument_recovered': {
                'data': np.array([2,3]),
                'source': 'source'
            }
        },
        1963: {
            'RS00ENGC|XX00X|00-CTDBPA002|streamed|ctdbp_no_sample': {
                'data': np.array([1,2,2,'foo','foo','foo','foo']),
                'source': 'derived'
            },
            'XX00XXXX|XX00X|00-CTDPFW100|recovered|ctdpf_ckl_wfp_instrument_recovered': {
                'data': np.array([2,3]),
                'source': 'derived'
            }
        }
    }

    try:
        file_output = NetCDF_Generator(data, None).chunks(request)
    except Exception as e:
        raise AssertionError(e)

    ncfile = open('tmp.zip', 'w')
    ncfile.write(file_output)
    ncfile.close()

    zf = zipfile.ZipFile('tmp.zip', 'r')
    namelist = zf.namelist()
    assert len(namelist) == 2
    assert 'RS00ENGC|XX00X|00-CTDBPA002|streamed|ctdbp_no_sample.nc' in namelist
    assert 'XX00XXXX|XX00X|00-CTDPFW100|recovered|ctdpf_ckl_wfp_instrument_recovered.nc' in namelist

    zf.extractall()
    group_1 = netCDF4.Dataset('RS00ENGC|XX00X|00-CTDBPA002|streamed|ctdbp_no_sample.nc', 'r')

    assert group_1.__dict__['subsite'] == 'RS00ENGC'
    assert group_1.__dict__['node'] == 'XX00X'
    assert group_1.__dict__['sensor'] == '00-CTDBPA002'
    assert group_1.__dict__['collection_method'] == 'streamed'
    assert group_1.__dict__['stream'] == 'ctdbp_no_sample'

    assert len(group_1.variables) == 6
    assert np.array_equal(group_1.variables['time'], [1,2,3,4])
    assert np.array_equal(group_1.variables['deployment'], [1,1,1,1])
    assert np.array_equal(group_1.variables['notaparm'], [[1,2],[1,2],[1,2],[1,2]])
    assert np.array_equal(group_1.variables['temperature'], [[1,2],[1,2],[1,2],[1,2]])
    assert np.array_equal(group_1.variables['pressure'], [1,2,1,2])
    underlying_array = group_1.variables['ctdpf_ckl_seawater_density'][:]
    assert type(underlying_array) is ma.MaskedArray
    assert np.all(underlying_array.mask)

    os.remove('RS00ENGC|XX00X|00-CTDBPA002|streamed|ctdbp_no_sample.nc')

    group_2 = netCDF4.Dataset('XX00XXXX|XX00X|00-CTDPFW100|recovered|ctdpf_ckl_wfp_instrument_recovered.nc', 'r')

    assert group_2.__dict__['subsite'] == 'XX00XXXX'
    assert group_2.__dict__['node'] == 'XX00X'
    assert group_2.__dict__['sensor'] == '00-CTDPFW100'
    assert group_2.__dict__['collection_method'] == 'recovered'
    assert group_2.__dict__['stream'] == 'ctdpf_ckl_wfp_instrument_recovered'

    assert len(group_2.variables) == 4
    assert np.array_equal(group_2.variables['time'], [2,3])
    assert np.array_equal(group_2.variables['deployment'], [2,2])
    assert np.array_equal(group_2.variables['pressure_temp'], [2,3])
    assert np.array_equal(group_2.variables['ctdpf_ckl_seawater_density'], [2,3])

    os.remove('XX00XXXX|XX00X|00-CTDPFW100|recovered|ctdpf_ckl_wfp_instrument_recovered.nc')
    os.remove('tmp.zip')