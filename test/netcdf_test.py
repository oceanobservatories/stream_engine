from preload_database.model.preload import Parameter
from util.calc import NetCDF_Generator, StreamRequest
from util.common import StreamKey
import numpy as np
import os
import preload_database.database
import netCDF4
import zipfile
import traceback
from util.datamodel import StreamData

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

    parameters = [Parameter.query.get(7),
                  Parameter.query.get(193),
                  Parameter.query.get(195),
                  Parameter.query.get(196),
                  Parameter.query.get(1963)]

    request = StreamRequest(stream_keys, parameters, {}, None, needs_only=True)

    dep1 = {
        7: {
            'RS00ENGC|XX00X|00-CTDBPA002|streamed|ctdbp_no_sample': {
                'data': np.array([1,2,2,3,4]),
                'source': 'source'
            },
        },
        'deployment': {
            'RS00ENGC|XX00X|00-CTDBPA002|streamed|ctdbp_no_sample': {
                'data': np.array([1,1,1,1,1]),
                'source': 'source'
            },
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
        # This will never happen.  When the pd_data is created the values will be cast to correct values
        # 1963: {
        #     'RS00ENGC|XX00X|00-CTDBPA002|streamed|ctdbp_no_sample': {
        #         'data': np.array([1,2,2,'foo','foo','foo','foo']),
        #         'source': 'derived'
        #     },
        # }
    }
    dep2 = {
        7: {
            'XX00XXXX|XX00X|00-CTDPFW100|recovered|ctdpf_ckl_wfp_instrument_recovered': {
                'data': np.array([2,3]),
                'source': 'source'
            }
        },
        'deployment': {
            'XX00XXXX|XX00X|00-CTDPFW100|recovered|ctdpf_ckl_wfp_instrument_recovered': {
                'data': np.array([2,2]),
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
            'XX00XXXX|XX00X|00-CTDPFW100|recovered|ctdpf_ckl_wfp_instrument_recovered': {
                'data': np.array([2,3]),
                'source': 'derived'
            }
        }
    }

    d = {1: dep1, 2 : dep2}
    sd = StreamData(request, d, None, None)
    try:
        file_output = NetCDF_Generator(sd).chunks()
    except Exception as e:
        traceback.print_exc()
        raise AssertionError(e)

    ncfile = open('tmp.zip', 'w')
    ncfile.write(file_output)
    ncfile.close()

    zf = zipfile.ZipFile('tmp.zip', 'r')
    namelist = zf.namelist()
    assert len(namelist) == 2
    assert 'deployment0001_RS00ENGC-XX00X-00-CTDBPA002-streamed-ctdbp_no_sample.nc' in namelist
    assert 'deployment0002_XX00XXXX-XX00X-00-CTDPFW100-recovered-ctdpf_ckl_wfp_instrument_recovered.nc' in namelist

    zf.extractall()
    group_1 = netCDF4.Dataset('deployment0001_RS00ENGC-XX00X-00-CTDBPA002-streamed-ctdbp_no_sample.nc', 'r')

    assert group_1.__dict__['subsite'] == 'RS00ENGC'
    assert group_1.__dict__['node'] == 'XX00X'
    assert group_1.__dict__['sensor'] == '00-CTDBPA002'
    assert group_1.__dict__['collection_method'] == 'streamed'
    assert group_1.__dict__['stream'] == 'ctdbp_no_sample'

    assert len(group_1.variables) == 5
    assert np.array_equal(group_1.variables['time'], [1,2,3,4])
    assert np.array_equal(group_1.variables['deployment'], [1,1,1,1])
    assert np.array_equal(group_1.variables['notaparm'], [[1,2],[1,2],[1,2],[1,2]])
    assert np.array_equal(group_1.variables['temperature'], [[1,2],[1,2],[1,2],[1,2]])
    assert np.array_equal(group_1.variables['pressure'], [1,2,1,2])

    os.remove('deployment0001_RS00ENGC-XX00X-00-CTDBPA002-streamed-ctdbp_no_sample.nc')

    group_2 = netCDF4.Dataset('deployment0002_XX00XXXX-XX00X-00-CTDPFW100-recovered-ctdpf_ckl_wfp_instrument_recovered.nc', 'r')

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

    os.remove('deployment0002_XX00XXXX-XX00X-00-CTDPFW100-recovered-ctdpf_ckl_wfp_instrument_recovered.nc')
    os.remove('tmp.zip')
