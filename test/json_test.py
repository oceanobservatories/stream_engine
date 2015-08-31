from util.common import CachedParameter, StreamKey
from util.jsonresponse import JsonResponse
import json
import numpy as np
import preload_database.database

preload_database.database.initialize_connection(preload_database.database.PreloadDatabaseMode.POPULATED_FILE)
preload_database.database.open_connection()

def test_JsonResponse():

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

    parameters = [CachedParameter.from_id(7),
                  CachedParameter.from_id(193),
                  CachedParameter.from_id(195),
                  # not on first stream
                  # CachedParameter.from_id(196),
                  CachedParameter.from_id(1963)]

    # Test:
    # * two streams
    # * duplicate times on first stream
    # * arrays with object types
    # * arrays with incompatible types int and string
    # * multi-dimensional arrays
    # * parameters that don't exist
    # * parameters that are unique to one stream, not on all streams
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
        'provenance': {
            'RS00ENGC|XX00X|00-CTDBPA002|streamed|ctdbp_no_sample': {
                'data': np.array([1,1,1,1,1]),
                'source': 'source'
            },
            'XX00XXXX|XX00X|00-CTDPFW100|recovered|ctdpf_ckl_wfp_instrument_recovered': {
                'data': np.array([2,2]),
                'source': 'source'
            }
        },
        'id': {
            'RS00ENGC|XX00X|00-CTDBPA002|streamed|ctdbp_no_sample': {
                'data': np.array([1,1,1,1,1]),
                'source': 'source'
            },
            'XX00XXXX|XX00X|00-CTDPFW100|recovered|ctdpf_ckl_wfp_instrument_recovered': {
                'data': np.array([2,2]),
                'source': 'source'
            }
        },
        'bin': {
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
        json_str = JsonResponse(data, {}).json(stream_keys[0], parameters)
        json_data = json.loads(json_str)
    except Exception as e:
        raise AssertionError(e)

    assert isinstance(json_data, list)
    assert len(json_data) == 4 # duplicate removed

    for i, particle in enumerate(json_data):
        assert len(particle) == len(parameters) + 2

        # shift to ignore duplicate
        if i > 1:
            i += 1

        pk = particle['pk']
        assert pk['subsite'] == 'RS00ENGC'
        assert pk['node'] == 'XX00X'
        assert pk['sensor'] == '00-CTDBPA002'
        assert pk['method'] == 'streamed'
        assert pk['stream'] == 'ctdbp_no_sample'
        assert pk['time'] == data[7][stream_keys[0].as_refdes()]['data'][i]
        assert pk['deployment'] == data['deployment'][stream_keys[0].as_refdes()]['data'][i]

        # skip 1963 ctdpf_ckl_seawater_density, it needs a separate test
        for param in parameters[:-1]:
            assert np.array_equal(particle[param.name], data[param.id][stream_keys[0].as_refdes()]['data'][i])

        # 1963 ctdpf_ckl_seawater_density is all fill values -9999999.0 because of incompatible types
        assert np.array_equal(particle['ctdpf_ckl_seawater_density'], -9999999.0)

        assert particle['provenance'] == str(data['provenance'][stream_keys[0].as_refdes()]['data'][i])
