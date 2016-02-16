from preload_database.model.preload import Parameter
from util.calc import StreamRequest
from util.common import StreamKey
from util.jsonresponse import JsonResponse
import json
import numpy as np
import preload_database.database
from util.datamodel import StreamData

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

    parameters = [Parameter.query.get(7),
                  Parameter.query.get(193),
                  Parameter.query.get(195),
                  # not on first stream
                  # Parameter.query.get(196),
                  Parameter.query.get(1963)]
    request = StreamRequest(stream_keys, parameters, {}, None, needs_only=True)
    # Test:
    # * two streams
    # * duplicate times on first stream
    # * arrays with object types
    # * arrays with incompatible types int and string
    # * multi-dimensional arrays
    # * parameters that don't exist
    # * parameters that are unique to one stream, not on all streams
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
        'provenance': {
            'RS00ENGC|XX00X|00-CTDBPA002|streamed|ctdbp_no_sample': {
                'data': np.array([1,1,1,1,1]),
                'source': 'source'
            },
        },
        'id': {
            'RS00ENGC|XX00X|00-CTDBPA002|streamed|ctdbp_no_sample': {
                'data': np.array([1,1,1,1,1]),
                'source': 'source'
            },
        },
        'bin': {
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
        1963: {
            'RS00ENGC|XX00X|00-CTDBPA002|streamed|ctdbp_no_sample': {
                'data': np.array([1,2,2,'foo','foo','foo','foo']),
                'source': 'derived'
            },
        }
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
        'provenance': {
            'XX00XXXX|XX00X|00-CTDPFW100|recovered|ctdpf_ckl_wfp_instrument_recovered': {
                'data': np.array([2,2]),
                'source': 'source'
            }
        },
        'id': {
            'XX00XXXX|XX00X|00-CTDPFW100|recovered|ctdpf_ckl_wfp_instrument_recovered': {
                'data': np.array([2,2]),
                'source': 'source'
            }
        },
        'bin': {
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
    pd_data = {1 : dep1, 2: dep2}
    data = StreamData(request, pd_data, None, None)

    json_str = JsonResponse(data).json({stream_keys[0]: parameters})
    json_data = json.loads(json_str)

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
        assert pk['time'] == dep1[7][stream_keys[0].as_refdes()]['data'][i]
        assert pk['deployment'] == dep1['deployment'][stream_keys[0].as_refdes()]['data'][i]

        # skip 1963 ctdpf_ckl_seawater_density, it needs a separate test
        for param in parameters[:-1]:
            assert np.array_equal(particle[param.name], dep1[param.id][stream_keys[0].as_refdes()]['data'][i])

        # 1963 ctdpf_ckl_seawater_density is all fill values -9999999.0 because of incompatible types
        assert np.array_equal(particle['ctdpf_ckl_seawater_density'], -9999999.0)

        assert particle['provenance'] == str(dep1['provenance'][stream_keys[0].as_refdes()]['data'][i])
