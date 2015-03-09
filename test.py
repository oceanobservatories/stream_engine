#!/usr/bin/env python
import msgpack
import base64
import json
import requests

d = {
    "streams": [
        {
            "subsite": "TEST",
            "node": "TEST",
            "sensor": "TEST",
            "method": "streamed",
            "stream": "ctdbp_no_sample",
            "parameters": [3651]
        }
    ],
    "coefficients": {
        "CC_a0": 1.0,
        "CC_a1": 1.0,
        "CC_a2": 1.0,
        "CC_a3": 1.0,
        "CC_lat": 1.0,
        "CC_lon": 1.0
    },
#    "start": 3634041530.2004886,
#    "stop": 3634043605.3394685
    "start": 1,
    "stop": 3634043605
}

d = { 
    "streams": [ 
        { 
            "subsite":"XX00XXXX", 
            "node":"XX00X", 
            "sensor":"00-CTDPFW100", 
            "method":"recovered", 
            "stream":"ctdpf_ckl_wfp_instrument_recovered", 
            "parameters": [ 1959 ] 
        } 
    ] 
} 

headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
data = requests.post('http://localhost:5000/calculate', data=json.dumps(d), headers=headers).json()

for id in data:
	print '%5s %35s %s' % (id, data[id]['name'], msgpack.unpackb(base64.b64decode(data[id]['data']))[:5])
