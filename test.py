#!/usr/bin/env python
import json
import sys
import time
import requests


d5 = {
    "streams": [
        {
            "subsite": "RS00ENGC",
            "node": "XX00X",
            "sensor": "00-CTDBPA002",
            "method": "streamed",
            "stream": "ctdbp_no_sample",
        }
    ],
    "coefficients": {
        "CC_lat": [{'value': 1.0}],
        "CC_lon": [{'value': 1.0}]
    },
    "start": 3634041542.2546076775,
#    "stop": 3634126116.3071446419,
    "stop": 3634041695.3398842812,
#    "limit": 2000
}

r = json.dumps(d5)

headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
def make_request():
    return requests.post('http://localhost:5000/particles', data=r, headers=headers).content


def timeit(func):
    s = time.time()
    data = func()
    return time.time() - s, data

results = None
for i in xrange(100):
    elapsed, data = timeit(make_request)
    if i == 0:
        results = data
    print elapsed, results == data
