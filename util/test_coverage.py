#!/usr/bin/env python2

"""
Queries all streams in cassandra for json

Usage: ./test_coverage.py [limit|limit url]

Examples:
    ./test_coverage.py 1000
    ./test_coverage.py
    ./test_coverage.py 1000 http://localhost:12576
"""

import time

import requests
from bs4 import BeautifulSoup

import sys

sensor_inventory_url = "http://localhost:12576"

def print_good(s):
    print '\033[92m' + str(s) + '\033[0m'

def print_bad(s):
    print '\033[91m' + str(s) + '\033[0m'

limit = '50'
if len(sys.argv) > 1:
    limit = sys.argv[1]

if len(sys.argv) > 2:
    sensor_inventory_url = sys.argv[2]

r = requests.get(sensor_inventory_url+"/sensor/allstreams?limit="+limit, proxies={'no':'pass'})
soup = BeautifulSoup(r.text)

urls = []
num_streams = 0
for a in soup.find_all('a'):
    if a.text == '1' and 'VALIDATE' not in a.attrs['href']:
        num_streams += 1
        url = sensor_inventory_url+a.attrs['href']
        urls.append(url)

total_particles = 0
start_time = time.time()
num_errors = 0
for url in urls:
    stime = time.time()
    print "URL: ", url

    stream_page = requests.get(url, proxies={'no': 'pass'})

    if stream_page.status_code != 200:
        print_bad("Code: {}".format(stream_page.status_code))
        num_errors += 1
    else:
        try:
            len_output = len(stream_page.json())
        except Exception as e:
            print_bad("Code: {}".format(stream_page.status_code))
            print "Exception: {}".format(e)
            num_errors += 1
            continue
            
        print_good("{} particles".format(len_output))
        total_particles += len_output
    print "Request took {}s".format(time.time()-stime)
    print ""

message = "\n\n\nNum errors: {}/{}".format(num_errors, num_streams)
print_good(message) if num_errors == 0 else print_bad(message)
print "Took {:.2f} seconds for {} particles".format(time.time()-start_time, total_particles)
