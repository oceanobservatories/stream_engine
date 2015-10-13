#!/usr/bin/env python
import codecs
from collections import defaultdict
import os
import time
import re
import xray

import jinja2

dre = re.compile('(deployment\d\d\d\d)_(\S*).nc')

__author__ = 'ataylor'

BASE_SAN_DIR = '/opt/ooi/async'

ATTRIBUTE_CARRYOVER_MAP = {
    'time_coverage_start': {'type' :'string', 'func' : min},
    'time_coverage_end': {'type': 'string', 'func': max},
    'geospatial_lat_min': {'type': 'float', 'func' : min},
    'geospatial_lat_max': {'type': 'float', 'func' : max},
    'geospatial_lon_min': {'type': 'float', 'func' : min},
    'geospatial_lon_max': {'type': 'float', 'func' : max},
}

def get_nc_info(file_name):
    ds = xray.open_dataset(file_name, decode_times=False)
    ret_val={
        'size': ds.time.size,
    }
    for i in ATTRIBUTE_CARRYOVER_MAP:
        if i in ds.attrs:
            ret_val[i] =  ds.attrs[i]
    return ret_val


def collect_subjob_info(job_direct):
    """
    :param root_dir:  Root directory to start walking path
    :return: Return a dictionary of file names and coordinate sizes
    """
    root_dir = os.path.join(BASE_SAN_DIR,job_direct)
    subjob_info = {}
    for direct, subdirs, files in os.walk(root_dir):
        for i in files:
            if i.endswith('.nc'):
                idx = direct.index(job_direct)
                pname = direct[idx:]
                fname = os.path.join(pname, i)
                subjob_info[fname] =  get_nc_info(os.path.join(direct , i))
    return subjob_info


def ouput_ncml(mapping):
    loader = jinja2.FileSystemLoader(searchpath='/home/ataylor/Projects/stream_engine/templates')
    env = jinja2.Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)
    ncml_template = env.get_template('ncml.jinja')
    for combined_file, info_dict in mapping.iteritems():
        attr_dict = {}
        for i in ATTRIBUTE_CARRYOVER_MAP:
            try:
                vals = ATTRIBUTE_CARRYOVER_MAP[i]['func']([x[i] for x in info_dict.itervalues()])
                attr_dict[i] = {'value': vals,
                                'type': ATTRIBUTE_CARRYOVER_MAP[i]['type']}
            except KeyError:
                # The attribute is not in the data
                print 'err'
                pass
        with codecs.open(combined_file, 'wb', 'utf-8') as ncml_file:
            ncml_file.write(ncml_template.render(coord_dict=info_dict, attr_dict=attr_dict))


def generate_combination_map(direct, subjob_info):
    mapping = defaultdict(dict)
    for fname, info in subjob_info.iteritems():
        match = dre.search(fname)
        if match is not None:
            dep, rest_of_info = match.groups()
            ncml_name = '{:s}_{:s}.ncml'.format(dep, rest_of_info)
            ncml_name = os.path.join(direct, ncml_name)
            mapping[ncml_name][fname] = info
    return mapping


def aggregate(asyn_job_dir):
    t1 = time.time()
    subjob_info = collect_subjob_info(asyn_job_dir)
    t2 = time.time()
    print('Time {:f}'.format(t2 - t1))
    direct = os.path.join(BASE_SAN_DIR,asyn_job_dir)
    mapping = generate_combination_map(direct, subjob_info)
    ouput_ncml(mapping)
