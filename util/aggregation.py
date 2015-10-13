#!/usr/bin/env python
import argparse
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

def get_time_size(file_name):
    ds = xray.open_dataset(file_name, decode_times=False)
    return ds.time.size


def get_file_size(job_direct):
    """
    :param root_dir:  Root directory to start walking path
    :return: Return a dictionary of file names and coordinate sizes
    """
    root_dir = os.path.join(BASE_SAN_DIR,job_direct)
    coord_dict = {}
    for direct, subdirs, files in os.walk(root_dir):
        for i in files:
            if i.endswith('.nc'):
                idx = direct.index(job_direct)
                pname = direct[idx:]
                fname = os.path.join(pname, i)
                coord_dict[fname] =  get_time_size(os.path.join(direct , i))
    return coord_dict


def ouput_ncml(mapping):
    loader = jinja2.FileSystemLoader(searchpath='templates')
    env = jinja2.Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)
    ncml_template = env.get_template('ncml.jinja')
    for combined_file, coord_dict in mapping.iteritems():
        with codecs.open(combined_file, 'wb', 'utf-8') as ncml_file:
            ncml_file.write(ncml_template.render(coord_dict=coord_dict))


def generate_combination_map(direct, coord_dict):
    mapping = defaultdict(dict)
    for fname, size in coord_dict.iteritems():
        match = dre.search(fname)
        if match is not None:
            dep, rest_of_info = match.groups()
            ncml_name = '{:s}_{:s}.ncml'.format(dep, rest_of_info)
            ncml_name = os.path.join(direct, ncml_name)
            mapping[ncml_name][fname] = size
    return mapping


def aggregate(asyn_job_dir):
    t1 = time.time()
    coord_dict = get_file_size(asyn_job_dir)
    t2 = time.time()
    print('Time {:f}'.format(t2 - t1))
    direct = os.path.join(BASE_SAN_DIR,asyn_job_dir)
    mapping = generate_combination_map(direct, coord_dict)
    print mapping
    ouput_ncml(mapping)

def main():
    parser = argparse.ArgumentParser("Create the .ncml file to aggregate the datasets")
    d = 'frank/full'
    args = parser.parse_args()
    aggregate(d)

if __name__ == '__main__':
    main()
