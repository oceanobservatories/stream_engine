#!/usr/bin/env python
import codecs
from collections import defaultdict, OrderedDict
from itertools import chain
import os
import re
import xray
from engine.routes import app
import numpy as np

import jinja2

dre = re.compile('(deployment\d+_\S*).nc')

ATTRIBUTE_CARRYOVER_MAP = {
    'time_coverage_start': {'type': 'string', 'func': min},
    'time_coverage_end': {'type': 'string', 'func': max},
    'geospatial_lat_min': {'type': 'float', 'func': min},
    'geospatial_lat_max': {'type': 'float', 'func': max},
    'geospatial_lon_min': {'type': 'float', 'func': min},
    'geospatial_lon_max': {'type': 'float', 'func': max},
}


def extract_single_value(arr):
    return [i[0] for i in arr]


def flatten(arr):
    return list(chain.from_iterable(arr))


VARIABLE_CARRYOVER_MAP = {
    'streaming_provenance': {'type': 'string', 'func': extract_single_value},
    'computed_provenance': {'type': 'string', 'func': extract_single_value},
    'query_parameter_provenance': {'type': 'string', 'func': extract_single_value},
    'instrument_provenance': {'type': 'string', 'func': extract_single_value},
    'provenance_messages': {'type': 'string', 'func': flatten},
    'annotations': {'type': 'string', 'func': flatten},
}


def get_nc_info(file_name):
    string_sizes = {}
    with xray.open_dataset(file_name, decode_times=False) as ds:
        ret_val = {
            'size': ds.time.size,
        }
        for i in ATTRIBUTE_CARRYOVER_MAP:
            if i in ds.attrs:
                ret_val[i] = ds.attrs[i]

        if 'l0_provenance_keys' in ds.variables and 'l0_provenance_data' in ds.variables:
            ret_val['l0_provenance'] = zip(ds.variables['l0_provenance_keys'].values,
                                       ds.variables['l0_provenance_data'].values)

        ret_val['file_start_time'] = ds.time.values[-1]
        for i in VARIABLE_CARRYOVER_MAP:
            if i in ds.variables:
                ret_val[i] = ds.variables[i].values
        for var in ds.variables:
            if ds.variables[var].dtype.kind == 'S':
                string_sizes[var] =  ds[var].dtype.itemsize
            #xray has a habit of loading strings as objects.  So cast it to a string and get the max size.
            elif ds.variables[var].dtype == 'object':
                string_sizes[var] = ds.variables[var].values.astype(str).dtype.itemsize
    return ret_val, string_sizes


def collect_subjob_info(job_direct):
    """
    :param root_dir:  Root directory to start walking path
    :return: Return a dictionary of file names and coordinate sizes
    """
    root_dir = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'], job_direct)
    subjob_info = {}
    string_sizes = {}
    for direct, subdirs, files in os.walk(root_dir):
        for i in files:
            if i.endswith('.nc'):
                idx = direct.index(job_direct)
                pname = direct[idx+len(job_direct)+1:]
                fname = os.path.join(pname, i)
                nc_info, ss = get_nc_info(os.path.join(direct, i))
                subjob_info[fname] = nc_info
                # store the size of all string parameters in the nc file
                string_sizes[os.path.join(direct, i)]  = ss

    #get a set of all of the sizes strings in the subjobs
    var_sizes = defaultdict(set)
    for i in string_sizes:
        for v in string_sizes[i]:
            var_sizes[v].add(string_sizes[i][v])
    to_mod = {}
    # check to see if we have any mismatches
    for v in var_sizes:
        if len(var_sizes[v]) > 1:
            to_mod[v] = max(var_sizes[v])
    # if mismatches we need to rewrite the string values
    if len(to_mod) > 0:
        files = string_sizes.keys()
        modify_strings(files, to_mod)
    return subjob_info

def modify_strings(files, to_mod):
    """
    :param files:  List of files to rewrite
    :param to_mod:  Dictonary of variables that need to be modified to the max size
    :return:
    """
    for f in files:
        with xray.open_dataset(f, decode_times=False) as ds:
            for var, size in to_mod.iteritems():
            # pad the strings
                ds.variables[var].values = np.array([x.ljust(size) for x in ds.variables[var].values]).astype(str)
            new_ds = ds.copy(deep=True)
            new_ds.load()
        new_ds.to_netcdf(f)

def do_provenance(param):
    prov_dict = {}
    for set_prov in param:
        for k, v in set_prov:
            prov_dict[k] = v
    keys = []
    values = []
    for k, v in prov_dict.iteritems():
        keys.append(k)
        values.append(v)
    return keys, values


def output_ncml(mapping, async_job_dir):
    loader = jinja2.FileSystemLoader(searchpath='templates')
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
                pass

        # do something with provenance...
        file_start_time = [x['file_start_time'] for x in info_dict.itervalues()]
        try:
            l0keys, l0values = do_provenance([x['l0_provenance'] for x in info_dict.itervalues()])
            variable_dict = {
                'l0_provenance_keys': {'value': l0keys, 'type': 'string', 'size': len(l0keys), 'separator': '*'},
                'l0_provenance_data': {'value': l0values, 'type': 'string', 'size': len(l0values), 'separator': '*'},
                'combined_file_start_time': {'value': file_start_time, 'type': 'float', 'size': len(file_start_time), 'separator': None}
            }
        except KeyError:
            #no l0_provenance output
            variable_dict = {
                'combined_file_start_time': {'value': file_start_time, 'type': 'float', 'size': len(file_start_time), 'separator': None}
            }

        for i in VARIABLE_CARRYOVER_MAP:
            try:
                arr = []
                for x in info_dict.itervalues():
                    if i in x:
                        arr.append(x[i])
                if len(arr) > 0:
                    vals = VARIABLE_CARRYOVER_MAP[i]['func'](arr)
                    variable_dict[i] = {'value': vals, 'type': VARIABLE_CARRYOVER_MAP[i]['type'], 'size': len(vals),
                                    'separator': '*'}
            except KeyError:
                pass
        with codecs.open(combined_file, 'wb', 'utf-8') as ncml_file:
            ncml_file.write(
                ncml_template.render(coord_dict=info_dict, attr_dict=attr_dict,
                                     var_dict=variable_dict))

        # aggregate the netcdf files now.
        datasets = []
        size = 0
        for datafile, info in info_dict.iteritems():
            size += info['size']
            to_open = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'], async_job_dir, datafile)
            # open the dataset
            ds = xray.open_dataset(to_open)

            # for each variable that we are going to readd later remove it from the dataset
            to_delete = set()
            for i in variable_dict:
                if i in ds:
                    to_delete.add(i)
                    for thing in ds[i].coords:
                        to_delete.add(thing)
            for deleteme in list(to_delete):
                if deleteme in  ds:
                    del ds[deleteme]
            datasets.append(ds)

        new_ds = xray.concat(datasets, dim='obs', data_vars='minimal', coords='minimal')

        #add and fix up variables
        new_ds['obs'].values= np.array([x for x in range(new_ds.obs.size)], dtype=np.int32)
        for updated_var, info in variable_dict.iteritems():
            data = info['value']
            npdata = np.array(data)
            new_ds[updated_var] = (updated_var + '_dim0', npdata, {'long_name': updated_var})
        base, _ = os.path.splitext(combined_file)
        new_ds.to_netcdf(base+ '.nc')


def generate_combination_map(direct, subjob_info):
    mapping = defaultdict(dict)
    for fname, info in subjob_info.iteritems():
        match = dre.search(fname)
        if match is not None:
            file_base = match.groups()[0]
            ncml_name = '{:s}.ncml'.format(file_base)
            ncml_name = os.path.join(direct, ncml_name)
            mapping[ncml_name][fname] = info
    #sort the map so the time in the file increases along with obs
    sorted_map = {}
    for fname, sji in mapping.iteritems():
        sorted_subjobs = OrderedDict()
        for subjob in sorted(sji):
            sorted_subjobs[subjob] = sji[subjob]
        sorted_map[fname] = sorted_subjobs
    return sorted_map


def aggregate(async_job_dir):
    subjob_info = collect_subjob_info(async_job_dir)
    direct = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'], async_job_dir)
    mapping = generate_combination_map(direct, subjob_info)
    output_ncml(mapping, async_job_dir)
