#!/usr/bin/env python
import codecs
from collections import defaultdict, OrderedDict
import glob
from itertools import chain
import os
import re
import xray as xr
import logging
from engine import app
import numpy as np

import jinja2

log = logging.getLogger(__name__)

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
    with xr.open_dataset(file_name, decode_times=False) as ds:
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
    return ret_val


def collect_subjob_info(job_direct):
    """
    :return: Return a dictionary of file names and coordinate sizes
    """
    root_dir = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'], job_direct)
    subjob_info = {}
    for direct, subdirs, files in os.walk(root_dir):
        for i in files:
            if i.endswith('.nc'):
                idx = direct.index(job_direct)
                pname = direct[idx + len(job_direct) + 1:]
                fname = os.path.join(pname, i)
                nc_info = get_nc_info(os.path.join(direct, i))
                subjob_info[fname] = nc_info

    return subjob_info


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


def output_ncml(mapping):
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
                'combined_file_start_time': {'value': file_start_time, 'type': 'float', 'size': len(file_start_time),
                                             'separator': None}
            }
        except KeyError:
            # no l0_provenance output
            variable_dict = {
                'combined_file_start_time': {'value': file_start_time, 'type': 'float', 'size': len(file_start_time),
                                             'separator': None}
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


def generate_combination_map(direct, subjob_info):
    mapping = defaultdict(dict)
    for fname, info in subjob_info.iteritems():
        match = dre.search(fname)
        if match is not None:
            file_base = match.groups()[0]
            ncml_name = '{:s}.ncml'.format(file_base)
            ncml_name = os.path.join(direct, ncml_name)
            mapping[ncml_name][fname] = info
    # sort the map so the time in the file increases along with obs
    sorted_map = {}
    for fname, sji in mapping.iteritems():
        sorted_subjobs = OrderedDict()
        for subjob in sorted(sji):
            sorted_subjobs[subjob] = sji[subjob]
        sorted_map[fname] = sorted_subjobs
    return sorted_map


def aggregate(async_job_dir):
    if app.config['AGGREGATE']:
        subjob_info = collect_subjob_info(async_job_dir)
        direct = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'], async_job_dir)
        mapping = generate_combination_map(direct, subjob_info)
        output_ncml(mapping)
        erddap(async_job_dir)


def find_representative_nc_file(file_paths, file_base):
    nc_name = file_base + '.nc'
    agg_nc = os.path.join(file_paths, nc_name)
    if os.path.exists(agg_nc):
        # return aggregated file and we want to include it.
        return agg_nc, True
    else:
        # walk directory and find first include subfile we do not want to include nc file in output
        for directory, _, files in os.walk(file_paths):
            for f in files:
                if f == nc_name:
                    return os.path.join(directory, f), False


def map_erddap_type(dtype):
    """
    Returns the ERDDAP data type for a given dtype
    """
    dtype_map = {
        np.dtype('float64'): 'double',
        np.dtype('float32'): 'float',
        np.dtype('int64'): 'long',
        np.dtype('int32'): 'int',
        np.dtype('int16'): 'short',
        np.dtype('int8'): 'byte',
        np.dtype('uint64'): 'unsignedLong',
        np.dtype('uint32'): 'unsignedInt',
        np.dtype('uint16'): 'unsignedShort',
        np.dtype('uint8'): 'unsignedByte',
        np.dtype('bool'): 'boolean',
        np.dtype('S'): 'String',
        np.dtype('O'): 'String',
    }
    if dtype.char == 'S':
        return 'String'

    return dtype_map[dtype]


def get_type_map(nc_file_name, index='obs'):
    data_vars = {}
    with xr.open_dataset(nc_file_name, decode_times=False) as ds:
        for nc_var in ds.variables:
            # if multi variables continue to cause problems we can drop them here if dims > 1
            if ds[nc_var].dims[0] == index:
                data_type = map_erddap_type(ds[nc_var].dtype)
                data_vars[nc_var] = {'dataType': data_type, 'attrs': {}}
                for i in ds[nc_var].attrs:
                    data_vars[nc_var]['attrs'][i] = ds[nc_var].attrs[i]
    return data_vars


def get_template():
    """
    Returns the XML for the dataset entry in ERDDAP's datasets.xml
    """
    loader = jinja2.FileSystemLoader(searchpath='templates')
    env = jinja2.Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)
    template = env.get_template('erddap_dataset.jinja')
    return template


def get_attr_dict(nc_file):
    attrs = {}
    with xr.open_dataset(nc_file, decode_times=False) as ds:
        for i in ds.attrs:
            attrs[i] = ds.attrs[i]
    return attrs


def erddap(agg_dir):
    path_to_dataset = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'], agg_dir)
    _, async_job_id = os.path.split(path_to_dataset)
    # get datasets (deployments and nc files) and representative netcdf files and whether we are doing ncml or just one
    for ncml_file in glob.glob(path_to_dataset + '/*.ncml'):
        _, fname = os.path.split(ncml_file)
        file_base, _ = os.path.splitext(fname)
        nc_file, include = find_representative_nc_file(path_to_dataset, file_base)
        dataset_vars = get_type_map(nc_file)
        attr_dict = get_attr_dict(nc_file)
        template = get_template()
        title = '{:s}_{:s}'.format(async_job_id, file_base)
        with codecs.open(os.path.join(path_to_dataset, file_base + '_erddap.xml'), 'wb', 'utf-8') as erddap_file:
            erddap_file.write(template.render(dataset_title=title,
                                              dataset_id=title,
                                              dataset_dir=path_to_dataset,
                                              data_vars=dataset_vars,
                                              attr_dict=attr_dict,
                                              base_file_name=file_base,
                                              recursive=str(not include).lower(),
                                              ))
