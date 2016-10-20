#!/usr/bin/env python
import codecs
import fnmatch
import json
from collections import defaultdict, OrderedDict
import glob
from itertools import chain
import os
import re
import xarray as xr
import logging
from engine import app
import numpy as np

import jinja2

from util.common import log_timing
from util.datamodel import compile_datasets
from util.gather import gather_files
from util.netcdf_utils import write_netcdf, add_dynamic_attributes, analyze_datasets

log = logging.getLogger(__name__)

dre = re.compile('(deployment\d+_\S*).nc')


MAX_AGGREGATION_SIZE = app.config['MAX_AGGREGATION_SIZE']
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


def get_nc_info(file_name):
    with xr.open_dataset(file_name, decode_times=False, mask_and_scale=False, decode_cf=False) as ds:
        ret_val = {
            'size': ds.obs.size,
        }
        for i in ATTRIBUTE_CARRYOVER_MAP:
            if i in ds.attrs:
                ret_val[i] = ds.attrs[i]

        ret_val['file_start_time'] = ds.time.values[-1]

    return ret_val


def collect_subjob_info(job_direct):
    """
    :return: Return a dictionary of file names and coordinate sizes
    """
    subjob_info = {}
    for direct, subdirs, files in os.walk(job_direct):
        for fname in fnmatch.filter(files, '*.nc'):
            fpath = os.path.join(job_direct, fname)
            nc_info = get_nc_info(fpath)
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


def output_ncml(mapping, request_id=None):
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
        variable_dict = {
            'combined_file_start_time': {'value': file_start_time, 'type': 'float', 'size': len(file_start_time),
                                         'separator': None}
        }

        with codecs.open(combined_file, 'wb', 'utf-8') as ncml_file:
            ncml_file.write(
                    ncml_template.render(coord_dict=info_dict, attr_dict=attr_dict,
                                         var_dict=variable_dict))


def generate_combination_map(out_dir, subjob_info):
    mapping = defaultdict(dict)
    for fname, info in subjob_info.iteritems():
        match = dre.search(fname)
        if match is not None:
            file_base = match.groups()[0]
            index = file_base.rfind('_')
            nameprefix = file_base[:index] if index > 0 else file_base
            ncml_name = '{:s}.ncml'.format(nameprefix)
            ncml_name = os.path.join(out_dir, ncml_name)
            mapping[ncml_name][fname] = info
    # sort the map so the time in the file increases along with obs
    sorted_map = {}
    for fname, sji in mapping.iteritems():
        sorted_subjobs = OrderedDict()
        for subjob in sorted(sji):
            sorted_subjobs[subjob] = sji[subjob]
        sorted_map[fname] = sorted_subjobs
    return sorted_map


def aggregate_provenance_group(job_dir, files):
    aggregate_dict = {}
    for f in sorted(files):
        path = os.path.join(job_dir, f)
        data = json.load(open(path))
        for key in data:
            if key == 'instrument_provenance':
                aggregate_dict[key] = data[key]
            elif key == 'provenance':
                aggregate_dict.setdefault(key, {}).update(data[key])
            else:
                aggregate_dict.setdefault(key, {})[f] = data[key]

    return aggregate_dict


@log_timing(log)
def aggregate_provenance(job_dir, output_dir, request_id=None):
    groups = {}
    prov_label = '_provenance_'
    for f in os.listdir(job_dir):
        if prov_label in f and f.endswith('json'):
            group = f.split(prov_label)[0]
            groups.setdefault(group, []).append(f)

    for group in groups:
        aggregate_dict = aggregate_provenance_group(job_dir, groups[group])
        with open(os.path.join(output_dir, '%s_aggregate_provenance.json' % group), 'w') as fh:
            json.dump(aggregate_dict, fh, indent=2)


def get_name(ds, group_name):
    start = ds.attrs['time_coverage_start'].translate(None, '-:')
    end = ds.attrs['time_coverage_end'].translate(None, '-:')

    return '%s_%s-%s.nc' % (group_name, start, end)


@log_timing(log)
def shape_up(dataset, parameters, request_id=None):
    """
    Ensure that all parameters in this dataset match the supplied dimensions
    padding with the fill value as necessary.
    Dataset is modified in place
    :param dataset: dataset to be updated
    :param shapes: map of expected shapes
    :return:
    """
    temp_dims = []

    if 'obs' in dataset:
        for var in parameters:
            shape = (dataset.obs.size, ) + parameters[var]['shape']
            dtype = parameters[var]['dtype']
            dims = ('obs', ) + parameters[var]['dims']
            fill = parameters[var]['fill']

            if var not in dataset:
                fv = np.zeros(shape).astype(dtype)
                fv[:] = fill

                # insert the missing data into our dataset as fill values
                dataset[var] = (dims, fv, {'_FillValue': fill})

            else:
                if dataset[var].dims == dims and dataset[var].shape == shape:
                    # Nothing to do here
                    continue

                # uh-oh, dimensions/shape don't match
                if dataset[var].shape == shape:
                    # only dimension names are mismatched. Rewrite with "correct" names
                    dataset[var] = (dims, dataset[var].values, dataset[var].attrs)
                    continue

                # shape and dimensions mismatched
                # pad data and re-insert
                pads = []
                current_shape = dataset[var].shape
                vals = dataset[var].values

                # add any missing dimensions
                while len(shape) > len(current_shape):
                    current_shape += (1, )

                # if dimensions were added, reshape the data
                if current_shape != dataset[var].shape:
                    vals = vals.reshape(current_shape)

                # generate any necessary pads
                for index, size in enumerate(shape):
                    pads.append((0, size-current_shape[index]))

                # if dimension names are the same but shape has changed
                # we have to rename the existing dimension(s) or we won't
                # be able to re-insert our data
                for index, dim_name in enumerate(dataset[var].dims):
                    # skip the obs dimension
                    if index == 0:
                        continue
                    if dim_name == dims[index]:
                        temp_name = 'TEMP_DIM_%s_%d' % (var, index)
                        dataset.rename({dim_name: temp_name}, inplace=True)
                        temp_dims.append(temp_name)

                # pad the data and re-insert
                padded_data = np.pad(vals, pads, mode='constant', constant_values=fill)
                dataset[var] = (dims, padded_data, dataset[var].attrs)

        # delete any temporary dimensions created
        for dim in temp_dims:
            del dataset[dim]


@log_timing(log)
def concatenate_and_write(datasets, out_dir, group_name, request_id=None):
    ds = compile_datasets(datasets)
    add_dynamic_attributes(ds)
    write_netcdf(ds, os.path.join(out_dir, get_name(ds, group_name)))


@log_timing(log)
def aggregate_netcdf_group(job_dir, output_dir, files, group_name, request_id=None):
    datasets = []
    accum_size = 0
    parameters = analyze_datasets(job_dir, files, request_id=request_id)
    for f in sorted(files):
        path = os.path.join(job_dir, f)
        size = os.stat(path).st_size
        accum_size += size
        if accum_size > MAX_AGGREGATION_SIZE:
            concatenate_and_write(datasets, output_dir, group_name, request_id=request_id)
            accum_size = size
            datasets = []

        with xr.open_dataset(path, decode_times=False, mask_and_scale=False, decode_cf=False) as ds:
            ds.load()
            shape_up(ds, parameters, request_id=request_id)
            datasets.append(ds)

    if datasets:
        concatenate_and_write(datasets, output_dir, group_name, request_id=request_id)


@log_timing(log)
def aggregate_netcdf(job_dir, output_dir, request_id=None):
    groups = {}
    for f in fnmatch.filter(os.listdir(job_dir), '*.nc'):
        group = f.rsplit('_', 1)[0]
        groups.setdefault(group, []).append(f)

    for group in groups:
        aggregate_netcdf_group(job_dir, output_dir, groups[group], group, request_id=request_id)


@log_timing(log)
def generate_ncml(job_dir, out_dir, request_id=None):
    subjob_info = collect_subjob_info(job_dir)
    mapping = generate_combination_map(out_dir, subjob_info)
    output_ncml(mapping, out_dir)


def aggregate_status(job_dir, out_dir, request_id=None):
    results = []
    for f in fnmatch.filter(os.listdir(job_dir), '*-status.txt'):
        start, stop, _ = f.split('-', 2)
        results.append((start, stop, 'complete'))

    for f in fnmatch.filter(os.listdir(job_dir), '*-failure.json'):
        start, stop, _ = f.split('-', 2)
        results.append((start, stop, json.load(open(os.path.join(job_dir, f)))))

    results.sort()
    out = OrderedDict()
    for start, stop, value in results:
        key = '-'.join((start, stop))
        out[key] = value

    with open(os.path.join(out_dir, 'status.json'), 'w') as fh:
        json.dump(out, fh, indent=2)


def aggregate_csv(job_dir, out_dir, request_id=None):
    # TODO -- aggregate CSV/TSV files
    for f in fnmatch.filter(os.listdir(job_dir), '*.[ct]sv'):
        os.rename(os.path.join(job_dir, f),
                  os.path.join(out_dir, f))


def cleanup(job_dir, request_id=None):
    # NO-OP for now, once testing is complete, remove local files.
    # TODO
    pass


def aggregate(async_job_dir, request_id=None):
    local_dir = os.path.join(app.config['LOCAL_ASYNC_DIR'], async_job_dir)
    final_dir = os.path.join(app.config['FINAL_ASYNC_DIR'], async_job_dir)
    se_nodes = app.config['STREAM_ENGINE_NODES']

    # Fetch all files from remote nodes
    gather_files(se_nodes, local_dir)

    # old aggregation
    generate_ncml(local_dir, local_dir, request_id=request_id)

    # new aggregation
    if not os.path.exists(final_dir):
        os.makedirs(final_dir)

    aggregate_status(local_dir, final_dir, request_id=request_id)
    aggregate_csv(local_dir, final_dir, request_id=request_id)
    aggregate_netcdf(local_dir, final_dir, request_id=request_id)
    aggregate_provenance(local_dir, final_dir, request_id=request_id)
    generate_ncml(local_dir, final_dir, request_id=request_id)
    cleanup(local_dir, request_id=request_id)


