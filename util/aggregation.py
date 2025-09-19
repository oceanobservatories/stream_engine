#!/usr/bin/env python
import codecs
import fnmatch
import json
import logging
import os
import re
import shutil
import signal
import time
from collections import OrderedDict, defaultdict

import jinja2
import numpy as np
from engine import app
from util.common import PROVENANCE_KEYORDER, TimedOutException, log_timing, sort_dict
from util.datamodel import compile_datasets
from util.gather import gather_files
from util.netcdf_utils import add_dynamic_attributes, analyze_datasets, write_netcdf
from util.xarray_overrides import xr

log = logging.getLogger(__name__)

dre = re.compile('(deployment\d+_\S*).nc')
# support new style and legacy directory naming - the new style has timestamps accurate to milliseconds vs seconds and
# the character 'Z' to indicate UTC
valid_jobdir_re = re.compile('\d{8}T(\d{6}|\d{9}Z)-')


MAX_AGGREGATION_SIZE = app.config['MAX_AGGREGATION_SIZE']
AGGREGATION_RANGE = app.config['AGGREGATION_RANGE']
AGGREGATION_SLICE_SIZE = app.config['AGGREGATION_SLICE_SIZE']
TEMP_AGGREGATION_FILE = app.config['TEMP_AGGREGATION_FILE']

ATTRIBUTE_CARRYOVER_MAP = {
    'time_coverage_start': {'type': 'string', 'func': min},
    'time_coverage_end': {'type': 'string', 'func': max},
    'geospatial_lat_min': {'type': 'float', 'func': min},
    'geospatial_lat_max': {'type': 'float', 'func': max},
    'geospatial_lon_min': {'type': 'float', 'func': min},
    'geospatial_lon_max': {'type': 'float', 'func': max},
}


def get_nc_info(file_name):
    with xr.open_dataset(file_name, decode_times=False, mask_and_scale=False, decode_coords=False) as ds:
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
            ncml_file.write(ncml_template.render(coord_dict=info_dict, attr_dict=attr_dict, var_dict=variable_dict))


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
            # json.load undoes the ordering of provenance_metadata - reapply correct order here
            for key in aggregate_dict['instrument_provenance']:
                aggregate_dict['instrument_provenance'][key] = [sort_dict(e, PROVENANCE_KEYORDER, sorted_first=False)
                                                                for e in aggregate_dict['instrument_provenance'][key]]
            json.dump(aggregate_dict, fh, indent=2)


def aggregate_annotation_group(job_dir, files):
    aggregate_dict = {'annotations': []}
    for f in sorted(files):
        path = os.path.join(job_dir, f)
        data = json.load(open(path))
        for key in data:
            # value is an annotation list, so add entries to aggregated annotation list
            if key == 'annotations':
                annotation_list = aggregate_dict[key]
                new_annotations = [x for x in data[key] if x not in annotation_list]
                annotation_list.extend(new_annotations)
            # encountered extraneous data
            # copy the JSON to the aggregate JSON but add a filename 'sub-key'
            # i.e. key: {filename: value} for key: value in the source
            else:
                aggregate_dict.setdefault(key, {})[f] = data[key]

    return aggregate_dict


@log_timing(log)
def aggregate_annotations(job_dir, output_dir, request_id=None):
    groups = {}
    anno_label = '_annotations_'
    for f in os.listdir(job_dir):
        if anno_label in f and f.endswith('json'):
            group = f.split(anno_label)[0]
            groups.setdefault(group, []).append(f)
    
    for group in groups:
        aggregate_dict = aggregate_annotation_group(job_dir, groups[group])
        with open(os.path.join(output_dir, '%s_annotations.json' % group), 'w') as fh:
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
    temp_dims = {}

    if 'obs' in dataset.dims:
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
                # support data without an observation dimension (13025 AC2)
                if 'obs' not in dataset[var].dims:
                    dims = parameters[var]['dims']
                    shape = parameters[var]['shape']

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
                    if dim_name == "obs":
                        continue
                    if dim_name == dims[index]:
                        temp_name = 'TEMP_DIM_%s_%d' % (var, index)
                        dataset.rename({dim_name: temp_name}, inplace=True)
                        temp_dims[temp_name] = dim_name

                # pad the data and re-insert
                padded_data = np.pad(vals, pads, mode='constant', constant_values=fill)
                dataset[var] = (dims, padded_data, dataset[var].attrs)

        # delete any temporary dimensions created
        for dim in temp_dims.keys():
            # The rename and drop strategy can mess up coordinates
            # Go ahead and drop the renamed variable, but only after a correct coordinate has been created
            if dim in dataset.coords:
                original_dim = temp_dims[dim]
                new_dim_length = dataset.dims[original_dim]
                # create a new data variable representing the corrected coordinate data
                # reuse the attrs from the renamed coordinate
                dataset[original_dim] = ((original_dim), np.arange(new_dim_length), dataset[dim].attrs)
            if dim in dataset.variables:
                dataset.drop(labels=dim, dim=None, inplace=True)
        

@log_timing(log)
def concatenate_and_write(datasets, out_dir, group_name, request_id=None):
    # keep track of data not dimensioned along obs (13025 AC2)
    non_obs_data = []
    for ds in datasets:
        non_obs_data = [var for var in ds.data_vars if 'obs' not in ds[var].dims]

    # Look for alternate filtering based on stream name
    stream_name = group_name.split('-')[-1]
    filter_variable_type_map = app.config['STREAM_DEDUPLICATION_MAP'].get(stream_name, None)

    # compiled data sets will compile all data along the obs dimension
    ds = compile_datasets(datasets, filter_variable_type_map=filter_variable_type_map)

    # remove obs dimension from non_obs data (13025 AC2)
    for non_obs in non_obs_data:
        if 'obs' in ds[non_obs].dims:
            ds[non_obs] = (ds[non_obs].dims[1:], ds[non_obs].values[0], ds[non_obs].attrs)

    add_dynamic_attributes(ds)
    write_netcdf(ds, os.path.join(out_dir, get_name(ds, group_name)))


@log_timing(log)
def aggregate_netcdf_group(job_dir, output_dir, files, group_name, request_id=None):
    datasets = []
    #track size of full netcdf files aggregated to gether
    accum_size = 0
    #track size of split netcdf files
    concat_size = 0
    parameters = analyze_datasets(job_dir, files, request_id=request_id)
    for f in sorted(files):
        path = os.path.join(job_dir, f)
        orig_file_size = os.stat(path).st_size
        concat_size = accum_size
        accum_size += orig_file_size   
        if accum_size < MAX_AGGREGATION_SIZE:    
            # coordinates must be decoded to gracefully handle mismatched coordinates during dataset concatenation
            with xr.open_dataset(path, decode_times=False, mask_and_scale=False, decode_coords=True) as ds:
                ds.load()
                #log.error("Parameters: %s", parameters)
                shape_up(ds, parameters, request_id=request_id)
                datasets.append(ds)
            # aggregated file size should be within allowable range so write it to disk 
            if accum_size >= MAX_AGGREGATION_SIZE-AGGREGATION_RANGE:
                concatenate_and_write(datasets, output_dir, group_name, request_id=request_id)
                accum_size = 0
                concat_size = 0
                datasets = []
        elif accum_size > MAX_AGGREGATION_SIZE:
            #aggregated NetCDF file would be too big, so we will try to slice the original file into smaller sections until the aggretated file is the appropriate size
            dset = None
            # coordinates must be decoded to gracefully handle mismatched coordinates during dataset concatenation
            with xr.open_dataset(path, decode_times=False, mask_and_scale=False, decode_coords=True) as ds:
                ds.load()
                dset = ds
            slice_start = 0
            data_size = dset['time'].size            
            while slice_start < data_size:
                # slice the original netcdf file into smaller pieces, and aggretate the slice until we reach MAX_AGGREGATION_SIZE 
                subset = dset.isel(obs=slice(slice_start, slice_start+AGGREGATION_SLICE_SIZE)).load()
                slice_start=slice_start+AGGREGATION_SLICE_SIZE
                shape_up(subset, parameters, request_id=request_id)

                # write temp file to calculate the actual size of the slice on disk
                concat_path =os.path.join(output_dir, TEMP_AGGREGATION_FILE)
                write_netcdf(subset, concat_path)
                temp_concat_size = os.stat(concat_path).st_size
                concat_size = temp_concat_size + concat_size
                datasets.append(subset)
                accum_size = concat_size
         
                # amount left to read in original file is minimal, so concatanate remainder to netcdf file and reset
                if concat_size >= MAX_AGGREGATION_SIZE-AGGREGATION_RANGE:
                    concatenate_and_write(datasets, output_dir, group_name, request_id=request_id)
                    accum_size = 0
                    concat_size = 0
                    datasets = []

    if datasets:
        #write any remaing data sets to NetCDF file
        concatenate_and_write(datasets, output_dir, group_name, request_id=request_id)
    # cleanup temporary aggregation file    
    temp_file = os.path.join(output_dir, TEMP_AGGREGATION_FILE)
    if os.path.exists(temp_file):
        os.remove(temp_file)  


@log_timing(log)
def aggregate_netcdf(job_dir, output_dir, request_id=None):
    groups = {}
    for f in fnmatch.filter(os.listdir(job_dir), '*.nc'):
        group = f.rsplit('_', 1)[0]
        groups.setdefault(group, []).append(f)

    for group in groups:
        try:
            aggregate_netcdf_group(job_dir, output_dir, groups[group], group, request_id=request_id)
        except Exception as e:
            log.exception('<%s> Exception aggregating group: %r', request_id, group)
            # Aggregation failed, move the un-aggregated files to the output directory
            for filename in groups[group]:
                shutil.move(os.path.join(job_dir, filename),
                            os.path.join(output_dir, filename))
            # cleanup temporary aggregation file                
            temp_file = os.path.join(output_dir, TEMP_AGGREGATION_FILE)
            if os.path.exists(temp_file):
                os.remove(temp_file)


@log_timing(log)
def generate_ncml(job_dir, out_dir, request_id=None):
    subjob_info = collect_subjob_info(job_dir)
    mapping = generate_combination_map(out_dir, subjob_info)
    output_ncml(mapping, out_dir)


@log_timing(log)
def aggregate_status(job_dir, out_dir, request_id=None):
    results = {}
    for f in fnmatch.filter(os.listdir(job_dir), '*-status.txt'):
        key, _ = f.rsplit('-', 1)
        results[key] = 'complete'

    # failures generate a complete message and a failure.
    # only keep the failure
    for f in fnmatch.filter(os.listdir(job_dir), '*-failure.json'):
        key, _ = f.rsplit('-', 1)
        results[key] = json.load(open(os.path.join(job_dir, f)))

    out = OrderedDict()
    for key in sorted(results):
        out[key] = results[key]
       
    # completely empty job_dir checked in aggregate()
    # non-empty job_dir, but no status files 
    if not out:
        out = {
            "code": 500,
            "message": "Aggregation found no status files (status.txt/failure.json)!"
        }

    with open(os.path.join(out_dir, 'status.json'), 'w') as fh:
        json.dump(out, fh, indent=2)


@log_timing(log)
def aggregate_csv(job_dir, out_dir, request_id=None):
    # TODO -- aggregate CSV/TSV files - current logic copies files over
    # as is instead of combining them when applicable
    for f in fnmatch.filter(os.listdir(job_dir), '*.[ct]sv'):
        shutil.move(os.path.join(job_dir, f),
                    os.path.join(out_dir, f))


@log_timing(log)
def aggregate_json(job_dir, out_dir, request_id=None):
    # TODO -- aggregate JSON files - current logic copies files over
    # as is instead of combining them when applicable
    for f in fnmatch.filter(os.listdir(job_dir), '*.json'):
        # only process particle data files
        if 'deployment' in f and not ('annotation' in f or 'provenance' in f):
            shutil.move(os.path.join(job_dir, f),
                        os.path.join(out_dir, f))


@log_timing(log)
def cleanup(job_dir, request_id=None):
    """
    All files have been aggregated, remove the pre-aggregation files
    :param job_dir:
    :param request_id:
    :return:
    """
    # SANITY CHECK. Ensure the supplied directory exists.
    if not os.path.isdir(job_dir):
        log.error('<%s> Cannot cleanup, %s is not a directory', request_id, job_dir)

    # SANITY CHECK Ensure directory is owned by me.
    if not os.stat(job_dir).st_uid == os.getuid():
        log.error('<%s> Cannot cleanup, %s not owned by me', request_id, job_dir)

    # SANITY CHECK Ensure directory follows our naming convention.
    if not valid_jobdir_re.match(os.path.basename(job_dir)):
        log.error('<%s> Cannot cleanup, %s does not meet expected naming convention', request_id, job_dir)

    # SANITY CHECK Ensure directory has no subdirectories.
    files = os.listdir(job_dir)
    if any((os.path.isdir(f) for f in files)):
        log.error('<%s> Cannot cleanup, %s contains subdirectories', request_id, job_dir)

    # Unlink all files and remove directory
    for f in files:
        os.unlink(os.path.join(job_dir, f))
    os.rmdir(job_dir)


def log_completion(job_dir):
    with open(os.path.join(job_dir, 'status.txt'), 'w') as fh:
        fh.write('complete\n')


def log_failure(e, job_dir):
    output = {
        "code": 500,
        "message": "Aggregation failed for the following reason: %s" % e.message
    }
    json_str = json.dumps(output, indent=2, separators=(',', ': '))
    with open(os.path.join(job_dir, 'status.txt'), 'w') as fh:
        fh.write(json_str)


def is_aggregation_progressing(**kwargs):
    async_job_dir = kwargs['async_job_dir']
    poll_period = kwargs['poll_period']
    local_dir = os.path.join(app.config['LOCAL_ASYNC_DIR'], async_job_dir)
    final_dir = os.path.join(app.config['FINAL_ASYNC_DIR'], async_job_dir)
    return has_updated_within(local_dir, poll_period) or has_updated_within(final_dir, poll_period)


def has_updated_within(directory, seconds):
    files = [fle for rt, _, f in os.walk(directory) for fle in f if
             time.time() - os.stat(os.path.join(rt, fle)).st_mtime < seconds]
    if files:
        return True
    return False


@log_timing(log)
def aggregate(async_job_dir, request_id=None):
    local_dir = os.path.join(app.config['LOCAL_ASYNC_DIR'], async_job_dir)
    final_dir = os.path.join(app.config['FINAL_ASYNC_DIR'], async_job_dir)
    se_nodes = app.config['STREAM_ENGINE_NODES']
    
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    
    # new aggregation
    if not os.path.exists(final_dir):
        os.makedirs(final_dir)

        # Fetch all files from remote nodes
        gather_files(se_nodes, local_dir)

    try:
        # check for empty local_dir
        if os.listdir(local_dir):
            aggregate_status(local_dir, final_dir, request_id=request_id)
            aggregate_json(local_dir, final_dir, request_id=request_id)
            aggregate_csv(local_dir, final_dir, request_id=request_id)
            aggregate_netcdf(local_dir, final_dir, request_id=request_id)
            aggregate_provenance(local_dir, final_dir, request_id=request_id)
            aggregate_annotations(local_dir, final_dir, request_id=request_id)
            generate_ncml(final_dir, final_dir, request_id=request_id)
        # local_dir not empty - aggregate files
        else:
            output = {
                "code": 500,
                "message": "Aggregation called on empty directory!"
            }
            with open(os.path.join(final_dir, 'status.json'), 'w') as fh:
                json.dump(output, fh, indent=2)
        cleanup(local_dir, request_id=request_id)
        log_completion(final_dir)
    except Exception as e:
        log.exception("Exception occured during aggregation! Marking status as failed.")
        log_failure(e, final_dir)
