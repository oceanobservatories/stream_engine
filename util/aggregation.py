#!/usr/bin/env python
import codecs
from collections import defaultdict, OrderedDict
import glob
from itertools import chain
import os
import re
import xray
import logging
from engine.routes import app
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
    string_sizes = {}
    arr_sizes = {}
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
            elif len(ds.variables[var].shape) > 1:
                arr_sizes[var] =  tuple(ds.variables[var].shape[1:])
            else:
                arr_sizes[var] = 1
    return ret_val, string_sizes, arr_sizes


def collect_subjob_info(job_direct):
    """
    :param root_dir:  Root directory to start walking path
    :return: Return a dictionary of file names and coordinate sizes
    """
    root_dir = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'], job_direct)
    subjob_info = {}
    string_sizes = {}
    array_sizes = {}
    for direct, subdirs, files in os.walk(root_dir):
        for i in files:
            if i.endswith('.nc'):
                idx = direct.index(job_direct)
                pname = direct[idx+len(job_direct)+1:]
                fname = os.path.join(pname, i)
                nc_info, ss, arrs = get_nc_info(os.path.join(direct, i))
                subjob_info[fname] = nc_info
                # store the size of all string parameters in the nc file
                string_sizes[os.path.join(direct, i)]  = ss
                array_sizes[os.path.join(direct, i)] = arrs

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


    # clean up array size mismatch.
    var_sizes = defaultdict(set)
    for i in array_sizes:
        for v in array_sizes[i]:
            var_sizes[v].add(array_sizes[i][v])
    to_mod = {}
    # check to see if we have any mismatches
    for v in var_sizes:
        if len(var_sizes[v]) > 1:
            to_mod[v] = max(var_sizes[v])
    # if mismatches we need to rewrite the string values
    if len(to_mod) > 0:
        files = string_sizes.keys()
        modify_arrs(files, to_mod)
    return subjob_info

def modify_arrs(files, to_mod):
    for f in files:
        modified = False
        with xray.open_dataset(f, decode_times=False, mask_and_scale=False) as ds:
            for var, size in to_mod.iteritems():
                if ds[var].shape[1:] != size:
                    log.warn("Array size mismatch in file %s for parameter %s", f, var)
                    modified = True
                    new_shape =  ds[var].shape[:1] + size
                    arr = np.empty(new_shape, ds[var].dtype)
                    if '_FillValue' in ds[var].attrs:
                        arr.fill(ds[var].attrs['_FillValue'])
                    else:
                        log.warn("No fill value for %s which had size mismatch", var)
                        arr.fill(-9999)
                    # fix dimensions
                    dims = ['obs']
                    for i in range(len(size)):
                        dims.append("{:s}_dim_{:d}".format(var, i))
                    ds[var] = (dims, arr, ds[var].attrs)
            if modified:
                new_ds = ds.copy(deep=True)
                new_ds.load()
        if modified:
            new_ds.to_netcdf(f)

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
        last_time = 0
        for datafile, info in info_dict.iteritems():
            size += info['size']
            to_open = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'], async_job_dir, datafile)
            # open the dataset
            ds = xray.open_dataset(to_open, decode_times=False)
            time = ds.time.values[0]
            if time == last_time:
                log.warn("Duplicate time dropping first time!")
                ds = drop_first_time(ds)
            last_time = ds.time.values[-1]

            # for each variable that we are going to read later remove it from the dataset
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

        try:
            new_ds = xray.concat(datasets, dim='obs', data_vars='minimal', coords='minimal')
            #add and fix up variables
            new_ds['obs'].values= np.array([x for x in range(new_ds.obs.size)], dtype=np.int32)
            for updated_var, info in variable_dict.iteritems():
                data = info['value']
                npdata = np.array(data)
                new_ds[updated_var] = (updated_var + '_dim0', npdata, {'long_name': updated_var})
            base, _ = os.path.splitext(combined_file)
            new_ds.to_netcdf(base+ '.nc')

            # Now reindex the new dataset based on times!
            drop_obs = xray.Dataset(attrs=new_ds.attrs)
            for i in new_ds.variables:
                if i == 'obs':
                    continue
                dims = new_ds.variables[i].dims
                if dims[0] == 'obs':
                    new_dims = ('time',) + dims[1:]
                    drop_obs[i] = (new_dims, new_ds.variables[i].values, new_ds.variables[i].attrs)
                else:
                    drop_obs[i] = new_ds[i]

            drop_obs.to_netcdf(base + '_time_indexed.nc')
            #As a final step drop lat, lon, depth if they are all the same and put them in the attributes indexes
            no_dups = drop_same_data(drop_obs.copy(deep=True))
            # TEMPORARY WORKAROUND TO GET ADCP sensors in correct format
            # This calcuation should be moved to stream engine proper when time permits.
            if 'ADCP' in no_dups.sensor:
                no_dups = fix_adcp(no_dups)
            no_dups.to_netcdf(base + '_latlon_attr.nc')
            # May need to change attributes here
        except Exception as e:
            log.exception("Exception when aggregating netcdf file for request: %s", async_job_dir)

def fix_adcp(ds):
    # get num cells
    if 'num_cells' in ds.attrs:
        num_cells = ds.num_cells
    elif 'num_cells' in ds.variables:
        num_cells = ds.variables['num_cells']
    else:
        log.warn("No number of cells in ADCP data")
        return ds
    if 'cell_length' in ds.attrs:
        cell_length = ds.cell_length
    elif 'cell_length' in ds.variables:
        cell_length = ds.variables['cell_length']
    else:
        log.warn("No cell_length in ADCP data")
        return ds
    if 'bin_1_distance' in ds.attrs:
        bin_1_distance = ds.bin_1_distance
    elif 'bin_1_distance' in ds.variables:
        bin_1_distance = ds.variables['bin_1_distance']
    else:
        log.warn("No bin_1_distance in ADCP data")
        return ds

    z = np.linspace(0, num_cells-1, num=num_cells) * cell_length + bin_1_distance
    ds['z'] = ('z_dim', z, {'long_name' : 'the distance of the center of the cells to the ADDCP transducer head'})
    return ds



def drop_same_data(ds):
    lats = set(ds.lat.values)
    lons = set(ds.lon.values)
    depths = set(ds.depth.values)
    if len(lats) == 1 and len(lons) == 1:
        # get rid of lat and lon and put in header
        ds.attrs['lat'] = list(lats)[0]
        ds.attrs['lon'] = list(lons)[0]
        del ds['lon']
        del ds['lat']
    if len(depths) == 1:
        ds.attrs['loc_water_pressure'] = list(depths)[0]
        del ds['loc_water_pressure']

        # put depths in the header
    for i in app.config['DATA_TO_ATTRIBUTE_PARTS']:
        if i in ds.sensor:
            for data_var in app.config['DATA_TO_ATTRIBUTE_PARTS'][i]:
                if data_var in ds.variables:
                    ds.attrs[data_var] = ds[data_var].values[0]
                    del ds[data_var]
    return ds

def drop_first_time(ds):
    new_ds = xray.Dataset(attrs=ds.attrs)
    new_cord = {}
    # Set up the new coordinates
    for i in ['time', 'lat', 'lon', 'loc_water_pressure']:
        if ds[i].dims[0] == 'obs':
            a = xray.DataArray(ds[i].values[1:], dims=ds[i].dims, attrs=ds[i].attrs)
            new_cord[i] = a
    # drop the first variable of data values along the obs
    for v in ds.variables:
        if ds[v].dims[0] == 'obs':
            data_arr = xray.DataArray(ds[v].values[1:], coords=new_cord, dims=ds[v].dims, attrs=ds[v].attrs)
            new_ds[v] = data_arr
        else:
            new_ds[v] = ds[v]

    return new_ds


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
    erddap(async_job_dir)

def find_representative_nc_file(file_paths, file_base):
    nc_name = file_base + '.nc'
    agg_nc = os.path.join(file_paths,  nc_name)
    if os.path.exists(agg_nc):
        # return aggregated file and we want to include it.
        return agg_nc, True
    else:
        #walk directory and find first include subfile we do not want to include nc file in output
        for directory, _, files in os.walk(file_paths):
            for f in files:
                if f == nc_name:
                    return os.path.join(directory, f), False

def map_erddap_type(dtype):
        '''
        Returns the ERDDAP data type for a given dtype
        '''
        dtype_map = {
            np.dtype('float64') : 'double',
            np.dtype('float32') : 'float',
            np.dtype('int64')   : 'long',
            np.dtype('int32')   : 'int',
            np.dtype('int16')   : 'short',
            np.dtype('int8')    : 'byte',
            np.dtype('uint64')  : 'unsignedLong',
            np.dtype('uint32')  : 'unsignedInt',
            np.dtype('uint16')  : 'unsignedShort',
            np.dtype('uint8')   : 'unsignedByte',
            np.dtype('bool')    : 'boolean',
            np.dtype('S')       : 'String',
            np.dtype('O')       : 'String',
        }
        if dtype.char == 'S':
            return 'String'

        return dtype_map[dtype]

def get_type_map(nc_file_name, index='obs'):
    data_vars = {}
    with xray.open_dataset(nc_file_name, decode_times=False) as ds:
        for nc_var in ds.variables:
            # if multi variables continue to cause problems we can drop them here if dims > 1
            if ds[nc_var].dims[0] == index:
                data_type = map_erddap_type(ds[nc_var].dtype)
                data_vars[nc_var] = {'dataType': data_type, 'attrs': {}}
                for i in ds[nc_var].attrs:
                    data_vars[nc_var]['attrs'][i] = ds[nc_var].attrs[i]
    return data_vars


def get_template():
    '''
    Returns the XML for the dataset entry in ERDDAP's datasets.xml
    '''
    loader = jinja2.FileSystemLoader(searchpath='templates')
    env = jinja2.Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)
    template = env.get_template('erddap_dataset.jinja')
    return template

def get_attr_dict(nc_file):
    attrs = {}
    with xray.open_dataset(nc_file, decode_times=False) as ds:
        for i in ds.attrs:
            attrs[i] = ds.attrs[i]
    return attrs


def erddap(agg_dir):
    path_to_dataset = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'], agg_dir)
    _, async_job_id = os.path.split(path_to_dataset)
    # get datasets (deployments and nc files) and representative netcdf files and wheter we are doing ncml or just one
    for ncml_file in glob.glob(path_to_dataset + '/*.ncml'):
        _, fname = os.path.split(ncml_file)
        file_base, _ = os.path.splitext(fname)
        nc_file, include = find_representative_nc_file(path_to_dataset, file_base)
        dataset_vars = get_type_map(nc_file)
        attr_dict = get_attr_dict(nc_file)
        template = get_template()
        title = '{:s}_{:s}'.format(async_job_id, file_base)
        with codecs.open(os.path.join(path_to_dataset, file_base+ '_erddap.xml'), 'wb', 'utf-8') as erddap_file:
            erddap_file.write(template.render(dataset_title=title,
                           dataset_id=title,
                           dataset_dir=path_to_dataset,
                           data_vars=dataset_vars,
                           attr_dict=attr_dict,
                           base_file_name=file_base,
                           recursive=not include,
                    ))
        nc_file, include = find_representative_nc_file(path_to_dataset, file_base + '_time_indexed')
        dataset_vars = get_type_map(nc_file, index='time')
        attr_dict = get_attr_dict(nc_file)
        template = get_template()
        title = '{:s}_{:s}'.format(async_job_id, file_base)
        with codecs.open(os.path.join(path_to_dataset, file_base+ '_time_indexed_erddap.xml'), 'wb', 'utf-8') as erddap_file:
            erddap_file.write(template.render(dataset_title=title,
                                              dataset_id=title + '_te',
                                              dataset_dir=path_to_dataset,
                                              data_vars=dataset_vars,
                                              attr_dict=attr_dict,
                                              base_file_name=file_base  + '_time_indexed',
                                              recursive=not include,
                                              ))
        nc_file, include = find_representative_nc_file(path_to_dataset, file_base + '_latlon_attr')
        dataset_vars = get_type_map(nc_file, index='time')
        attr_dict = get_attr_dict(nc_file)
        template = get_template()
        title = '{:s}_{:s}'.format(async_job_id, file_base)
        with codecs.open(os.path.join(path_to_dataset, file_base+ '_latlon_attr_erddap.xml'), 'wb', 'utf-8') as erddap_file:
            erddap_file.write(template.render(dataset_title=title,
                                              dataset_id=title + '_ns',
                                              dataset_dir=path_to_dataset,
                                              data_vars=dataset_vars,
                                              attr_dict=attr_dict,
                                              base_file_name=file_base  + '_latlon_attr',
                                              recursive=not include,
                                              ))
