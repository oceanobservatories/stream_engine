import importlib
import logging
import tempfile
import zipfile
import uuid
import os
from collections import OrderedDict, defaultdict

import numexpr
import numpy
import xray
import pandas as pd
import scipy as sp
import requests

from util.cass import get_streams, get_distinct_sensors, fetch_nth_data, fetch_all_data, fetch_annotations, \
    get_available_time_range, fetch_l0_provenance, time_to_bin, bin_to_time, get_location_metadata, \
    get_san_location_metadata, get_full_cass_dataset, insert_dataset
from util.common import log_timing, ntp_to_datestring,ntp_to_ISO_date, StreamKey, TimeRange, CachedParameter, \
    FUNCTION, CoefficientUnavailableException, UnknownFunctionTypeException, \
    CachedStream, StreamEngineException, CachedFunction, Annotation, \
    MissingTimeException, MissingDataException, MissingStreamMetadataException, get_stream_key_with_param, \
    isfillvalue, InvalidInterpolationException, to_xray_dataset
from parameter_util import PDRef
from engine.routes import  app

import datamodel
from jsonresponse import JsonResponse
import xray_interpolation as xinterp
from datetime import datetime
import time

try:
    import simplejson as json
except ImportError:
    import json

log = logging.getLogger(__name__)

# MONKEY PATCH XRAY - REMOVE WHEN FIXED UPSTREAM
from xray.backends import netcdf3
netcdf3._nc3_dtype_coercions = {'int64': 'int32', 'bool': 'int8'}
# END MONKEY PATCH - REMOVE WHEN FIXED UPSTREAM


@log_timing
def get_particles(streams, start, stop, coefficients, qc_parameters, limit=None, custom_times=None, custom_type=None,
                  include_provenance=False, include_annotations=False, strict_range=False, request_uuid=''):
    """
    Returns a list of particles from the given streams, limits and times
    """
    stream_keys = [StreamKey.from_dict(d) for d in streams]
    parameters = []
    for s in streams:
        for p in s.get('parameters', []):
            parameters.append(CachedParameter.from_id(p))
    time_range = TimeRange(start, stop)

    qc_stream_parameters = {}
    for qc_parameter in qc_parameters:
        qc_pk = qc_parameter['qcParameterPK']
        current_stream_parameter = qc_pk['streamParameter'].encode('ascii', 'ignore')
        qc_ids = qc_stream_parameters.get(current_stream_parameter, None)
        if qc_ids is None:
            qc_ids = {}
            qc_stream_parameters[current_stream_parameter] = qc_ids

        current_qc_id = qc_pk['qcId'].encode('ascii', 'ignore')
        parameter_dict = qc_ids.get(current_qc_id, None)
        if parameter_dict is None:
            parameter_dict = {}
            qc_ids[current_qc_id] = parameter_dict

        current_parameter_name = qc_pk.get('parameter', None)
        if current_parameter_name is not None:
            current_parameter_name = current_parameter_name.encode('ascii', 'ignore')
            qc_parameter_value = qc_parameter['value'].encode('ascii', 'ignore')
            if qc_parameter['valueType'].encode('ascii', 'ignore') == 'INT':
                parameter_dict[current_parameter_name] = int(qc_parameter_value)
            elif qc_parameter['valueType'].encode('ascii', 'ignore') == 'FLOAT':
                parameter_dict[current_parameter_name] = float(qc_parameter_value)
            elif qc_parameter['valueType'].encode('ascii', 'ignore') == 'LIST':
                parameter_dict[current_parameter_name] = [float(x) for x in qc_parameter_value[1:-1].split()]
            else:
                parameter_dict[current_parameter_name] = qc_parameter_value

    # create the store that will keep track of provenance for all streams/datasources
    provenance_metadata = ProvenanceMetadataStore()
    annotation_store = AnnotationStore()
    stream_request = StreamRequest(stream_keys, parameters, coefficients, time_range,
                                   qc_parameters=qc_stream_parameters, limit=limit, times=custom_times,
                                   include_provenance=include_provenance,include_annotations=include_annotations,
                                   strict_range=strict_range)

    # Create the medata store
    provenance_metadata.add_query_metadata(stream_request, request_uuid, 'JSON')
    pd_data = fetch_pd_data(stream_request, streams, start, stop, coefficients, limit, provenance_metadata, annotation_store)

    json_response = JsonResponse(pd_data,
                                 qc_stream_parameters,
                                 provenance_metadata if include_provenance else None,
                                 annotation_store if include_annotations else None)
    stream_0_parameters = [p for p in stream_request.parameters if stream_keys[0].stream.id in p.streams]
    return json_response.json(stream_keys[0], stream_0_parameters)


def onload_netCDF(file_name):
    """
    Put data from the given netCDF back into Cassandra
    :param file_name:
    :return: String message detailing what happend or what went wrong
    """
    # Validate that we have a file that exists
    if not os.path.exists(file_name):
        log.warn("File {:s} does not exist".format(file_name))
        return "File {:s} does not exist".format(file_name)
    # Validate that it has the information that we need and read in the data
    try:
        with xray.open_dataset(file_name, decode_times=False) as dataset:
            stream_key, errors = validate_dataset(dataset)
            if stream_key is None:
                return errors
            else:
                result = insert_dataset(stream_key, dataset)
                return result
    except RuntimeError as e:
        log.warn(e)
        return "Error opening netCDF file " + e.message
    return ''

def validate_dataset(dataset):
    # Validate netcdf file Check to make sure we have the subsite, node, sensor, stream, and collection_method
    errors = ''
    if 'subsite' not in dataset.attrs:
        errors += 'No subsite in netCDF files attributes'
    else:
        subsite = dataset.attrs['subsite']
    if 'node' not in dataset.attrs:
        errors += 'No node in netCDF files attributes'
    else:
        node = dataset.attrs['node']
    if 'sensor' not in dataset.attrs:
        errors += 'No sensor in netCDF files attributes'
    else:
        sensor = dataset.attrs['sensor']
    if 'stream' not in dataset.attrs:
        errors += 'No stream in netCDF files attributes'
    else:
        stream = dataset.attrs['stream']
    if 'collection_method' not in dataset.attrs:
        errors += 'No collection_method in netCDF files attributes'
    else:
        method = dataset.attrs['collection_method']
    if len(errors) > 0:
        return None, errors
    stream_key = StreamKey(subsite, node, sensor, method, stream)
    return stream_key, errors

def SAN_netcdf(streams, bins):
    """
    Dump netcdfs for the stream and bins to the SAN.
    Will return success or list of bins that failed.
    Streams should be a length 1 list of streams
    """
    # Get data from cassandra
    stream = StreamKey.from_dict(streams[0])
    san_dir_string = get_SAN_directories(stream)
    results = []
    message = ''
    for data_bin in bins:
        try:
            res, msg = offload_bin(stream, data_bin, san_dir_string)
            results.append(res)
            message += msg
        except Exception as e:
            log.warn(e)
            results.append(False)
            message = message + '{:d} : {:s}\n'.format(data_bin, e.message)
    return results, message


def offload_bin(stream, data_bin, san_dir_string):
    #get the data and drop dupplicates
    arrays = set([p.name for p in stream.stream.parameters if p.parameter_type != FUNCTION and p.is_array])
    cols, data = fetch_all_data(stream, TimeRange(bin_to_time(data_bin), bin_to_time(data_bin+1)))
    dataset = to_xray_dataset(cols, data, stream, san=True)
    nc_directory = san_dir_string.format(data_bin)
    if not os.path.exists(nc_directory):
        os.makedirs(nc_directory)
    for deployment, deployment_ds in dataset.groupby('deployment'):
        # get a file name and create deployment directory if needed
        nc_file_name = get_nc_filename(stream, nc_directory, deployment)
        log.info('Offloading %s deployment %d to %s - There are  %d particles', str(stream),
                 deployment, nc_file_name, len(deployment_ds['index']))
        # create netCDF file
        deployment_ds.to_netcdf(path=nc_file_name)
    return True, ''



@log_timing
def get_netcdf(streams, start, stop, coefficients, limit=None, custom_times=None, custom_type=None,
               include_provenance=False, include_annotations=False, strict_range=False, request_uuid='', disk_path=None):
    """
    Returns a netcdf from the given streams, limits and times
    """
    stream_keys = [StreamKey.from_dict(d) for d in streams]
    parameters = []
    for s in streams:
        for p in s.get('parameters', []):
            parameters.append(CachedParameter.from_id(p))
    time_range = TimeRange(start, stop)

    # Create the provenance metadata store to keep track of all files that are used
    provenance_metadata = ProvenanceMetadataStore()
    annotation_store = AnnotationStore()
    stream_request = StreamRequest(stream_keys, parameters, coefficients, time_range, limit=limit, times=custom_times,
                                   include_provenance=include_provenance,include_annotations=include_annotations,
                                   strict_range=strict_range)
    provenance_metadata.add_query_metadata(stream_request, request_uuid, "netCDF")

    pd_data = fetch_pd_data(stream_request, streams, start, stop, coefficients, limit, provenance_metadata, annotation_store)

    if len(streams) > 1 and custom_times is None:
        custom_times = datamodel.get_time_data(pd_data, stream_keys[0])[0]
        stream_request.times = custom_times
    return NetCDF_Generator(pd_data, provenance_metadata, annotation_store).chunks(stream_request, disk_path)


@log_timing
def get_needs(streams):
    """
    Returns a list of required calibration constants for a list of streams
    """
    stream_keys = [StreamKey.from_dict(d) for d in streams]
    parameters = []
    for s in streams:
        for p in s.get('parameters', []):
            parameters.append(CachedParameter.from_id(p))
    stream_request = StreamRequest(stream_keys, parameters, {}, None, needs_only=True, include_provenance=False,
                                   include_annotations=False)

    stream_list = []
    for sk in stream_request.stream_keys:
        needs = list(sk.needs_cc)
        d = sk.as_dict()
        d['coefficients'] = needs
        stream_list.append(d)
    return stream_list


def replace_values(data_slice, param):
    '''
    Replace any missing values in the parameter
    :param data_slice: pandas series to replace missing values in
    :param param: Information about the parameter
    :return: data_slice with missing values filled with fill value
    '''
    # Nones can only be in ndarrays with dtype == object.  NetCDF
    # doesn't like objects.  First replace Nones with the
    # appropriate fill value.
    #
    # pandas does some funny things to missing values if the whole column is missing it becomes a None filled object
    # Otherwise pandas will replace floats with Not A Number correctly.
    # Integers are cast as floats and missing values replaced with Not A Number
    # The below case will take care of instances where the whole series is missing or if it is an array or
    # some other object we don't know how to fill.
    if data_slice.dtype == 'object' and not param.is_array:
        nones = numpy.equal(data_slice, None)
        if numpy.any(nones):
            # If there are nones either fill with specific value for ints, floats, string, or throw an error
            if param.value_encoding in ['int', 'uint8', 'uint16', 'uint32', 'uint64', 'int8', 'int16', 'int32', 'int64']:
                data_slice[nones] = -999999999
                data_slice = data_slice.astype('int64')
            elif param.value_encoding in ['float16', 'float32', 'float64', 'float96']:
                data_slice[nones] = numpy.nan
                data_slice = data_slice.astype('float64')
            elif param.value_encoding == 'string':
                data_slice[nones] = ''
                data_slice = data_slice.astype('str')
            else:
                log.error("Do not know how to fill type: {:s}".format(param.value_encoding))
                raise StreamEngineException('Do not know how to fill for data type ' + str(param.value_encoding))
    # otherwise if the returned data is a float we need to check and make sure it is not supposed to be an int
    elif data_slice.dtype == 'float64':
        # Int's are upcast to floats if there is a missing value.
        if param.value_encoding in ['int', 'uint8', 'uint16', 'uint32', 'uint64', 'int8', 'int16', 'int32', 'int64']:
            # We had a missing value because it was upcast
            indexes = numpy.where(numpy.isnan(data_slice))
            data_slice[indexes] = -999999999
            data_slice = data_slice.astype('int64')

    # Pandas also treats strings as objects.  NetCDF doesn't
    # like objects.  So convert objects to strings.
    if data_slice.dtype == object:
        try:
            data_slice = data_slice.astype('str')
        except ValueError as e:
            log.error('Unable to convert {} (PD{}) to string (may be caused by jagged arrays): {}'.format(param.name, param.id, e))
    return data_slice

def get_SAN_directories(stream_key, split=False):
    """
    Get the directory that the stream should be on in the SAN with format postion for data bin
    If split is passed the directory for the reference designator will be returned along with the full path
    with format position for data bin
    :param stream_key: Stream key of the data
    :param split: Return the reference designator directory along with the format string for the netcdf file
    :return: format string (or reference_designator directory and format string)
    """
    ref_des_dir = os.path.join(app.config['SAN_BASE_DIRECTORY'] , stream_key.stream_name ,  stream_key.subsite + '-' + stream_key.node+ '-' + stream_key.sensor)
    if ref_des_dir[-1] != os.sep:
        ref_des_dir = ref_des_dir  + os.sep
    dir_string = os.path.join(ref_des_dir + '{:d}',  stream_key.method)
    if split:
        return ref_des_dir, dir_string
    else:
        return dir_string

def get_nc_filename(stream, nc_directory, deployment):
    """
    Simple method to get the netcdf file name to use and create for a stream from a given directory and deployment
    :param stream: Stream Key
    :param nc_directory: Directory
    :param deployment: Deployment
    :return: Full path to directory and file name to user
    """
    directory = os.path.join(nc_directory, 'deployment_{:04d}'.format(deployment))
    if not os.path.exists(directory):
        os.makedirs(directory)
    base =  os.path.join(directory, stream.stream_name + "_{:04d}.nc")
    index = 0
    while os.path.exists(base.format(index)):
            index += 1
    return base.format(index)


def get_SAN_samples(time_range, num_points, ref_des_dir, location_metadata):
    if location_metadata.total < num_points * 2:
        log.info("SAN: Number of points (%d) less than twice requested (%d). Returning all",
                 location_metadata.total, num_points)
        to_sample = []
        for data_bin in location_metadata.bin_list:
            to_sample.append((data_bin, location_metadata.bin_information[data_bin][0]))
    elif len(location_metadata.bin_list) > num_points:
        log.info("SAN: Number of bins (%d) greater than number of points (%d). Sampling 1 value from %d bins.", )
        selection = numpy.floor(numpy.linspace(0, len(location_metadata.bin_list)-1, num_points)).astype(int)
        bins_to_use = numpy.array(location_metadata.bin_list)[selection]
        to_sample = [(x,1) for x in bins_to_use]
    else:
        log.info("SAN: Sampling %d points from %d bins", num_points, len(location_metadata.bin_list)  )
        bin_counts = get_sample_numbers(len(location_metadata.bin_list), num_points)
        to_sample = []
        for idx, count in enumerate(bin_counts):
            to_sample.append((location_metadata.bin_list[idx], count))
    return to_sample

def fetch_nsan_data(stream_key, time_range, num_points=1000, location_metadata=None):
    """
    Given a time range and stream key.  Genereate evenly spaced times over the inverval using data
    from the SAN.
    :param stream_key:
    :param time_range:
    :param strict_range:
    :param num_points:
    :return:
    """
    if location_metadata is None:
        location_metadata = get_san_location_metadata(stream_key, time_range)
    ref_des_dir, dir_string = get_SAN_directories(stream_key, split=True)
    if not os.path.exists(ref_des_dir):
        log.warning("Reference Designator does not exist in offloaded SAN")
        return None
    to_sample = get_SAN_samples(time_range, num_points, ref_des_dir, location_metadata)
    # now get data in the present we are going to start by grabbing first file in the directory with name that matches
    # grab a random amount of particles from that file if they are within the time range.
    missed = 0
    data = []
    next_index = 0
    for time_bin, num_data_points in to_sample:
        direct = dir_string.format(time_bin)
        if os.path.exists(direct):
            # get data from all of the  deployments
            deployments = os.listdir(direct)
            for deployment in deployments:
                full_path = os.path.join(direct, deployment)
                if os.path.isdir(full_path):
                    new_data = get_deployment_data(full_path, stream_key.stream_name, num_data_points, time_range, index_start=next_index)
                    if new_data is None:
                        missed += num_data_points
                        continue
                    count = len(new_data['index'])
                    missed += (num_data_points - count)
                    data.append(new_data)
                    # keep track of the indexes so that the final dataset has uniqe indicies
                    next_index = next_index + len(new_data['index'])
        else:
            missed += num_data_points
    log.warn("SAN: Failed to produce {:d} points due to nature of sampling".format(missed))
    if len(data) == 0:
        return None
    return  xray.concat(data, dim='index')


def fetch_full_san_data(stream_key, time_range, location_metadata=None):
    """
    Given a time range and stream key.  Genereate all data in the inverval using data
    from the SAN.
    :param stream_key:
    :param time_range:
    :return:
    """
    if location_metadata is None:
        location_metadata = get_san_location_metadata(stream_key, time_range)
    # get which bins we can gather data from
    ref_des_dir, dir_string = get_SAN_directories(stream_key, split=True)
    if not os.path.exists(ref_des_dir):
        log.warning("Reference Designator does not exist in offloaded DataSAN")
        return None
    data = []
    next_index = 0
    for time_bin in location_metadata.bin_list:
        direct = dir_string.format(time_bin)
        if os.path.exists(direct):
            # get data from all of the  deployments
            deployments = os.listdir(direct)
            for deployment in deployments:
                full_path = os.path.join(direct, deployment)
                if os.path.isdir(full_path):
                    new_data = get_deployment_data(full_path, stream_key.stream_name, -1, time_range, index_start=next_index)
                    if new_data is not None:
                        data.append(new_data)
                        # Keep track of indexes so they are unique in the final dataset
                        next_index = next_index + len(new_data['index'])
    if len(data) == 0:
        return None
    return xray.concat(data, dim='index')


def get_deployment_data(direct, stream_name, num_data_points, time_range, index_start=0):
    '''
    Given a directory of NETCDF files for a deployment
    try to return num_data_points that are valid in the given time range.
    Return them from the first netcdf file in the directory that has the stream name and was offloaded by the SAN
    :param direct: Directory to search for files in
    :param stream_name: Name of the stream
    :param num_data_points: Number of data points to get out of the netcdf file -1 means return all valid in time range
    :param time_range: Time range of the query
    :return: dictonary of data stored in numpy arrays.
    '''
    files = os.listdir(direct)
    # Loop until we get the data we want
    okay = False
    for f in files:
        # only netcdf files
        if stream_name in f and os.path.splitext(f)[-1] == '.nc':
            f = os.path.join(direct, f)
            with xray.open_dataset(f, decode_times=False) as dataset:
                out_ds = xray.Dataset(attrs=dataset.attrs)
                t = dataset['time'].values
                # get the indexes to pull out of the data
                indexes = numpy.where(numpy.logical_and(time_range.start <= t, t <= time_range.stop))[0]
                if len(indexes) != 0:
                    # less indexes than data or request for all data ->  get everything
                    if num_data_points < 0:
                        selection = indexes
                    elif num_data_points > len(indexes):
                        selection = indexes
                    else:
                        # do a linear sampling of the data points
                        selection = numpy.floor(numpy.linspace(0, len(indexes)-1, num_data_points)).astype(int)
                        selection = indexes[selection]
                        selection = sorted(selection)
                    idx = [x for x in range(index_start, index_start + len(selection))]
                    for var_name in dataset.variables.keys():
                        # Don't sample on coordinate variables
                        if var_name in dataset.coords and var_name != 'index':
                            continue
                        var = dataset[var_name]
                        var_data = var.values[selection]
                        coords = {k:v for k, v in var.coords.iteritems()}
                        coords['index'] = idx
                        da = xray.DataArray(var_data, coords=coords, dims=var.dims, name=var.name, attrs=var.attrs)
                        out_ds[var_name] = da
                    # set the index here
                    out_ds['index'] = idx
                    return out_ds
    return None


def get_sample_numbers(bins, points):
    num = points /bins
    total = bins * num
    remainder = points - total
    vals = [num for x in range(bins)]
    increment = numpy.floor(numpy.linspace(0, bins-1, remainder)).astype(int)
    for i in increment:
        vals[i] += 1
    return vals


def get_dataset(key, time_range, limit, provenance_metadata):
    cass_locations, san_locations, messages = get_location_metadata(key, time_range)
    provenance_metadata.add_messages(messages)
    # check for no data
    if cass_locations.total + san_locations.total  == 0:
        return None
    san_percent = san_locations.total / float(san_locations.total + cass_locations.total)
    cass_percent = cass_locations.total / float(san_locations.total + cass_locations.total)
    datasets = []
    if san_locations.total > 0:
        # put the range down if we are within the time range
        t1 = time_range.start
        t2 = time_range.stop
        if t1 < san_locations.bin_information[san_locations.bin_list[0]][1]:
            t1 = san_locations.bin_information[san_locations.bin_list[0]][1]
        if t2 > san_locations.bin_information[san_locations.bin_list[-1]][2]:
            t2 = san_locations.bin_information[san_locations.bin_list[-1]][2]
        san_times = TimeRange(t1, t2)
        if limit:
            datasets.append(fetch_nsan_data(key, san_times, num_points=int(limit * san_percent),
                                            location_metadata=san_locations))
        else:
            datasets.append(fetch_full_san_data(key, san_times, location_metadata=san_locations))
    if cass_locations.total > 0:
        t1 = time_range.start
        t2 = time_range.stop
        if t1 < cass_locations.bin_information[cass_locations.bin_list[0]][1]:
            t1 = cass_locations.bin_information[cass_locations.bin_list[0]][1]
        if t2 > cass_locations.bin_information[cass_locations.bin_list[-1]][2]:
            t2 = cass_locations.bin_information[cass_locations.bin_list[-1]][2]
        # issues arise when sending cassandra a query with the exact time range.  Data points at the start and end will
        # be left out of the results.  This is an issue for full data queries.  To compensate for this we add .1 seconds
        # to the given start and end time
        t1 -= .1
        t2 += .1
        cass_times = TimeRange(t1, t2)
        if limit:
            datasets.append(fetch_nth_data(key, cass_times, num_points=int(limit * cass_percent),
                                       location_metadata=cass_locations))
        else:
            datasets.append(get_full_cass_dataset(key, cass_times, location_metadata=cass_locations))

    # Now we need to combined the two dataset.
    # First check too see if time ranges overlap. If they do we need to sort.  If not figure which comes first and append them.
    datasets = filter(None, datasets)
    if cass_locations.total > 0 and san_locations.total > 0 and len(datasets) == 2:
        # update the indexes
        # they do overlap we need to sort
        # fix the indicies
        si = len(datasets[0]['index'])
        new_index = [x for x in range(si, si + len(datasets[1]['index']))]
        datasets[1]['index'] = new_index
        if cass_locations.bin_list[0] < san_locations.bin_list[-1] and cass_locations.bin_list[-1] > san_locations.bin_list[0]:
            # sort the dataset it is out of order
            ds = xray.concat(datasets, dim='index')
            sorted_idx = ds.time.argsort()
            ds = ds.reindex({'index' : sorted_idx})
        else:
            #determine which one goes first and append them together
            if san_locations.bin_list[0] < cass_locations.bin_list[0]:
                ds = xray.concat([datasets[0], datasets[1]], dim='index')
            else:
                ds = xray.concat([datasets[1], datasets[0]], dim='index')
    elif len(datasets) == 1:
        # only one value
        ds = datasets[0]
    else:
        # No data return none so we throw an error
        ds = None
    return ds

def fetch_pd_data(stream_request, streams, start, stop, coefficients, limit, provenance_metadata, annotation_store):
    """
    Fetches all parameters from the specified streams, calculates the dervived products,
    and returns the parameters in a map

    example return value:
    {'7':
        {u'CP02PMUO-WFP01-03-CTDPFK000-telemetered-ctdpf_ckl_wfp_instrument':
            {'source': u'CP02PMUO-WFP01-03-CTDPFK000-telemetered-ctdpf_ckl_wfp_instrument', 'data': array([...]) }},
        {u'CP02PMUO-WFP01-03-CTDPFK000-telemetered-ctdpf_ckl_wfp_instrument_recovered':
            {'source': u'derived', 'data': array([...]) }},
    '1337':
        {u'CP02PMUO-WFP01-03-CTDPFK000-telemetered-ctdpf_ckl_wfp_instrument':
            {'source': u'CP02PMUO-WFP01-03-CTDPFK000-telemetered-ctdpf_ckl_wfp_instrument', 'data': array([...]) }},
    }
    """
    primary_key = stream_request.stream_keys[0]
    time_range = TimeRange(start, stop)
    # fit time ranges to metadata if we can.
    if app.config['COLLAPSE_TIMES']:
        log.warn('Collapsing time range to data: ' + ntp_to_datestring(time_range.start) +
                 " -- " + ntp_to_datestring(time_range.stop))
        for sk in stream_request.stream_keys:
            if not sk.stream.is_virtual:
                data_range = get_available_time_range(sk)
                time_range.start = max(time_range.start, data_range.start)
                time_range.stop = min(time_range.stop, data_range.stop)
        log.warn('Collapsed to : ' + ntp_to_datestring(time_range.start) +
                 "  -- " + ntp_to_datestring(time_range.stop) + '\n')

    log.info("Fetching data from {} stream(s): {}".format(len(stream_request.stream_keys), [x.as_refdes() for x in stream_request.stream_keys]))

    pd_data = {}
    first_time = None
    for key_index, key in enumerate(stream_request.stream_keys):
        annotations = []
        if stream_request.include_annotations:
            annotations = query_annotations(key, time_range)
            annotation_store.add_annotations(annotations)

        if key.stream.is_virtual:
            log.info("Skipping virtual stream: {}".format(key.stream.name))
            continue

        ds = get_dataset(key, time_range, limit, provenance_metadata)
        if ds is None:
            if key == primary_key:
                raise MissingDataException("Query returned no results for primary stream")
            elif primary_key.stream.is_virtual and key.stream in primary_key.stream.source_streams:
                raise MissingDataException("Query returned no results for source stream")
            else:
                continue

        # transform data from cass and put it into pd_data
        parameters = [p for p in key.stream.parameters if p.parameter_type != FUNCTION]

        # save first time for interpolation later
        if first_time is None:
            first_time = ds['time'].values

        deployments = ds.groupby('deployment')
        for dep_num, data_set in deployments:
            for param in parameters:
                data_slice = data_set[param.name].values

                # transform non scalar params
                data_slice = replace_values(data_slice, param)

                if param.id not in pd_data:
                    pd_data[param.id] = {}
                pd_data[param.id][key.as_refdes()] = {
                    'data': data_slice,
                    'source': key.as_dashed_refdes()
                }

            if stream_request.include_provenance:
                for prov_uuid, deployment in zip(data_set.provenance.values.astype(str), data_set.deployment.values):
                    value = (primary_key.subsite, primary_key.node, primary_key.sensor, primary_key.method, deployment, prov_uuid)
                    provenance_metadata.add_metadata(value)

            # Add non-param data to particle
            if 'provenance' not in pd_data:
                pd_data['provenance'] = {}
            pd_data['provenance'][key.as_refdes()] = {
                'data': data_set.provenance.values.astype('str'),
                'source': key.as_dashed_refdes()
            }

            if 'deployment' not in pd_data:
                pd_data['deployment'] = {}
            pd_data['deployment'][key.as_refdes()] = {
                'data': data_set.deployment.values,
                'source': key.as_dashed_refdes()
            }

            if 'id' not in pd_data:
                pd_data['id'] = {}
            pd_data['id'][key.as_refdes()] = {
                'data': data_set.id.values.astype('str'),
                'source': key.as_dashed_refdes()
            }

            if 'bin' not in pd_data:
                pd_data['bin'] = {}
            pd_data['bin'][key.as_refdes()] = {
                'data': data_set.bin.values,
                'source': key.as_dashed_refdes()
            }

    # exec dpa for stream

    # calculate time param first if it's a derived product
    time_param = CachedParameter.from_id(primary_key.stream.time_parameter)
    if time_param.parameter_type == FUNCTION:
        calculate_derived_product(time_param, stream_request.coefficients, pd_data, primary_key, provenance_metadata, stream_request)

        if time_param.id not in pd_data or primary_key.as_refdes() not in pd_data[time_param.id]:
            raise MissingTimeException("Time param is missing from main stream")

    for param in primary_key.stream.parameters:
        if param.id not in pd_data:
            if param.parameter_type == FUNCTION:
                # calculate inserts derived products directly into pd_data
                calculate_derived_product(param, stream_request.coefficients, pd_data, primary_key, provenance_metadata, stream_request)
            else:
                log.warning("Required parameter not present: {}".format(param.name))
        if stream_request.qc_parameters.get(param.name) is not None \
                and pd_data.get(param.id, {}).get(primary_key.as_refdes(), {}).get('data') is not None:
            try:
                _qc_check(stream_request, param, pd_data, primary_key)
            except Exception as e:
                log.error("Unexpected error while running qc functions: {}".format(e.message))

    return pd_data


def _qc_check(stream_request, parameter, pd_data, primary_key):
    qcs = stream_request.qc_parameters.get(parameter.name)
    for function_name in qcs:
        for qcp in qcs[function_name].keys(): #vector qc parameters are the only ones with string values and need populating
            if qcs[function_name][qcp] == 'time': #populate time vectors with particle times
                qcs[function_name][qcp] =  pd_data[primary_key.stream.time_parameter][primary_key.as_refdes()]['data'][:]
            if qcs[function_name][qcp] == 'data': #populate data vectors with particle data
                qcs[function_name][qcp] = pd_data[parameter.id][primary_key.as_refdes()]['data']
            if isinstance(qcs[function_name][qcp], basestring):
                for p in stream_request.parameters: #populate stream parameter vectors with data from that stream
                    if p.name.encode('ascii', 'ignore') == qcs[function_name][qcp] and pd_data.get(p.id, {}).get(primary_key.as_refdes(), {}).get('data') is not None:
                        qcs[function_name][qcp] = pd_data[p.id][primary_key.as_refdes()]['data']
                        break

        if 'strict_validation' not in qcs[function_name]:
            qcs[function_name]['strict_validation'] = False

        module = importlib.import_module(CachedFunction.from_qc_function(function_name).owner)

        qc_name = '%s_%s' % (parameter.name.encode('ascii', 'ignore'), function_name)
        if qc_name not in pd_data:
            pd_data[qc_name] = {}
        pd_data[qc_name][primary_key.as_refdes()] = {
            'data': getattr(module, function_name)(**qcs.get(function_name)),
            'source': 'qc'
        }


def interpolate_list(desired_time, data_time, data):
    if len(data) == 0:
        raise InvalidInterpolationException("Can't perform interpolation, data is empty".format(len(data_time), len(data)))

    if len(data_time) != len(data):
        raise InvalidInterpolationException("Can't perform interpolation, time len ({}) does not equal data len ({})".format(len(data_time), len(data)))

    try:
        float(data[0])  # check that data can be interpolated
    except (ValueError, TypeError):
        raise InvalidInterpolationException("Can't perform interpolation, type ({}) cannot be interpolated".format(type(data[0])))
    else:
        mask = numpy.logical_not(isfillvalue(data))
        data_time = numpy.asarray(data_time)[mask]
        data = numpy.asarray(data)[mask]
        if len(data) == 0:
            raise InvalidInterpolationException("Can't perform interpolation, data is empty".format(len(data_time), len(data)))
        return sp.interp(desired_time, data_time, data)


def calculate_derived_product(param, coeffs, pd_data, primary_key, provenance_metadata, stream_request, level=1):
    """
    Calculates a derived product by (recursively) calulating its derived products.

    The products are added to the pd_data map, not returned

    Calculated provenance information added and not returned

    Does *return* a unique calculation id for this calculation.
    """
    log.info("")
    spaces = level * 4 * ' '
    log.info("{}Running dpa for {} (PD{}){{".format(spaces[:-4], param.name, param.id))

    func_map = param.parameter_function_map
    rev_func_map = {v : k for k,v in func_map.iteritems()}
    ref_to_name = {}
    for i in rev_func_map:
        if PDRef.is_pdref(i):
            ref_to_name[PDRef.from_str(i)] = rev_func_map[i]
    this_ref = PDRef(None, param.id)
    needs = [pdref for pdref in param.needs if pdref.pdid not in pd_data.keys()]

    calc_meta = OrderedDict()
    subs = []
    parameters = {}
    functions = {}

    # prevent loops since they are only warned against
    other = set([pdref for pdref in param.needs if pdref != this_ref]) - set(needs)
    if this_ref in needs:
        needs.remove(this_ref)

    for pdref in needs:
        needed_parameter = CachedParameter.from_id(pdref.pdid)
        if needed_parameter.parameter_type == FUNCTION:
            sub_id = calculate_derived_product(needed_parameter, coeffs, pd_data, primary_key, provenance_metadata, stream_request, level + 1)
            # Keep track of all sub function we used.
            if sub_id is not None:
                # only include sub_functions if it is in the function map. Otherwise it is calculated in the sub function
                if pdref in ref_to_name:
                    functions[ref_to_name[pdref]] = [sub_id]
                    subs.append(sub_id)

        else:
            # add a placeholder
            parameters[pdref] = pdref

    # Gather information about all other things that we already have.
    for i in other:
        local_param = CachedParameter.from_id(i.pdid)
        if local_param.parameter_type == FUNCTION:
            # the function has already been calculated so we need to get the subcalculation from metadata..
            sub_calcs = provenance_metadata.calculated_metatdata.get_keys_for_calculated(i)
            # only include sub_functions if it is in the function map. Otherwise it is calculated in the sub function
            if i in ref_to_name:
                functions[ref_to_name[i]] = sub_calcs
                for j in sub_calcs:
                    subs.append(j)
        else:
            parameters[i] = i

    # Determine the parameter's parent stream
    parameter_key = None
    for key in stream_request.stream_keys:
        if param in key.stream.parameters:
            parameter_key = key
            break

    if parameter_key is None:
        parameter_key = primary_key

    calc_id = None
    try:
        args, arg_meta = build_func_map(param, coeffs, pd_data, parameter_key)
        args = munge_args(args, primary_key)
        data, version = execute_dpa(param, args)

        calc_meta['function_name'] = param.parameter_function.function
        calc_meta['function_type'] = param.parameter_function.function_type
        calc_meta['function_version'] = version
        calc_meta['function_id'] = param.parameter_function.id
        calc_meta['function_owner'] = param.parameter_function.owner
        calc_meta['argument_list'] = [arg for arg in param.parameter_function_map]
        calc_meta['sub_calculations'] = subs
        calc_meta['arguments'] = {}
        for k, v in arg_meta.iteritems():
            if k in functions:
                v['type'] = 'sub_calculation'
                v['source'] = functions[k]
                calc_meta['arguments'][k] = v
            else:
                calc_meta['arguments'][k] = v
    except StreamEngineException as e:
        #build an error dictonary to pass to computed provenance.
        error_info = e.payload
        if error_info is None:
            provenance_metadata.calculated_metatdata.errors.append(e.message)
        else:
            target_param = error_info.pop('parameter')
            error_info['derived_id'] = target_param.id
            error_info['derived_name'] = target_param.name
            error_info['derived_display_name'] = target_param.display_name
            if 'pdRef' in error_info:
                pdRef = error_info.pop('pdRef')
                error_parameter = CachedParameter.from_id(pdRef.pdid)
                error_info['missing_id'] = error_parameter.id
                error_info['missing_name'] = error_parameter.name
                error_info['missing_display_name'] = error_parameter.display_name
                error_info['missing_possible_stream_names'] = [CachedStream.from_id(s).name for s in error_parameter.streams]
                error_info['missing_possible_stream_ids'] = [s for s in error_parameter.streams]

            error_info['message'] = e.message
            provenance_metadata.calculated_metatdata.errors.append(error_info)

        log.info("{}aborting - {}".format(spaces, e.message))
    else:
        if param.id not in pd_data:
            pd_data[param.id] = {}

        if not isinstance(data, (list, tuple, numpy.ndarray)):
            data = [data]


        pd_data[param.id][parameter_key.as_refdes()] = {'data': data, 'source': 'derived'}
        calc_id = provenance_metadata.calculated_metatdata.insert_metadata(param, this_ref, calc_meta)

        log.info("{}returning data (t: {}, l: {})".format(spaces, type(data[0]).__name__, len(data)))
    log.info("{}}}".format(spaces[:-4]))

    return calc_id

def munge_args(args, primary_key):
    """
    Munges algorithm arguments, most of this should be preload/algorithm changes
    """
    # jcool and jwarm must *always* be 1
    if "jcool" in args:
        args["jcool"] = 1

    if "jwarm" in args:
        args["jwarm"] = 1

    # zwindsp, ztmpwat, ztmpair, zhumair should be scalars when calculating metbk
    if "zwindsp" in args:
        args["zwindsp"] = args["zwindsp"][0]

    if "ztmpwat" in args:
        args["ztmpwat"] = args["ztmpwat"][0]

    if "ztmpair" in args:
        args["ztmpair"] = args["ztmpair"][0]

    if "zhumair" in args:
        args["zhumair"] = args["zhumair"][0]
    return args


def execute_dpa(parameter, kwargs):
    """
    Executes a derived product algorithm
    """
    func = parameter.parameter_function
    func_map = parameter.parameter_function_map

    version = 'unversioned'
    if len(kwargs) == len(func_map):
        if func.function_type == 'PythonFunction':
            module = importlib.import_module(func.owner)

            result = None
            try:
                dpa_function = getattr(module, func.function)
                if hasattr(dpa_function, 'version'):
                    version = dpa_function.version
                result = getattr(module, func.function)(**kwargs)
            except Exception as e:
                to_attach= {'type' : 'FunctionError', "parameter" : parameter, 'function' : str(func.id) + " " + str(func.description)}
                raise StreamEngineException('DPA threw exception: %s' % e, payload=to_attach)
        elif func.function_type == 'NumexprFunction':
            try:
                result = numexpr.evaluate(func.function, kwargs)
            except Exception as e:
                to_attach= {'type' : 'FunctionError', "parameter" : parameter, 'function' : str(func.id) + " " + str(func.description)}
                raise StreamEngineException('Numexpr function threw exception: %s' % e, payload=to_attach)
        else:
            to_attach= {'type' : 'UnkownFunctionError', "parameter" : parameter, 'function' : str(func.function_type)}
            raise UnknownFunctionTypeException(func.function_type, payload=to_attach)
        return result, version

    return None, version


def build_func_map(parameter, coefficients, pd_data, base_key):
    """
    Builds a map of arguments to be passed to an algorithm
    """
    func_map = parameter.parameter_function_map
    args = {}
    arg_metadata = {}

    # find stream that provide each parameter
    ps = defaultdict(dict)
    for key, val in func_map.iteritems():
        if PDRef.is_pdref(val):
            pdRef = PDRef.from_str(val)
            if pdRef.pdid in pd_data:
                for stream in pd_data[pdRef.pdid]:
                    ps[pdRef.pdid][stream] = True



    # find correct time parameter for the main stream
    time_meta = {}
    main_stream_refdes = base_key.as_refdes()
    if base_key.stream.is_virtual:
        # use source stream for time
        time_stream = base_key.stream.source_streams[0]
        time_stream_key = get_stream_key_with_param(pd_data, time_stream, time_stream.time_parameter)
    else:
        time_stream_key = base_key

    if time_stream_key is None:
        to_attach = {'type' : 'TimeMissingError', 'parameter' : parameter}
        raise MissingTimeException("Could not find time parameter for dpa", payload=to_attach)

    time_stream_refdes = time_stream_key.as_refdes()
    if time_stream_key.stream.time_parameter in pd_data and time_stream_refdes in pd_data[time_stream_key.stream.time_parameter]:
        main_times = pd_data[time_stream_key.stream.time_parameter][time_stream_refdes]['data']
        time_meta['type'] = 'time_source'
        time_meta['source'] = pd_data[time_stream_key.stream.time_parameter][time_stream_refdes]['source']
    elif 7 in pd_data and time_stream_refdes in pd_data[7]:
        main_times = pd_data[7][time_stream_refdes]['data']
        time_meta['type'] = 'time_source'
        time_meta['source'] = pd_data[7][time_stream_refdes]['source']
    else:
        to_attach = {'type' : 'TimeMissingError', 'parameter' : parameter}
        raise MissingTimeException("Could not find time parameter for dpa", payload=to_attach)
    time_meta['begin'] = main_times[0]
    time_meta['end'] = main_times[-1]
    time_meta['beginDT'] = ntp_to_datestring(main_times[0])
    time_meta['endDT'] = ntp_to_datestring(main_times[-1])
    arg_metadata['time_source'] = time_meta

    for key in func_map:
        if PDRef.is_pdref(func_map[key]):
            pdRef = PDRef.from_str(func_map[key])
            param_meta = {}
            if pdRef.pdid not in pd_data:
                to_attach = {'type' : 'ParameterMissingError', 'parameter' : parameter, 'pdRef' : pdRef, 'missing_argument_name' : key}
                raise StreamEngineException('Required parameter %s not found in pd_data when calculating %s (PD%s) ' % (func_map[key], parameter.name, parameter.id), payload=to_attach)

            # if pdref has a required stream, try to find it in pd_data
            pdref_refdes = None
            if pdRef.is_fqn():
                required_stream_name = pdRef.stream_name
                for refdes in pd_data[pdRef.pdid]:
                    skey = StreamKey.from_refdes(refdes)
                    if skey.stream_name == required_stream_name:
                        pdref_refdes = refdes
                        break

            if pdref_refdes is None and main_stream_refdes in pd_data[pdRef.pdid]:
                args[key] = pd_data[pdRef.pdid][main_stream_refdes]['data']
                param = CachedParameter.from_id(pdRef.pdid)
                param_meta['type'] = "parameter"
                param_meta['source'] = pd_data[pdRef.pdid][main_stream_refdes]['source']
                param_meta['parameter_id'] = param.id
                param_meta['name'] = param.name
                param_meta['data_product_identifier'] = param.data_product_identifier
                param_meta['iterpolated'] = False
                param_meta['time_begin'] = main_times[0]
                param_meta['time_beginDT'] = ntp_to_datestring(main_times[0])
                param_meta['time_end'] =  main_times[-1]
                param_meta['time_endDT'] =ntp_to_datestring(main_times[-1])
                try:
                    param_meta['deployments'] = list(set(pd_data['deployment'][main_stream_refdes]['data']))
                except:
                    pass
            else:
                # Need to get data from non-main stream, so it must be interpolated to match main stream
                if pdref_refdes is None:
                    data_stream_refdes = next(ps[pdRef.pdid].iterkeys())  # get arbitrary stream that provides pdid
                else:
                    data_stream_refdes = pdref_refdes
                data_stream_key = StreamKey.from_refdes(data_stream_refdes)

                # perform interpolation
                data = pd_data[pdRef.pdid][data_stream_refdes]['data']
                data_time = pd_data[data_stream_key.stream.time_parameter][data_stream_refdes]['data']

                interpolated_data = interpolate_list(main_times, data_time, data)
                args[key] = interpolated_data
                param = CachedParameter.from_id(pdRef.pdid)
                param_meta['type'] = "parameter"
                param_meta['source'] = pd_data[pdRef.pdid][data_stream_refdes]['source']
                param_meta['parameter_id'] = param.id
                param_meta['name'] = param.name
                param_meta['data_product_identifier'] = param.data_product_identifier
                param_meta['iterpolated'] = True
                param_meta['time_begin'] = data_time[0]
                param_meta['time_beginDT'] = ntp_to_datestring(data_time[0])
                param_meta['time_end'] =  data_time[-1]
                param_meta['time_endDT'] =ntp_to_datestring(data_time[-1])
                try:
                    param_meta['deployments'] = list(set(pd_data['deployment'][data_stream_refdes]['data']))
                except:
                    pass
            arg_metadata[key] = param_meta

        elif str(func_map[key]).startswith('CC'):
            name = func_map[key]
            if name in coefficients:
                framed_CCs = coefficients[name]
                # Need to catch exceptions from the call to build the CC argument.
                try:
                    CC_argument, CC_meta = build_CC_argument(framed_CCs, main_times)
                except StreamEngineException as e:
                    to_attach = {
                        'type' : 'CCTimeError', 'parameter' : parameter, 'begin' : main_times[0], 'end' : main_times[-1],
                        'beginDT' : ntp_to_datestring(main_times[0]), 'endDT' : ntp_to_datestring(main_times[-1]),
                        'CC_present' : coefficients.keys(), 'missing_argument_name' : key
                    }
                    raise CoefficientUnavailableException(e.message, payload=to_attach)
                if numpy.isnan(numpy.min(CC_argument)):
                    to_attach = {
                        'type' : 'CCTimeError', 'parameter' : parameter, 'begin' : main_times[0], 'end' : main_times[-1],
                        'beginDT' : ntp_to_datestring(main_times[0]), 'endDT' : ntp_to_datestring(main_times[-1]),
                        'CC_present' : coefficients.keys(), 'missing_argument_name' : key
                                 }
                    raise CoefficientUnavailableException('Coefficient %s missing times in range (%s, %s)' % (name, ntp_to_datestring(main_times[0]), ntp_to_datestring(main_times[-1])), payload=to_attach)
                else:
                    args[key] = CC_argument
                    arg_metadata[key] = CC_meta
            else:
                to_attach = {
                    'type' : 'CCMissingError', 'parameter' : parameter, 'begin' : main_times[0], 'end' : main_times[-1],
                    'beginDT' : ntp_to_datestring(main_times[0]), 'endDT' : ntp_to_datestring(main_times[-1]),
                    'CC_present' : coefficients.keys(), 'missing_argument_name' : key
                }
                raise CoefficientUnavailableException('Coefficient %s not provided' % name, payload=to_attach)
        elif isinstance(func_map[key], (int, float, long, complex)):
            args[key] = func_map[key]
            arg_metadata[key] = {'type' : 'constant', 'value' : func_map[key]}
        else:
            to_attach = {'type' : 'UnknownParameter', 'parameter' : parameter, 'value' : str(func_map[key]),
                         'missing_argument_name' : key}
            raise StreamEngineException('Unable to resolve parameter \'%s\' in PD%s %s' %
                                        (func_map[key], parameter.id, parameter.name), payload=to_attach)
    return args, arg_metadata


def in_range(frame, times):
    """
    Returns boolean masking array for times in range.

      frame is a tuple such that frame[0] is the inclusive start time and
      frame[1] is the exclusive stop time.  None for any of these indices
      indicates unbounded.

      times is a numpy array of ntp times.

      returns a bool numpy array the same shape as times
    """
    times = numpy.array(times)
    if frame[0] is None and frame[1] is None:
        mask = numpy.ones(times.shape, dtype=bool)
    elif frame[0] is None:
        mask = (times < frame[1])
    elif frame[1] is None:
        mask = (times >= frame[0])
    elif frame[0] == frame[1]:
        mask = (times == frame[0])
    else:
        mask = numpy.logical_and(times >= frame[0], times < frame[1])
    return mask


def build_CC_argument(frames, times):
    st = times[0]
    et = times[-1]
    startDt = ntp_to_datestring(st)
    endDt = ntp_to_datestring(et)
    frames = [(f.get('start'), f.get('stop'), f['value'], f['deployment']) for f in frames]
    frames.sort()
    frames = [f for f in frames if any(in_range(f, times))]

    try:
        sample_value = frames[0][2]
    except IndexError, e:
        raise StreamEngineException('Unable to build cc arguments for algorithm: {}'.format(e))

    if type(sample_value) == list:
        cc = numpy.empty(times.shape + numpy.array(sample_value).shape)
    else:
        cc = numpy.empty(times.shape)
    cc[:] = numpy.NAN

    values = []
    for frame in frames[::-1]:
        values.append({'CC_begin' : frame[0], 'CC_stop' : frame[1], 'value' : frame[2], 'deployment' : frame[3],
                       'CC_beginDT' : ntp_to_datestring(frame[0]), 'CC_stopDT' : ntp_to_datestring(frame[1])})
        mask = in_range(frame, times)
        try:
            cc[mask] = frame[2]
        except ValueError, e:
            raise StreamEngineException('Unable to build cc arguments for algorithm: {}'.format(e))
    cc_meta = {
        'sources' : values,
        'data_begin' :  st,
        'data_end' : et,
        'beginDT' : startDt,
        'endDT' : endDt,
        'type' : 'CC',
        }
    return cc, cc_meta


class StreamRequest(object):
    """
    Stores the information from a request, and calculates the required
    parameters and their streams
    """
    def __init__(self, stream_keys, parameters, coefficients, time_range, qc_parameters={}, needs_only=False,
                 limit=None, times=None, include_provenance=False, include_annotations=False, strict_range=False):
        self.stream_keys = stream_keys
        self.time_range = time_range
        self.qc_parameters = qc_parameters if qc_parameters is not None else {}
        self.parameters = parameters
        self.coefficients = coefficients
        self.needs_cc = None
        self.needs_params = None
        self.limit = limit
        self.times = times
        self.include_provenance = include_provenance
        self.include_annotations = include_annotations
        self.strict_range = strict_range
        self._initialize(needs_only)

    def _initialize(self, needs_only):
        if len(self.stream_keys) == 0:
            raise StreamEngineException('Received no stream keys', status_code=400)

        # virtual streams are not in cassandra, so we can't fit the time range
        if not needs_only and not self.stream_keys[0].stream.is_virtual:
            self._fit_time_range()

        # no duplicates allowed
        handled = []
        for key in self.stream_keys:
            if key in handled:
                raise StreamEngineException('Received duplicate stream_keys', status_code=400)
            handled.append(key)

        # populate self.parameters if empty or None
        if self.parameters is None or len(self.parameters) == 0:
            self.parameters = set()
            for each in self.stream_keys:
                self.parameters = self.parameters.union(each.stream.parameters)

        # sort parameters by name for particle output
        self.parameters = sorted(self.parameters, key=lambda x: x.name)

        needs = set()
        for parameter in self.parameters:
            if parameter.parameter_type == FUNCTION:
                needs = needs.union([pdref for pdref in parameter.needs])

        needs_fqn = set([pdref for pdref in needs if pdref.is_fqn()])
        needs = set([pdref.pdid for pdref in needs if not pdref.is_fqn()])

        # available in the specified streams?
        provided = []
        for stream_key in self.stream_keys:
            provided.extend([p.id for p in stream_key.stream.parameters])

        needs = needs.difference(provided)

        distinct_sensors = get_distinct_sensors()

        # find the available streams which provide any needed parameters
        found = set()
        for need in needs:
            need = CachedParameter.from_id(need)
            if need in found:
                continue

            streams = [CachedStream.from_id(sid) for sid in need.streams]
            found_stream_key = find_stream(self.stream_keys[0], streams, distinct_sensors)

            if found_stream_key is not None:
                self.stream_keys.append(found_stream_key)
                found = found.union(found_stream_key.stream.parameters)

        found = [p.id for p in found]
        self.found = found

        self.needs_params = needs.difference(found)

        log.debug('Found FQN needs %s' % (', '.join(map(str, needs_fqn))))

        # find the available streams which provide any needed fqn parameters
        found = set([stream_key.stream_name for stream_key in self.stream_keys])
        for pdref in needs_fqn:
            if pdref.stream_name in found:
                continue
            parameter = CachedParameter.from_id(pdref.pdid)
            streams = [CachedStream.from_id(sid) for sid in parameter.streams]
            streams = [s for s in streams if s.name == pdref.stream_name]

            found_stream_key = find_stream(self.stream_keys[0], streams, distinct_sensors)

            if found_stream_key is not None:
                self.stream_keys.append(found_stream_key)
                found = found.union(found_stream_key.stream.name)

        needs_cc = set()
        for sk in self.stream_keys:
            needs_cc = needs_cc.union(sk.needs_cc)

        self.needs_cc = needs_cc.difference(self.coefficients.keys())

        if len(self.needs_params) > 0:
            log.error('Unable to find needed parameters: %s', self.needs_params)

        if len(self.needs_cc) > 0:
            log.error('Missing calibration coefficients: %s', self.needs_cc)

    def _fit_time_range(self):
        # assumes start <= stop for time ranges
        try:
            available_time_range = get_available_time_range(self.stream_keys[0])
        except IndexError:
            log.info('No stream metadata in cassandra for %s', self.stream_keys[0])
            raise MissingStreamMetadataException('No stream metadata in cassandra for %s' % self.stream_keys[0])
        else:
            if self.time_range.start >= available_time_range.stop or self.time_range.stop <= available_time_range.start:
                log.info('No data in requested time range (%s, %s) for %s ', ntp_to_datestring(self.time_range.start),
                         ntp_to_datestring(self.time_range.stop), self.stream_keys[0])
                raise MissingDataException("No data in requested time range")

            start = max(self.time_range.start, available_time_range.start)
            stop = min(self.time_range.stop, available_time_range.stop)
            log.debug('fit (%s, %s) to (%s, %s) for %s', ntp_to_datestring(self.time_range.start),
                      ntp_to_datestring(self.time_range.stop), ntp_to_datestring(start), ntp_to_datestring(stop),
                      self.stream_keys[0])
            self.time_range = TimeRange(start, stop)


class ProvenanceMetadataStore(object):
    def __init__(self):
        self._prov_set = set()
        self.calculated_metatdata = CalculatedProvenanceMetadataStore()
        self.messages = []

    def add_messages(self, messages):
        self.messages.extend(messages)

    def add_metadata(self, value):
        self._prov_set.add(value)

    def _get_metadata(self):
        return list(self._prov_set)

    def get_provenance_dict(self):
        prov = {}
        for i in self._get_metadata():
            prov_data = fetch_l0_provenance(*i)
            if prov_data:
                fname = prov_data[0][-3]
                pname = prov_data[0][-2]
                pversion = prov_data[0][-1]
                prov[i[-1]] = {}
                prov[i[-1]]['file_name'] = fname
                prov[i[-1]]['parser_name'] = pname
                prov[i[-1]]['parser_version'] = pversion
            else:
                log.info("Received empty provenance row")
        return prov


    def add_query_metadata(self, stream_request, query_uuid, query_type):
        self._query_metadata = OrderedDict()
        self._query_metadata["query_type"] = query_type
        self._query_metadata['query_uuid'] = query_uuid
        self._query_metadata['begin'] = stream_request.time_range.start
        self._query_metadata['beginDT'] = ntp_to_ISO_date(stream_request.time_range.start)
        self._query_metadata['end'] = stream_request.time_range.stop
        self._query_metadata['endDT'] = ntp_to_ISO_date(stream_request.time_range.stop)
        self._query_metadata['limit'] = stream_request.limit
        self._query_metadata["requested_streams"] = [x.as_dashed_refdes() for x in stream_request.stream_keys]
        self._query_metadata["include_provenance"] = stream_request.include_provenance
        self._query_metadata["include_annotations"] = stream_request.include_annotations
        self._query_metadata["strict_range"] = stream_request.strict_range


    def get_query_dict(self):
        return self._query_metadata


class CalculatedProvenanceMetadataStore(object):
    """Metadata store for provenance values"""

    def __init__(self):
        self.params = defaultdict(list)
        self.calls = {}
        self.ref_map = defaultdict(list)
        self.errors = []

    def insert_metadata(self, parameter, ref,  to_insert):
        # check to see if we have a matching metadata call
        # if we do return that id otherwise store it.
        for call in self.params[parameter]:
            if dict_equal(to_insert, self.calls[call]):
                return call
        # create and id and append it to the list
        call_id = str(uuid.uuid4())
        self.calls[call_id] = to_insert
        self.params[parameter].append(call_id)
        self.ref_map[ref].append(call_id)
        return call_id

    def get_dict(self):
        """return dictonary representation"""
        res = {}
        res['parameters'] = {parameter.name : v  for parameter, v in self.params.iteritems()}
        res['calculations'] = self.calls
        res['errors'] = self.errors
        return res

    def get_keys_for_calculated(self, parameter):
        return self.ref_map[parameter]


def dict_equal(d1, d2):
    """Function to recursively check if two dicts are equal"""
    if isinstance(d1, dict) and isinstance(d2, dict):
        # check keysets
        s1 = set(d1.keys())
        s2 = set(d2.keys())
        sd = s1.symmetric_difference(s2)
        # If there is a symetric difference they do not contain the same keys
        if len(sd) > 0:
            return False

        #otherwise loop through all the keys and check if the dicts and items are equal
        for key, value in d1.iteritems():
            if key in d2:
                if not dict_equal(d1[key], d2[key]):
                    return False
        # we made it through
        return True
    # check equality on other objects
    else:
        return d1 == d2

class AnnotationStore(object):
    """
    Handles the storage of annotations during the lifecycle of a request.
    """

    def __init__(self):
        self._store = set()

    def add_annotations(self, anotations):
        for i in anotations:
            self._add_annotation(i)

    def _add_annotation(self, annotation):
        self._store.add(annotation)

    def get_annotations(self):
        return list(self._store)

    def get_json_representation(self):
        ret = [x.as_dict() for x in self._store]
        return ret


def query_annotations(key, time_range):
    '''
    Query edex for annotations on a stream request.
    :param stream_request: Stream request
    :param key: "Key from fetch pd data"
    :param time_range: Time range to use  (float, float)
    :return:
    '''

    # Query Cassandra annotations table
    result = fetch_annotations(key, time_range)

    resultList = []
    # Seconds from NTP epoch to UNIX epoch
    NTP_OFFSET_SECS = 2208988800
    
    for r in result:
        # Annotations columns in order defined in cass.py
        subsite,node,sensor,time1,time2,parameters,provenance,annotation,method,deployment,myid = r

        ref_des = '-'.join([subsite,node,sensor])
        startt = datetime.utcfromtimestamp(time1 - NTP_OFFSET_SECS).isoformat() + "Z"
        endt = datetime.utcfromtimestamp(time2 - NTP_OFFSET_SECS).isoformat()+ "Z"

        # Add to JSON document
        anno = Annotation(ref_des, startt, endt, parameters, provenance, annotation, method, deployment, str(myid))
        resultList.append(anno)

    return resultList


class NetCDF_Generator(object):

    def __init__(self, pd_data, provenance_metadata, annotation_store):
        self.pd_data = pd_data
        self.provenance_metadata = provenance_metadata
        self.annotation_store = annotation_store

    def chunks(self, r, disk_path=None):
        try:
            if disk_path is not None:
                return self.create_raw_files(r, disk_path)
            else:
                return self.create_zip(r)
        except GeneratorExit:
            raise
        except:
            log.exception('An unexpected error occurred.')
            raise

    def create_raw_files(self, r, path):
        strings = list()
        base_path = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'],path)
        # ensure the directory structure is there
        if not os.path.isdir(base_path):
            os.makedirs(base_path)
        for stream_key in r.stream_keys:
            p = self.provenance_metadata if r.include_provenance else None
            a = self.annotation_store if r.include_annotations else None
            ds = datamodel.as_xray(stream_key, self.pd_data, p, a)
            #write file to path
            fn = '%s/%s.nc' % (base_path, stream_key.as_dashed_refdes())
            ds.to_netcdf(fn, format='NETCDF4_CLASSIC')
            strings.append(fn)
        # build json return
        return json.dumps(strings)


    def create_zip(self, r):
        with tempfile.NamedTemporaryFile() as tzf:
            with zipfile.ZipFile(tzf.name, 'w') as zf:
                self.write_to_zipfile(r, zf)
            return tzf.read()

    def write_to_zipfile(self, r, zf):
        for stream_key in r.stream_keys:
            p = self.provenance_metadata if r.include_provenance else None
            a = self.annotation_store if r.include_annotations else None
            ds = datamodel.as_xray(stream_key, self.pd_data, p, a)
            with tempfile.NamedTemporaryFile() as tf:
                if r.times is not None:
                    ds = xinterp.interp1d_Dataset(ds, time=r.times)
                ds.to_netcdf(tf.name, format='NETCDF4_CLASSIC')
                zf.write(tf.name, '%s.nc' % (stream_key.as_dashed_refdes(),))


def find_stream(stream_key, streams, distinct_sensors):
    """
    Attempt to find a "related" sensor which provides one of these streams
    :param stream_key
    :return:
    """
    stream_map = {s.name: s for s in streams}
    # check our specific reference designator first
    for stream in get_streams(stream_key.subsite, stream_key.node, stream_key.sensor, stream_key.method):
        if stream in stream_map:
            return StreamKey.from_dict({
                "subsite": stream_key.subsite,
                "node": stream_key.node,
                "sensor": stream_key.sensor,
                "method": stream_key.method,
                "stream": stream
            })

    # check other reference designators in the same subsite-node
    for subsite1, node1, sensor in distinct_sensors:
        if subsite1 == stream_key.subsite and node1 == stream_key.node:
            for stream in get_streams(stream_key.subsite, stream_key.node, sensor, stream_key.method):
                if stream in stream_map:
                    return StreamKey.from_dict({
                        "subsite": stream_key.subsite,
                        "node": stream_key.node,
                        "sensor": sensor,
                        "method": stream_key.method,
                        "stream": stream
                    })

    # check other reference designators in the same subsite
    for subsite1, node, sensor in distinct_sensors:
        if subsite1 == stream_key.subsite:
            for stream in get_streams(stream_key.subsite, node, sensor, stream_key.method):
                if stream in stream_map:
                    return StreamKey.from_dict({
                        "subsite": stream_key.subsite,
                        "node": node,
                        "sensor": sensor,
                        "method": stream_key.method,
                        "stream": stream
                    })
    return None
