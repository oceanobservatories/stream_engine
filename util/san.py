import os
import logging

import numpy
import xray

from engine import app
from util.cass import insert_dataset, get_san_location_metadata, fetch_bin, execution_pool
from util.common import StreamKey, to_xray_dataset, compile_datasets, log_timing

log = logging.getLogger(__name__)

DEPLOYMENT_FORMAT = 'deployment_{:04d}'
NETCDF_ENDING_NAME = '_{:04d}.nc'

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
        with xray.open_dataset(file_name, decode_times=False, mask_and_scale=False) as dataset:
            stream_key, errors = validate_dataset(dataset)
            if stream_key is None:
                return errors
            else:
                result = insert_dataset(stream_key, dataset)
                return result
    except RuntimeError as e:
        log.warn(e)
        return "Error opening netCDF file " + e.message


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
    #get the data and drop duplicates
    cols, data = fetch_bin(stream, data_bin)
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
    directory = os.path.join(nc_directory, DEPLOYMENT_FORMAT.format(deployment))
    if not os.path.exists(directory):
        os.makedirs(directory)
    base =  os.path.join(directory, stream.stream_name + NETCDF_ENDING_NAME)
    index = 0
    while os.path.exists(base.format(index)):
            index += 1
    return base.format(index)


def get_SAN_samples(num_points, location_metadata):
    data_ratio = float(location_metadata.total) / float(num_points)
    if data_ratio < app.config['UI_FULL_RETURN_RATIO'] :
        log.info("SAN: Number of points (%d) / requested  points (%d) is less than %f. Returning all data",
                 location_metadata.total, num_points, app.config['UI_FULL_RETURN_RATIO'])
        to_sample = []
        for data_bin in location_metadata.bin_list:
            to_sample.append((data_bin, location_metadata.bin_information[data_bin][0]))
    elif len(location_metadata.bin_list) > num_points:
        log.info("SAN: Number of bins (%d) greater than number of points (%d). Sampling 1 value from %d bins.",
                 len(location_metadata.bin_list), num_points, len(location_metadata.bin_list))
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


@log_timing(log)
def fetch_nsan_data(stream_key, time_range, num_points=1000, location_metadata=None):
    """
    Given a time range and stream key.  Genereate evenly spaced times over the inverval using data
    from the SAN.
    :param stream_key:
    :param time_range:
    :param num_points:
    :return:
    """
    if location_metadata is None:
        location_metadata = get_san_location_metadata(stream_key, time_range)
    ref_des_dir, dir_string = get_SAN_directories(stream_key, split=True)
    if not os.path.exists(ref_des_dir):
        log.warning("Reference Designator does not exist in offloaded SAN")
        return None
    to_sample = get_SAN_samples(num_points, location_metadata)
    # now get data in the present we are going to start by grabbing first file in the directory with name that matches
    # grab a random amount of particles from that file if they are within the time range.
    missed = 0
    data = []
    next_index = 0
    futures = []
    for time_bin, num_data_points in to_sample:
        direct = dir_string.format(time_bin)
        if os.path.exists(direct):
            # get data from all of the  deployments
            deployments = os.listdir(direct)

            for deployment in deployments:
                full_path = os.path.join(direct, deployment)
                if os.path.isdir(full_path):
                    futures.append(
                        execution_pool.apply_async(get_deployment_data,
                                                   (full_path, stream_key.stream_name, num_data_points, time_range),
                                                   kwds={'index_start': next_index}))
        else:
            missed += num_data_points

    for future in futures:
        new_data = future.get()
        if new_data is None:
            missed += num_data_points
            continue
        count = len(new_data['index'])
        missed += (num_data_points - count)
        data.append(new_data)
        # keep track of the indexes so that the final dataset has unique indices
        next_index += len(new_data['index'])

    log.warn("SAN: Failed to produce {:d} points due to nature of sampling".format(missed))
    return compile_datasets(data)

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
                        next_index += len(new_data['index'])
    if len(data) == 0:
        return None
    return xray.concat(data, dim='index')


@log_timing(log)
def get_deployment_data(direct, stream_name, num_data_points, time_range, index_start=0, forward_slice=True):
    """
    Given a directory of NETCDF files for a deployment
    try to return num_data_points that are valid in the given time range.
    Return them from the first netcdf file in the directory that has the stream name and was offloaded by the SAN
    :param direct: Directory to search for files in
    :param stream_name: Name of the stream
    :param num_data_points: Number of data points to get out of the netcdf file -1 means return all valid in time range
    :param time_range: Time range of the query
    :param forward_slice: Take from the first data point onwards or take from the last data point backwards
    :return: dictionary of data stored in numpy arrays.
    """
    files = os.listdir(direct)
    # Loop until we get the data we want
    for f in files:
        # only netcdf files
        if stream_name in f and os.path.splitext(f)[-1] == '.nc':
            f = os.path.join(direct, f)
            with xray.open_dataset(f, decode_times=False) as dataset:
                out_ds = xray.Dataset(attrs=dataset.attrs)
                t = dataset.time
                t.load()
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
                        if forward_slice:
                            selection = numpy.floor(numpy.linspace(0, len(indexes)-1, num_data_points)).astype(int)
                        else:
                            selection = numpy.floor(numpy.linspace(len(indexes)-1, 0, num_data_points)).astype(int)
                        selection = indexes[selection]
                        selection = sorted(selection)
                    idx = [x for x in range(index_start, index_start + len(selection))]
                    for var_name in dataset.variables.keys():
                        if var_name in dataset.coords:
                            continue
                        var = dataset[var_name][selection]
                        out_ds.update({var_name : var})
                    # set the index here
                    out_ds['index'] = idx
                    out_ds.load()
                    return out_ds
    return None


def get_sample_numbers(bins, points):
    num = points /bins
    total = bins * num
    remainder = points - total
    vals = [num for _ in range(bins)]
    increment = numpy.floor(numpy.linspace(0, bins-1, remainder)).astype(int)
    for i in increment:
        vals[i] += 1
    return vals


def get_san_lookback_dataset(stream_key, time_range, data_bin, deployments):
    """
    Get a length 1 dataset with the first value in the given data bin in the given time range from the SAN.
    :param stream_key:
    :param time_range:
    :param data_bin:
    :return:
    """
    datasets = []
    ref_des_dir, dir_string = get_SAN_directories(stream_key, split=True)
    if not os.path.exists(ref_des_dir):
        log.warning("Reference Designator does not exist in offloaded SAN")
        return None
    direct = dir_string.format(data_bin)
    deployment_dirs = os.listdir(direct)
    for deployment in deployments:
        # get the last deployment.  We are assuming that if there is more than one deployment
        # the last is the one wanted since we are padding forward.
        # get the correct deployment or return none
        dep_direct = DEPLOYMENT_FORMAT.format(deployment)
        if dep_direct in deployment_dirs:
            dep_direct = os.path.join(direct, dep_direct)
            datasets.append(get_deployment_data(dep_direct, stream_key.stream.name, 1, time_range, forward_slice=False, index_start=0))
        else:
            log.warn("Could not find deployment for lookback dataset.")
            datasets.append(None)
    return compile_datasets(datasets)
