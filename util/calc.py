import engine

from Queue import Queue, Empty
import importlib
import json
import struct
import tempfile
import netCDF4
from threading import Event
import traceback
import numexpr
import numpy
import time
from werkzeug.exceptions import abort
from engine import app
from collections import OrderedDict
from util.cass import get_streams, get_distinct_sensors, fetch_nth_data, fetch_data_sync
from util.common import log_timing, StreamKey, TimeRange, CachedParameter, UnknownEncodingException, \
    FUNCTION, CoefficientUnavailableException, parse_pdid, UnknownFunctionTypeException, \
    CachedStream, StreamEngineException, StreamUnavailableException, InvalidStreamException, arb
from util import common
from util import chunks
from util.chunks import Chunk
import pandas as pd
import uuid
import bisect
import scipy as sp
import logging

log = logging.getLogger(__name__)

@log_timing
def get_particles(streams, start, stop, coefficients, limit=None, custom_times=None, custom_type=None):
    """
    Returns a list of particles from the given streams, limits and times
    """
    stream_keys = [StreamKey.from_dict(d) for d in streams]
    parameters = []
    for s in streams:
        for p in s.get('parameters', []):
            parameters.append(CachedParameter.from_id(p))
    time_range = TimeRange(start, stop)

    #if custom_times is None and limit is not None:
        #custom_times = numpy.linspace(start, stop, num=limit)

    stream_request = StreamRequest(stream_keys, parameters, coefficients, time_range, limit=limit, times=custom_times)

    pd_data = fetch_pd_data(stream_request, streams, start, stop, coefficients, limit)

    ## convert data into a list of particles
    key = stream_keys[0]
    particles = []
    for index in range(len(pd_data[7][key.as_refdes()]['data'])):
        particle = OrderedDict()

        particle['pk'] = key.as_dict()

        # Add non-param data to particle
        particle['pk']['deployment'] = pd_data['deployment'][key.as_refdes()]['data'][index]
        particle['provenance'] = str(pd_data['provenance'][key.as_refdes()]['data'][index])

        for param in key.stream.parameters:
            if param.id in pd_data:
                #val = pd_data[param.id][key.as_refdes()]['data'][index]
                val = arb(pd_data[param.id])['data'][index]
                if isinstance(val, numpy.ndarray):
                    val = val.tolist()
                particle[param.name] = val
        
        particles.append(particle)

    return json.dumps(particles, indent=2)


@log_timing
def get_netcdf(streams, start, stop, coefficients, limit=None, custom_times=None, custom_type=None):
    """
    Returns a netcdf from the given streams, limits and times
    """
    stream_keys = [StreamKey.from_dict(d) for d in streams]
    parameters = []
    for s in streams:
        for p in s.get('parameters', []):
            parameters.append(CachedParameter.from_id(p))
    time_range = TimeRange(start, stop)
    stream_request = StreamRequest(stream_keys, parameters, coefficients, time_range, limit=limit, times=custom_times)

    min_len, pd_data = fetch_pd_data(stream_request, streams, start, stop, coefficients, limit)
    return create_netcdf(stream_request, pd_data)


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
    stream_request = StreamRequest(stream_keys, parameters, {}, None, needs_only=True)

    stream_list = []
    for sk in stream_request.stream_keys:
        needs = list(sk.needs_cc)
        d = sk.as_dict()
        d['coefficients'] = needs
        stream_list.append(d)
    return stream_list




def fetch_pd_data(stream_request, streams, start, stop, coefficients, limit):
    """
    Fetches all parameters from the specified streams, calculates the dervived products,
    and returns the parameters in a map

    example return value:
    {'7':
        {u'CP02PMUO-WFP01-03-CTDPFK000-telemetered-ctdpf_ckl_wfp_instrument': 
            {'source': u'CP02PMUO-WFP01-03-CTDPFK000-telemetered-ctdpf_ckl_wfp_instrument', 'data': array([...]) }},
    '7':
        {u'CP02PMUO-WFP01-03-CTDPFK000-telemetered-ctdpf_ckl_wfp_instrument': 
            {'source': u'CP02PMUO-WFP01-03-CTDPFK000-telemetered-ctdpf_ckl_wfp_instrument', 'data': array([...]) }},
    }
    """
    stream_keys = [StreamKey.from_dict(d) for d in streams]


    time_range = TimeRange(start, stop)

    log.info("Pulling data from {} stream(s)".format(len(stream_request.stream_keys)))

    pd_data = {}
    first_time = None
    for key_index, key in enumerate(stream_request.stream_keys):
        if limit:
            fields, cass_data = fetch_nth_data(key, time_range, num_points=limit)
        else:
            fields, cass_data = fetch_nth_data(key, time_range)

        df = pd.DataFrame(cass_data, columns=fields)

        ## transform data from cass and put it into pd_data
        parameters = [p for p in key.stream.parameters if p.parameter_type != FUNCTION]


        # save first time for interpolation later
        if key_index == 0:
            first_time = df['time'].values

        for param in parameters:
            data_slice = df[param.name].values

            # transform non scalar params
            shape_name = param.name + '_shape'
            if shape_name in fields:
                shape = [len(cass_data)] + df[shape_name][0]
                if param.value_encoding == 'string':
                    temp = [item for sublist in data_slice for item in sublist]
                    data_slice = numpy.array(temp).reshape(shape)
                else:
                    data_slice = handle_byte_buffer(''.join(data_slice), param.value_encoding, shape)

            # Nones can only be in ndarrays with dtype == object.  NetCDF
            # doesn't like objects.  First replace Nones with the
            # appropriate fill value.
            nones = numpy.equal(data_slice, None)
            if numpy.any(nones):
                data_slice[nones] = param.fill_value

            # Pandas also treats strings as objects.  NetCDF doesn't
            # like objects.  So convert objects to strings.
            if data_slice.dtype == object:
                data_slice = data_slice.astype('str')

            if key_index != 0:
                ## perform interpolation
                try:
                    float(data_slice[0]) # check that data can be interpolated
                    data_slice = sp.interp(first_time, df['time'].values, data_slice)
                except ValueError:
                    # data_slice cannot be interpolated
                    pass

            if param.id not in pd_data:
                pd_data[param.id] = {}
            pd_data.get(param.id, {})[key.as_refdes()] = {
                'data': data_slice,
                'source': key.as_refdes()
            }

            ## Add non-param data to particle
            if 'provenance' not in pd_data:
                pd_data['provenance'] = {}
            pd_data['provenance'][key.as_refdes()] = {
                'data': df.provenance.values,
                'source': key.as_refdes()
            }

            if 'deployment' not in pd_data:
                pd_data['deployment'] = {}
            pd_data['deployment'][key.as_refdes()] = {
                'data': df.deployment.values,
                'source': key.as_refdes()
            }

    ## exec dpa on chunk
    for param in key.stream.parameters:
        if param.id not in pd_data:
            if param.parameter_type == FUNCTION:
                # calculate inserts results into pd_data
                calculate_derived_product(param, stream_request.coefficients, pd_data, key)
            else:
                log.warning("Required parameter not present: {}".format(param.name))

    return pd_data


def handle_byte_buffer(data, encoding, shape):
    if encoding in ['int8', 'int16', 'int32', 'uint8', 'uint16']:
        format_string = 'i'
        count = len(data) / 4
    elif encoding in ['uint32', 'int64']:
        format_string = 'q'
        count = len(data) / 8
    elif 'float' in encoding:
        format_string = 'd'
        count = len(data) / 8
    else:
        raise UnknownEncodingException(encoding)

    data = numpy.array(struct.unpack('>%d%s' % (count, format_string), data))
    data = data.reshape(shape)
    return data


def calculate_derived_product(param, coeffs, pd_data, original_key, level=1):
    """
    Calculates a derived product by (recursively) calulating its (sub) derived products.

    The products are added to the pd_data map, not returned
    """
    log.info("\n"+((level-1)*4-1)*' ')
    spaces = level*4*' '
    log.info("{}Running dpa for {}{{".format((level-1)*4*' ', param.name))

    
    needs = [CachedParameter.from_id(p) for p in param.needs if p not in pd_data.keys()]

    #import IPython
    #IPython.embed()

    if param in needs:
        needs.remove(param)

    for need in needs:
        # this should descend through any L2 functions to
        # calculate the underlying L1 functions first
        if need.parameter_type == FUNCTION:
            calculate_derived_product(need, coeffs, pd_data, original_key, level+1)

        # we may have already inserted this during recursion
        if need.id not in pd_data:
            #TODO: get required param here
            log.info("{}Skipping required param: {}".format(spaces, need.name))

    try:
        args = build_func_map(param, pd_data, coeffs, spaces)
        data = execute_dpa(param, args)
    except StreamEngineException as e:
        log.info(e.message)
    else:
        if param.id not in pd_data:
            pd_data[param.id] = {}
        pd_data[param.id][original_key] = {'data': data, 'source': 'derived'}

        log.info(spaces+"returning data")

    log.info("{}}}".format((level-1)*4*' '))


def execute_dpa(parameter, kwargs):
    """
    Executes a derived product algorithm for a specific parameter and arguments
    """
    func = parameter.parameter_function
    func_map = parameter.parameter_function_map

    if len(kwargs) == len(func_map):
        if func.function_type == 'PythonFunction':
            module = importlib.import_module(func.owner)

            result = None
            try:
                result = getattr(module, func.function)(**kwargs)
            except Exception, e:
                raise StreamEngineException('DPA threw exception: %s' % e)
        elif func.function_type == 'NumexprFunction':
            result = numexpr.evaluate(func.function, kwargs)
        else:
            raise UnknownFunctionTypeException(func.function_type)
        return result


def build_func_map(parameter, pd_data, coefficients, spaces=""):
    """
    Builds a map of arguments to be passed to an algorithm
    """
    func_map = parameter.parameter_function_map
    args = {}

    times = arb(pd_data[7])['data']
    for key in func_map:
        if str(func_map[key]).startswith('PD'):
            pdid = parse_pdid(func_map[key])

            if pdid not in pd_data:
                raise StreamEngineException(spaces+'Needed parameter PD%s not found in pd_data when calculating PD%s %s' % (pdid, parameter.id, parameter.name))

            args[key] = arb(pd_data[pdid])['data']

        elif str(func_map[key]).startswith('CC'):
            name = func_map[key]
            if name in coefficients:
                framed_CCs = coefficients[name]
                CC_argument = build_CC_argument(framed_CCs, times)
                if(numpy.isnan(numpy.min(CC_argument))):
                    raise CoefficientUnavailableException(spaces+'Coefficient %s missing times in range (%s, %s) for PD%s %s' % (name, times[0], times[-1], parameter.id, parameter.name))
                else:
                    args[key] = CC_argument
            else:
                raise CoefficientUnavailableException(spaces+'Coefficient %s not provided for PD%s %s' % (name, parameter.id, parameter.name))
        elif isinstance(func_map[key], (int, float, long, complex)):
            args[key] = func_map[key]
        else:
            raise StreamEngineException('Unable to resolve parameter \'%s\' in PD%s %s' % (func_map[key], parameter.id, parameter.name))
    return args



def build_CC_argument(frames, times):
    """
    Builds the calibration constant argument for an algorithm
    """
    sample_value = frames[0]['value']
    if(type(sample_value) == list) :
        cc = numpy.empty(times.shape + numpy.array(sample_value).shape)
    else:
        cc = numpy.empty(times.shape)
    cc[:] = numpy.NAN
    
    frames = [(f.get('start'), f.get('stop'), f['value']) for f in frames]
    frames.sort()

    for frame in frames[::-1]:
        mask = in_range(frame, times)
        cc[mask] = frame[2]

    return cc


def in_range(frame, times):
    """
    Returns boolean masking array for times in range.

      frame is a tuple such that frame[0] is the inclusive start time and
      frame[1] is the exclusive stop time.  None for any of these indices
      indicates unbounded.

      times is a numpy array of ntp times.

      returns a bool numpy array the same shape as times
    """
    if(frame[0] is None and frame[1] is None):
        mask = numpy.ones(times.shape, dtype=bool)
    elif(frame[0] is None):
        mask = (times < frame[1])
    elif(frame[1] is None):
        mask = (times >= frame[0])
    elif(frame[0] == frame[1]):
        mask = (times == frame[0])
    else:
        mask = numpy.logical_and(times >= frame[0], times < frame[1])
    return mask


class StreamRequest(object):
    """
    Stores the information from a request, and calculates the required
    parameters and their streams (if needed)
    """

    def __init__(self, stream_keys, parameters, coefficients, time_range, needs_only=False, limit=None, times=None):
        self.stream_keys = stream_keys
        self.time_range = time_range
        self.parameters = parameters
        self.coefficients = coefficients
        self.needs_cc = None
        self.needs_params = None
        self.limit = limit
        self.times = times
        self._initialize(needs_only)

    def _initialize(self, needs_only):
        if len(self.stream_keys) == 0:
            abort(400)

        # no duplicates allowed
        handled = []
        for key in self.stream_keys:
            if key in handled:
                abort(400)
            handled.append(key)

        # populate self.parameters if empty or None
        if self.parameters is None or len(self.parameters) == 0:
            self.parameters = set()
            for each in self.stream_keys:
                self.parameters = self.parameters.union(each.stream.parameters)

        # sort parameters by name for particle output
        self.parameters = sorted(self.parameters, key=lambda x: x.name)


        needs = set()
        for each in self.parameters:
            if each.parameter_type == FUNCTION:
                needs = needs.union([p for p in each.needs if p not in self.parameters])

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
            sensor1, stream1 = find_stream(self.stream_keys[0], streams, distinct_sensors)

            if not any([sensor1 is None, stream1 is None]):
                new_stream_key = StreamKey.from_stream_key(self.stream_keys[0], sensor1, stream1.name)
                self.stream_keys.append(new_stream_key)
                found = found.union(stream1.parameters)

        found = [p.id for p in found]
        self.found = found

        self.needs_params = needs.difference(found)

        needs_cc = set()
        for sk in self.stream_keys:
            needs_cc = needs_cc.union(sk.needs_cc)

        self.needs_cc = needs_cc.difference(self.coefficients.keys())

        if len(self.needs_params) > 0:
            log.warning("Couldn't find all required params, still need: {}".format(self.needs_params))


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
            return stream_key.sensor, stream_map[stream]

    # check other reference designators in the same family
    for subsite1, node1, sensor in distinct_sensors:
        if subsite1 == stream_key.subsite and node1 == stream_key.node:
            for stream in get_streams(stream_key.subsite, stream_key.node, sensor, stream_key.method):
                if stream in stream_map:
                    return sensor, stream_map[stream]

    return None, None


def create_netcdf(r, pd_data):
    """
    Creates and returns a netcdf file from a map of pd data
    """
    with tempfile.NamedTemporaryFile() as tf:
        with netCDF4.Dataset(tf.name, 'w', format='NETCDF4') as ncfile:
            # set up file level attributes
            ncfile.subsite = r.stream_keys[0].subsite
            ncfile.node = r.stream_keys[0].node
            ncfile.sensor = r.stream_keys[0].sensor
            ncfile.collection_method = r.stream_keys[0].method
            ncfile.stream = r.stream_keys[0].stream.name

            # create the time dimension in the root group
            ncfile.createDimension('time', None)
            # create a derived group
            groups = {
                'derived': ncfile.createGroup('derived')
            }

            variables = {}

            # sometimes we will get duplicate timestamps
            # INITIAL solution is to remove any duplicate timestamps
            # and the corresponding data.

            # create a mask to match only valid INCREASING times
            # we will insert the last valid timestamp from the previous
            # chunk at the beginning of the array
            chunk_times = pd_data[7]['data']
            chunk_valid = numpy.diff(numpy.insert(chunk_times, 0, 0.0)) != 0

            # We will need to keep track of the last timestamp of each chunk so
            # that we can apply this logic across chunks
            last_timestamp = chunk_times[chunk_valid][-1]

            # create the netcdf variables and any extra dimensions
            for param_id in pd_data:
                param = CachedParameter.from_id(param_id)
                # param can be None if this is not a real parameter,
                # like deployment for deployment number
                param_name = param_id if param is None else param.name

                data = pd_data[param_id]['data']
                source = pd_data[param_id]['source']
                if param_id == 7:
                    group = ncfile
                elif param and param.parameter_type == FUNCTION:
                    group = groups['derived']
                else:
                    if source not in groups:
                        groups[source] = ncfile.createGroup(source)
                    group = groups[source]

                dims = ['time']
                if len(data.shape) > 1:
                    for index, dimension in enumerate(data.shape[1:]):
                        name = '%s_dim_%d' % (param_name, index)
                        group.createDimension(name, dimension)
                        dims.append(name)


                data_type = data.dtype
                if data.dtype == 'object' and len(data[chunk_valid]) > 0:
                    # convert uuid to str
                    if isinstance(data[chunk_valid][0], uuid.UUID):
                        data_type = "str"
                        data[chunk_valid] = [str(x) for x in data[chunk_valid]]
                    else:
                        log.error("Unknown object type: {}, skipping".format(type(data[chunk_valid][0])))
                        continue

                variables[param_id] = group.createVariable(param_name,
                                                            data_type,
                                                            dims,
                                                            zlib=True)

                if param:
                    if param.unit is not None:
                        variables[param_id].units = param.unit
                    if param.fill_value is not None:
                        variables[param_id].fill_value = param.fill_value
                    if param.description is not None:
                        variables[param_id].long_name = param.description
                    if param.display_name is not None:
                        variables[param_id].display_name = param.display_name
                    if param.data_product_identifier is not None:
                        variables[param_id].data_product_identifier = param.data_product_identifier

                variables[param_id][:] = data[chunk_valid]

        return tf.read()

