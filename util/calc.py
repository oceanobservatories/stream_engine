import importlib
import logging
import tempfile
from threading import Event, Lock
import traceback
import zipfile

import msgpack
import netCDF4
import numexpr
import numpy
import xray

from util.cass import get_streams, get_distinct_sensors, fetch_nth_data, \
    get_available_time_range, fetch_l0_provenance
from util.common import log_timing, ntp_to_datestring, StreamKey, TimeRange, CachedParameter, \
    FUNCTION, CoefficientUnavailableException, UnknownFunctionTypeException, \
    CachedStream, StreamEngineException, CachedFunction, \
    MissingTimeException, MissingDataException, arb, MissingStreamMetadataException, get_stream_key_with_param
from parameter_util import PDRef

from collections import OrderedDict, defaultdict
from werkzeug.exceptions import abort

import pandas as pd
import scipy as sp


try:
    import simplejson as json
except ImportError:
    import json

log = logging.getLogger(__name__)


@log_timing
def get_particles(streams, start, stop, coefficients, qc_parameters, limit=None, custom_times=None, custom_type=None, include_provenance=False, strict_range=False):
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
            else:
                parameter_dict[current_parameter_name] = qc_parameter_value

    # create the store that will keep track of provenance for all streams/datasources
    provenance_metadata = ProvenanceMetadataStore()
    stream_request = StreamRequest(stream_keys, parameters, coefficients, time_range,
                                   qc_parameters=qc_stream_parameters, limit=limit, times=custom_times, include_provenance=include_provenance,
                                   strict_range=strict_range)

    pd_data = fetch_pd_data(stream_request, streams, start, stop, coefficients, limit, provenance_metadata)

    # convert data into a list of particles
    primary_key = stream_keys[0]
    particles = []

    time_param = primary_key.stream.time_parameter
    if time_param not in pd_data:
        raise MissingTimeException("Time param: {} is missing from the primary stream".format(time_param))

    for index in range(len(pd_data[time_param][primary_key.as_refdes()]['data'])):
        particle = OrderedDict()

        if not primary_key.stream.is_virtual:
            particle['pk'] = primary_key.as_dict()

            # Add non-param data to particle
            particle['pk']['deployment'] = pd_data['deployment'][primary_key.as_refdes()]['data'][index]
            particle['pk']['time'] = pd_data[primary_key.stream.time_parameter][primary_key.as_refdes()]['data'][index]
            particle['provenance'] = str(pd_data['provenance'][primary_key.as_refdes()]['data'][index])

        for param in stream_request.parameters:
            if param.id in pd_data:
                val = None
                try:
                    val = pd_data[param.id][primary_key.as_refdes()]['data'][index]
                except Exception as e:
                    log.info("Failed to get data for {}: {}".format(param.id, e))
                    continue

                if isinstance(val, numpy.ndarray):
                    val = val.tolist()

                particle[param.name] = val

            # add qc results to particle
            for qc_function_name in qc_stream_parameters.get(param.name, []):
                qc_function_results = '%s_%s' % (param.name, qc_function_name)

                if qc_function_results in pd_data:
                    value = pd_data[qc_function_results][primary_key.as_refdes()]['data'][index]

                    qc_results_key = '%s_%s' % (param.name, 'qc_results')
                    if qc_results_key not in particle:
                        particle[qc_results_key] = 0b0000000000000000

                    qc_results_value = particle[qc_results_key]
                    qc_cached_function = CachedFunction.from_qc_function(qc_function_name)
                    qc_results_mask = int(qc_cached_function.qc_flag, 2)

                    if value == 0:
                        qc_results_value = ~qc_results_mask & qc_results_value
                    elif value == 1:
                        qc_results_value = qc_results_mask ^ qc_results_value

                    particle[qc_results_key] = qc_results_value

        particles.append(particle)

    if include_provenance:
        out = OrderedDict()
        out['data'] = particles
        out['provenance'] = provenance_metadata.get_provenance_dict()
    else:
        out = particles

    return json.dumps(out, indent=2)


@log_timing
def get_netcdf(streams, start, stop, coefficients, limit=None, custom_times=None, custom_type=None, include_provenance=False, strict_range=False):
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
    stream_request = StreamRequest(stream_keys, parameters, coefficients, time_range, limit=limit,
                                   times=custom_times, include_provenance=include_provenance, strict_range=strict_range)

    pd_data = fetch_pd_data(stream_request, streams, start, stop, coefficients, limit, provenance_metadata)
    return NetCDF_Generator(pd_data, provenance_metadata).chunks(stream_request)


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
    stream_request = StreamRequest(stream_keys, parameters, {}, None, needs_only=True, include_provenance=False)

    stream_list = []
    for sk in stream_request.stream_keys:
        needs = list(sk.needs_cc)
        d = sk.as_dict()
        d['coefficients'] = needs
        stream_list.append(d)
    return stream_list


def fetch_pd_data(stream_request, streams, start, stop, coefficients, limit, provenance_metadata):
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

    log.info("Fetching data from {} stream(s): {}".format(len(stream_request.stream_keys), [x.as_refdes() for x in stream_request.stream_keys]))

    pd_data = {}
    first_time = None
    for key_index, key in enumerate(stream_request.stream_keys):
        if key.stream.is_virtual:
            log.info("Skipping virtual stream: {}".format(key.stream.name))
            continue

        if limit:
            fields, cass_data = fetch_nth_data(key, time_range, strict_range=stream_request.strict_range, num_points=limit)
        else:
            fields, cass_data = fetch_nth_data(key, time_range, strict_range=stream_request.strict_range)

        if len(cass_data) == 0:
            log.warning("Query for {} returned no data".format(key.as_refdes()))
            if key == primary_key:
                raise MissingDataException("Query returned no results for primary stream")
            elif primary_key.stream.is_virtual and key.stream in primary_key.stream.source_streams:
                raise MissingDataException("Query returned no results for source stream")
            else:
                continue

        df = pd.DataFrame(cass_data, columns=fields)

        # transform data from cass and put it into pd_data
        parameters = [p for p in key.stream.parameters if p.parameter_type != FUNCTION]

        # save first time for interpolation later
        if first_time is None:
            first_time = df['time'].values

        deployments = df.groupby('deployment')
        for dep_num, data_frame in deployments:
            for param in parameters:
                data_slice = data_frame[param.name].values

                # transform non scalar params
                if param.is_array:
                    data_slice = numpy.array([msgpack.unpackb(x) for x in data_slice])

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

                if param.id not in pd_data:
                    pd_data[param.id] = {}
                pd_data[param.id][key.as_refdes()] = {
                    'data': data_slice,
                    'source': key.as_dashed_refdes()
                }

            if stream_request.include_provenance:
                for prov_uuid, deployment in zip(data_frame.provenance.values.astype(str), data_frame.deployment.values):
                    value = (primary_key.subsite, primary_key.node, primary_key.sensor, primary_key.method, deployment, prov_uuid)
                    provenance_metadata.add_metadata(value)

            # Add non-param data to particle
            if 'provenance' not in pd_data:
                pd_data['provenance'] = {}
            pd_data['provenance'][key.as_refdes()] = {
                'data': data_frame.provenance.values.astype('str'),
                'source': key.as_dashed_refdes()
            }

            if 'deployment' not in pd_data:
                pd_data['deployment'] = {}
            pd_data['deployment'][key.as_refdes()] = {
                'data': data_frame.deployment.values,
                'source': key.as_dashed_refdes()
            }

    # exec dpa for stream

    # calculate time param first if it's a derived product
    time_param = CachedParameter.from_id(primary_key.stream.time_parameter)
    if time_param.parameter_type == FUNCTION:
        calculate_derived_product(time_param, stream_request.coefficients, pd_data, primary_key)

        if time_param.id not in pd_data or primary_key.as_refdes() not in pd_data[time_param.id]:
            raise MissingTimeException("Time param is missing from main stream")

    for param in primary_key.stream.parameters:
        if param.id not in pd_data:
            if param.parameter_type == FUNCTION:
                # calculate inserts derived products directly into pd_data
                calculate_derived_product(param, stream_request.coefficients, pd_data, primary_key)
            else:
                log.warning("Required parameter not present: {}".format(param.name))

            if stream_request.qc_parameters.get(param.name) is not None:
                _qc_check(stream_request, param, pd_data, primary_key)

    return pd_data


def _qc_check(stream_request, parameter, pd_data, primary_key):

    qcs = stream_request.qc_parameters.get(parameter.name)
    for function_name in qcs:
        if 'strict_validation' not in qcs[function_name]:
            qcs[function_name]['strict_validation'] = 'False'

        qcs[function_name]['dat'] = pd_data[parameter.id][primary_key.as_refdes()]['data']
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
        return data

    try:
        float(data[0])  # check that data can be interpolated
    except (ValueError, TypeError):
        return data
    else:
        return sp.interp(desired_time, data_time, data)


def calculate_derived_product(param, coeffs, pd_data, primary_key, level=1):
    """
    Calculates a derived product by (recursively) calulating its derived products.

    The products are added to the pd_data map, not returned
    """
    log.info("")
    spaces = level * 4 * ' '
    log.info("{}Running dpa for {} (PD{}){{".format((level - 1) * 4 * ' ', param.name, param.id))

    this_ref = PDRef(None, param.id)
    needs = [pdref for pdref in param.needs if pdref.pdid not in pd_data.keys()]

    # prevent loops since they are only warned against
    if this_ref in needs:
        needs.remove(this_ref)

    for pdref in needs:
        needed_parameter = CachedParameter.from_id(pdref.pdid)
        if needed_parameter.parameter_type == FUNCTION:
            calculate_derived_product(needed_parameter, coeffs, pd_data, primary_key, level=level + 1)

        if pdref.pdid not in pd_data:
            # _get_param
            pass

    try:
        args = build_func_map(param, coeffs, pd_data, primary_key)
        args = munge_args(args, primary_key)
        data = execute_dpa(param, args)
    except StreamEngineException as e:
        log.info("{}aborting - {}".format(spaces, e.message))
    else:
        if param.id not in pd_data:
            pd_data[param.id] = {}

        if not isinstance(data, (list, tuple, numpy.ndarray)):
            data = [data]

        pd_data[param.id][primary_key.as_refdes()] = {'data': data, 'source': 'derived'}

        log.info(spaces + "returning data")
    log.info("{}}}".format((level - 1) * 4 * ' '))


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

    if len(kwargs) == len(func_map):
        if func.function_type == 'PythonFunction':
            module = importlib.import_module(func.owner)

            result = None
            try:
                result = getattr(module, func.function)(**kwargs)
            except Exception as e:
                raise StreamEngineException('DPA threw exception: %s' % e)
        elif func.function_type == 'NumexprFunction':
            try:
                result = numexpr.evaluate(func.function, kwargs)
            except Exception as e:
                raise StreamEngineException('Numexpr function threw exception: %s' % e)
        else:
            raise UnknownFunctionTypeException(func.function_type)
        return result

    return None


def build_func_map(parameter, coefficients, pd_data, primary_key):
    """
    Builds a map of arguments to be passed to an algorithm
    """
    func_map = parameter.parameter_function_map
    args = {}

    # find stream that provide each parameter
    ps = defaultdict(dict)
    for key, val in func_map.iteritems():
        if PDRef.is_pdref(val):
            pdRef = PDRef.from_str(val)
            if pdRef.pdid in pd_data:
                for stream in pd_data[pdRef.pdid]:
                    ps[pdRef.pdid][stream] = True



    # find correct time parameter for the main stream
    main_stream_refdes = primary_key.as_refdes()
    if primary_key.stream.is_virtual:
        # use source stream for time
        time_stream = primary_key.stream.source_streams[0]
        time_stream_key = get_stream_key_with_param(pd_data, time_stream, time_stream.time_parameter)
    else:
        time_stream_key = primary_key

    if time_stream_key is None:
        raise MissingTimeException("Could not find time parameter for dpa")

    time_stream_refdes = time_stream_key.as_refdes()
    if time_stream_key.stream.time_parameter in pd_data and time_stream_refdes in pd_data[time_stream_key.stream.time_parameter]:
        main_times = pd_data[time_stream_key.stream.time_parameter][time_stream_refdes]['data']
    elif 7 in pd_data and time_stream_refdes in pd_data[7]:
        main_times = pd_data[7][time_stream_refdes]['data']
    else:
        raise MissingTimeException("Could not find time parameter for dpa")

    for key in func_map:
        if PDRef.is_pdref(func_map[key]):
            pdRef = PDRef.from_str(func_map[key])

            if pdRef.pdid not in pd_data:
                raise StreamEngineException('Required parameter %s not found in pd_data when calculating %s (PD%s) ' % (func_map[key], parameter.name, parameter.id))

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
            else:
                # Need to get data from non-main stream, so it must be interpolated to match main stream
                if pdref_refdes is None:
                    data_stream_refdes = next(ps[pdRef.pdid].iterkeys())  # get arbitrary stream that provides pdid
                else:
                    data_stream_refdes = pdref_refdes

                # perform interpolation
                data = pd_data[pdRef.pdid][data_stream_refdes]['data']
                data_time = pd_data[StreamKey.from_refdes(data_stream_refdes).stream.time_parameter][data_stream_refdes]['data']

                interpolated_data = interpolate_list(main_times, data_time, data)
                args[key] = interpolated_data

        elif str(func_map[key]).startswith('CC'):
            name = func_map[key]
            if name in coefficients:
                framed_CCs = coefficients[name]
                CC_argument = build_CC_argument(framed_CCs, main_times)
                if numpy.isnan(numpy.min(CC_argument)):
                    raise CoefficientUnavailableException('Coefficient %s missing times in range (%s, %s)' % (name, ntp_to_datestring(main_times[0]), ntp_to_datestring(main_times[-1])))
                else:
                    args[key] = CC_argument
            else:
                raise CoefficientUnavailableException('Coefficient %s not provided' % name)
        elif isinstance(func_map[key], (int, float, long, complex)):
            args[key] = func_map[key]
        else:
            raise StreamEngineException('Unable to resolve parameter \'%s\' in PD%s %s' % (func_map[key], parameter.id, parameter.name))
    return args


def in_range(frame, times):
    """
    Returns boolean masking array for times in range.

      frame is a tuple such that frame[0] is the inclusive start time and
      frame[1] is the exclusive stop time.  None for any of these indices
      indicates unbounded.

      times is a numpy array of ntp times.

      returns a bool numpy array the same shape as times
    """
    frame = numpy.array(frame)
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
    frames = [(f.get('start'), f.get('stop'), f['value']) for f in frames]
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

    for frame in frames[::-1]:
        mask = in_range(frame, times)
        try:
            cc[mask] = frame[2]
        except ValueError, e:
            raise StreamEngineException('Unable to build cc arguments for algorithm: {}'.format(e))

    return cc


class StreamRequest(object):
    """
    Stores the information from a request, and calculates the required
    parameters and their streams
    """
    def __init__(self, stream_keys, parameters, coefficients, time_range, qc_parameters={},
                 needs_only=False, limit=None, times=None, include_provenance=False, strict_range=False):
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


class NetCDF_Generator(object):

    def __init__(self, pd_data, provenance_metadata):
        self.pd_data = pd_data
        self.provenance_metadata = provenance_metadata

    def chunks(self, r):
        try:
            return self.create_zip(r)
        except GeneratorExit:
            raise
        except:
            log.exception('An unexpected error occurred.')
            raise

    def create_zip(self, r):
        with tempfile.NamedTemporaryFile() as tzf:
            with zipfile.ZipFile(tzf.name, 'w') as zf:
                self.write_to_zipfile(r, zf)
            return tzf.read()

    def write_to_zipfile(self, r, zf):
        datasets = {}

        # sometimes we will get duplicate timestamps
        # INITIAL solution is to remove any duplicate timestamps
        # and the corresponding data.

        # create a mask to match only valid INCREASING times
        # we will insert the last valid timestamp from the previous
        # chunk at the beginning of the array
        primary_key = r.stream_keys[0]

        # Find appropriate times for stream
        tp = primary_key.stream.time_parameter
        chunk_times = None
        if primary_key.stream.is_virtual:
            source_stream = primary_key.stream.source_streams[0]
            tp = source_stream.time_parameter  # get tp from source stream
            source_stream_key = get_stream_key_with_param(self.pd_data, source_stream, source_stream.time_parameter)

            chunk_times = self.pd_data[tp][source_stream_key.as_refdes()]['data']
        elif tp in self.pd_data and primary_key.as_refdes() in self.pd_data[tp]:
            chunk_times = self.pd_data[tp][primary_key.as_refdes()]['data']

        if chunk_times is None:
            raise MissingTimeException("Could not find time parameter for netcdf")

        chunk_valid = numpy.diff(numpy.insert(chunk_times, 0, 0.0)) != 0

        # Select deployment number from the correct stream
        if not primary_key.stream.is_virtual:
            chunk_deployment = str(self.pd_data['deployment'][primary_key.as_refdes()]['data'][0])
        else:
            chunk_deployment = str(self.pd_data['deployment'][source_stream_key.as_refdes()]['data'][0])

        sources = set(self.pd_data[k][primary_key.as_refdes()]['source'] for k in self.pd_data if primary_key.as_refdes() in self.pd_data[k])

        for source in sources:
            datasets[source] = self.group_by_source(r, source, chunk_valid, chunk_times[chunk_valid], primary_key, tp)

        # dump remaining datasets
        for group, ds in datasets.iteritems():
            with tempfile.NamedTemporaryFile() as tf:
                ds.to_netcdf(tf.name)
                zf.write(tf.name, '%s_%s.nc' % (group, chunk_deployment))

    def open_new_ds(self, r):
        # set up file level attributes
        attrs = {
            'subsite': r.stream_keys[0].subsite,
            'node': r.stream_keys[0].node,
            'sensor': r.stream_keys[0].sensor,
            'collection_method': r.stream_keys[0].method,
            'stream': r.stream_keys[0].stream.name
        }

        provenance = {}
        if r.include_provenance:
            prov = self.provenance_metadata.get_provenance_dict()
            keys = []
            values = []
            for k,v in prov.iteritems():
                keys.append(k)
                values.append(v['file_name'] + " " + v['parser_name'] + " " + v['parser_version'])
            provenance['l0_provenance_keys'] = xray.DataArray(numpy.array(keys), dims=['l0_provenance'])
            provenance['l0_provenance_data'] = xray.DataArray(numpy.array(values), dims=['l0_provenance'])

        return xray.Dataset(provenance, attrs=attrs)

    def group_by_source(self, r, source_key, chunk_valid, time_data, primary_key, time_parameter):
        this_group = self.open_new_ds(r)

        for param_id in self.pd_data:
            if (
                param_id == time_parameter or
                primary_key.as_refdes() not in self.pd_data[param_id] or
                self.pd_data[param_id][primary_key.as_refdes()]['source'] != source_key
               ):
                continue

            param = CachedParameter.from_id(param_id)
            # param can be None if this is not a real parameter,
            # like deployment for deployment number
            param_name = param_id if param is None else param.name

            data = self.pd_data[param_id][primary_key.as_refdes()]['data'][chunk_valid]
            if param is not None:
                try:
                    data = data.astype(param.value_encoding)
                except ValueError:
                    log.warning(
                        'Unable to transform data %s of type %s to preload value_encoding %s, using fill_value instead\n%s' % (
                        param_id, data.dtype, param.value_encoding,
                        traceback.format_exc()))
                    data = numpy.full(data.shape, param.fill_value, param.value_encoding)

            dims = ['time']
            coords = {'time': time_data}
            if len(data.shape) > 1:
                for index, dimension in enumerate(data.shape[1:]):
                    name = '%s_dim_%d' % (param_name, index)
                    dims.append(name)

            array_attrs = {}
            if param:
                if param.unit is not None:
                    array_attrs['units'] = param.unit
                if param.fill_value is not None:
                    array_attrs['_FillValue'] = param.fill_value
                if param.description is not None:
                    array_attrs['long_name'] = param.description
                if param.display_name is not None:
                    array_attrs['display_name'] = param.display_name
                if param.data_product_identifier is not None:
                    array_attrs['data_product_identifier'] = param.data_product_identifier

            this_group.update({param_name: xray.DataArray(data, dims=dims, coords=coords, attrs=array_attrs)})
        return this_group


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
