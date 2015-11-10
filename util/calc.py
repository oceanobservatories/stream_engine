import importlib
import logging
import tempfile
import zipfile
import uuid
import os
from collections import OrderedDict, defaultdict

import requests

import numexpr
import numpy
import xray
import scipy as sp

from util.cass import get_streams, get_distinct_sensors, fetch_nth_data, fetch_all_data, fetch_annotations, \
    get_available_time_range, fetch_l0_provenance, time_to_bin, bin_to_time, store_qc_results, get_location_metadata, \
    get_first_before_metadata, get_cass_lookback_dataset, get_full_cass_dataset, get_san_location_metadata, \
    get_full_cass_dataset, insert_dataset, get_streaming_provenance, CASS_LOCATION_NAME, SAN_LOCATION_NAME, get_pool

from util.common import log_timing, ntp_to_datestring,ntp_to_ISO_date, StreamKey, TimeRange, CachedParameter, \
    FUNCTION, CoefficientUnavailableException, UnknownFunctionTypeException, \
    CachedStream, StreamEngineException, CachedFunction, Annotation, \
    MissingTimeException, MissingDataException, MissingStreamMetadataException, get_stream_key_with_param, \
    isfillvalue, InvalidInterpolationException, to_xray_dataset, get_params_with_dpi, ParamUnavailableException, compile_datasets

from parameter_util import PDArgument, FQNArgument, DPIArgument, CCArgument, NumericArgument, FunctionArgument

from csvresponse import CSVGenerator

from engine.routes import app
from util.san import fetch_nsan_data, fetch_full_san_data, get_san_lookback_dataset


import datamodel
from datamodel import StreamData
from jsonresponse import JsonResponse
from datetime import datetime
import time

import ion_functions
if hasattr(ion_functions, '__version__'):
    ION_VERSION = ion_functions.__version__
else:
    ION_VERSION = 'unversioned'

try:
    import simplejson as json
except ImportError:
    import json

log = logging.getLogger(__name__)


@log_timing(log)
def get_particles(streams, start, stop, coefficients, qc_parameters, limit=None,
                  include_provenance=False, include_annotations=False, strict_range=False, location_information={}, request_uuid=''):
    """
    Returns a list of particles from the given streams, limits and times
    """
    stream_keys = [StreamKey.from_dict(d) for d in streams]
    parameters = []
    for s in streams:
        for p in s.get('parameters', []):
            parameters.append(CachedParameter.from_id(p))
    time_range = TimeRange(start, stop)

    qc_stream_parameters = prepare_qc_stuff(qc_parameters)

    # create the store that will keep track of provenance for all streams/datasources
    provenance_metadata = ProvenanceMetadataStore(request_uuid)
    annotation_store = AnnotationStore()
    stream_request = StreamRequest(stream_keys, parameters, coefficients, time_range,
                                   qc_parameters=qc_stream_parameters, limit=limit,
                                   include_provenance=include_provenance,include_annotations=include_annotations,
                                   strict_range=strict_range, location_information=location_information, request_id=request_uuid)

    # Create the medata store
    provenance_metadata.add_query_metadata(stream_request, request_uuid, 'JSON')
    stream_data = fetch_stream_data(stream_request, streams, start, stop, coefficients, limit, provenance_metadata, annotation_store)

    do_qc_stuff(stream_keys[0], stream_data, stream_request.parameters, qc_stream_parameters)

    # If multi-stream request, default to interpolating all times to first stream
    if len(streams) > 1:
        stream_data.deployment_times = stream_data.get_time_data(stream_keys[0])

    # create StreamKey to CachedParameter mapping for the requested streams
    stream_to_params = {StreamKey.from_dict(s): [] for s in streams}
    for sk in stream_to_params:
        stream_to_params[sk] = [p for p in stream_request.parameters if sk.stream.id in p.streams]

    return JsonResponse(stream_data).json(stream_to_params)

@log_timing(log)
def prepare_qc_stuff(qc_parameters):
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
                try:
                    parameter_dict[current_parameter_name] = int(qc_parameter_value)
                except ValueError:
                    log.error(
                        "For parameter '%s' in qc function '%s' being run against '%s', the value '%s' is not an integer number.",
                        current_parameter_name, current_qc_id, current_stream_parameter, qc_parameter_value)
                    parameter_dict[current_parameter_name] = float('nan')
            elif qc_parameter['valueType'].encode('ascii', 'ignore') == 'FLOAT':
                try:
                    parameter_dict[current_parameter_name] = float(qc_parameter_value)
                except ValueError:
                    log.error(
                        "For parameter '%s' in qc function '%s' being run against '%s', the value '%s' is not a number.",
                        current_parameter_name, current_qc_id, current_stream_parameter, qc_parameter_value)
                    parameter_dict[current_parameter_name] = float('nan')
            elif qc_parameter['valueType'].encode('ascii', 'ignore') == 'LIST':
                parameter_dict[current_parameter_name] = [float(x) for x in qc_parameter_value[1:-1].split()]
            else:
                parameter_dict[current_parameter_name] = qc_parameter_value
    return qc_stream_parameters

@log_timing(log)
def do_qc_stuff(primary_key, stream_data, parameters, qc_stream_parameters):
    time_param = primary_key.stream.time_parameter
    for deployment in stream_data.deployments:
        if not stream_data.check_stream_deployment(primary_key, deployment):
            log.warn("%s not in deployment %d Continuing", primary_key.as_refdes(), deployment)
            continue
        pd_data = stream_data.data[deployment]
        if time_param not in pd_data:
            raise MissingTimeException("Time param: {} is missing from the primary stream".format(time_param))
        time_data = pd_data[time_param][primary_key.as_refdes()]['data']

        virtual_id_sub = None
        particle_ids = None
        particle_bins = None
        particle_deploys = None
        if not primary_key.stream.is_virtual:
            particle_ids = pd_data['id'][primary_key.as_refdes()]['data']
            particle_bins = pd_data['bin'][primary_key.as_refdes()]['data']
            particle_deploys = pd_data['deployment'][primary_key.as_refdes()]['data']
        else:
            if 'id' in pd_data:
                if virtual_id_sub is None:
                    for key in pd_data['id']:
                        if len(pd_data['id'][key]['data']) == len(time_data):
                            virtual_id_sub = key
                            break
                if virtual_id_sub is None:
                    continue

                particle_ids = pd_data['id'][virtual_id_sub]['data']

            if 'bin' in pd_data:
                if virtual_id_sub is None:
                    for key in pd_data['bin']:
                        if len(pd_data['bin'][key]['data']) == len(time_data):
                            virtual_id_sub = key
                            break
                particle_bins = pd_data['bin'][virtual_id_sub]['data']

            if 'deployment' in pd_data:
                if virtual_id_sub is None:
                    for key in pd_data['deployment']:
                        if len(pd_data['deployment'][key]['data']) == len(time_data):
                            virtual_id_sub = key
                            break
                particle_deploys = pd_data['deployment'][virtual_id_sub]['data']

        pk = primary_key.as_dict()
        for param in parameters:
            has_qc = False
            qc_results_key = '%s_%s' % (param.name, 'qc_results')
            qc_ran_key = '%s_%s' % (param.name, 'qc_executed')
            qc_results_values = numpy.zeros_like(time_data, dtype=numpy.int8)
            qc_ran_values = numpy.zeros_like(time_data, dtype=numpy.int8)

            for qc_function_name in qc_stream_parameters.get(param.name, []):
                qc_function_results = '%s_%s' % (param.name, qc_function_name)

                if qc_function_results in pd_data\
                        and pd_data.get(qc_function_results, {}).get(primary_key.as_refdes(), {}).get('data') is not None:
                    has_qc = True
                    qc_function_results_entry = pd_data.pop(qc_function_results)
                    values = qc_function_results_entry[primary_key.as_refdes()]['data']
                    qc_cached_function = CachedFunction.from_qc_function(qc_function_name)
                    qc_results_mask = numpy.full_like(time_data, int(qc_cached_function.qc_flag, 2), dtype=numpy.int8)
                    qc_results_values = numpy.where(values == 0, ~qc_results_mask & qc_results_values, qc_results_mask ^ qc_results_values)
                    qc_ran_values = qc_results_mask | qc_ran_values

            if has_qc:
                if particle_ids is not None and particle_bins is not None and particle_deploys is not None:
                    store_qc_results(qc_results_values, pk, particle_ids, particle_bins, particle_deploys, param.name)
                pd_data[qc_results_key] = {primary_key.as_refdes(): {'data': qc_results_values}}
                pd_data[qc_ran_key] = {primary_key.as_refdes(): {'data': qc_ran_values}}


@log_timing(log)
def get_csv(streams, start, stop, coefficients, limit=None,
               include_provenance=False, include_annotations=False, strict_range=False, location_information={},
               request_uuid='', disk_path=None, delimiter=','):
    if len(streams) > 1 and disk_path is None:
        msg ="Syncronous delimited data not currently permitted for multiple streams"
        log.error(msg)
        raise StreamEngineException(msg, status_code=400)
    stream_keys = [StreamKey.from_dict(d) for d in streams]
    parameters = []
    for s in streams:
        for p in s.get('parameters', []):
            parameters.append(CachedParameter.from_id(p))
    time_range = TimeRange(start, stop)

    # Create the provenance metadata store to keep track of all files that are used
    provenance_metadata = ProvenanceMetadataStore(request_uuid)
    annotation_store = AnnotationStore()
    stream_request = StreamRequest(stream_keys, parameters, coefficients, time_range, limit=limit,
                                   include_provenance=include_provenance,include_annotations=include_annotations,
                                   strict_range=strict_range, location_information=location_information, request_id=request_uuid)
    provenance_metadata.add_query_metadata(stream_request, request_uuid, "delimited({:s})".format(delimiter))
    stream_data = fetch_stream_data(stream_request, streams, start, stop, coefficients, limit, provenance_metadata, annotation_store)
    # create StreamKey to CachedParameter mapping for the requested streams
    stream_to_params = {StreamKey.from_dict(s): [] for s in streams}
    for sk in stream_to_params:
        stream_to_params[sk] = [p for p in stream_request.parameters if sk.stream.id in p.streams]
    return CSVGenerator(stream_data, delimiter, stream_to_params).chunks(disk_path)


@log_timing(log)
def get_netcdf(streams, start, stop, coefficients, qc_parameters, limit=None,
               include_provenance=False, include_annotations=False, strict_range=False, location_information={},
               request_uuid='', disk_path=None, classic=False):
    """
    Returns a netcdf from the given streams, limits and times
    """
    stream_keys = [StreamKey.from_dict(d) for d in streams]
    parameters = []
    for s in streams:
        for p in s.get('parameters', []):
            parameters.append(CachedParameter.from_id(p))
    time_range = TimeRange(start, stop)

    qc_stream_parameters = prepare_qc_stuff(qc_parameters)

    # Create the provenance metadata store to keep track of all files that are used
    provenance_metadata = ProvenanceMetadataStore(request_uuid)
    annotation_store = AnnotationStore()
    stream_request = StreamRequest(stream_keys, parameters, coefficients, time_range,
                                   qc_parameters=qc_stream_parameters, limit=limit,
                                   include_provenance=include_provenance,include_annotations=include_annotations,
                                   strict_range=strict_range, location_information=location_information, request_id=request_uuid)
    provenance_metadata.add_query_metadata(stream_request, request_uuid, "netCDF")

    stream_data = fetch_stream_data(stream_request, streams, start, stop, coefficients, limit, provenance_metadata, annotation_store)

    do_qc_stuff(stream_keys[0], stream_data, stream_request.parameters, qc_stream_parameters)

    # If multi-stream request, default to interpolating all times to first stream
    if len(streams) > 1:
        stream_data.deployment_times = stream_data.get_time_data(stream_keys[0])

    return NetCDF_Generator(stream_data, classic).chunks(disk_path)


@log_timing(log)
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


@log_timing(log)
def get_lookback_dataset(key, time_range, provenance_metadata, deployments):
    first_metadata = get_first_before_metadata(key, time_range.start)
    if CASS_LOCATION_NAME in first_metadata:
        locations = first_metadata[CASS_LOCATION_NAME]
        return get_cass_lookback_dataset(key, time_range.start, locations.bin_list[0], deployments)
    elif SAN_LOCATION_NAME in first_metadata:
        locations = first_metadata[SAN_LOCATION_NAME]
        return get_san_lookback_dataset(key, TimeRange(locations.start_time,time_range.start), locations.bin_list[0], deployments)
    else:
        return None


@log_timing(log)
def get_dataset(key, time_range, limit, provenance_metadata, pad_forward, deployments):
    cass_locations, san_locations, messages = get_location_metadata(key, time_range)
    provenance_metadata.add_messages(messages)
    # check for no data
    datasets = []
    if cass_locations.total + san_locations.total  != 0:
        san_percent = san_locations.total / float(san_locations.total + cass_locations.total)
        cass_percent = cass_locations.total / float(san_locations.total + cass_locations.total)
    else:
        san_percent = 0
        cass_percent = 0

    if pad_forward:
        # pad forward on some datasets
        datasets.append(get_lookback_dataset(key, time_range, provenance_metadata, deployments))

    if san_locations.total > 0:
        # put the range down if we are within the time range
        t1 = max(time_range.start, san_locations.start_time)
        t2 = min(time_range.stop, san_locations.end_time)
        san_times = TimeRange(t1, t2)
        if limit:
            datasets.append(fetch_nsan_data(key, san_times, num_points=int(limit * san_percent),
                                            location_metadata=san_locations))
        else:
            datasets.append(fetch_full_san_data(key, san_times, location_metadata=san_locations))
    if cass_locations.total > 0:
        t1 = max(time_range.start, cass_locations.start_time)
        t2 = min(time_range.stop, cass_locations.end_time)
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
    return compile_datasets(datasets)


@log_timing(log)
def fetch_stream_data(stream_request, streams, start, stop, coefficients, limit, provenance_metadata, annotation_store):
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
    primary_deployments = []
    virtual_request = primary_key.stream.is_virtual
    # fit time ranges to metadata if we can.
    if app.config['COLLAPSE_TIMES']:
        log.warn('Collapsing time range primary stream time: ' + ntp_to_datestring(time_range.start) +
                 " -- " + ntp_to_datestring(time_range.stop))
        if not virtual_request:
            # collapse only to main streams time range.
            data_range = get_available_time_range(primary_key)
            time_range.start = max(time_range.start, data_range.start)
            time_range.stop = min(time_range.stop, data_range.stop)
        else:
            # for virtual streams collapse all of the times
            for sk in stream_request.stream_keys:
                if not sk.stream.is_virtual:
                    data_range = get_available_time_range(sk)
                    time_range.start = max(time_range.start, data_range.start)
                    time_range.stop = min(time_range.stop, data_range.stop)
        log.warn('Collapsed to : ' + ntp_to_datestring(time_range.start) +
                 "  -- " + ntp_to_datestring(time_range.stop) + '\n')

    log.info("Fetching data from {} stream(s): {}".format(len(stream_request.stream_keys), [x.as_refdes() for x in stream_request.stream_keys]))

    # Find source stream if primary_stream is virtual
    primary_stream = stream_request.stream_keys[0].stream
    source_stream_key = None
    if primary_stream.is_virtual:
        for sk in stream_request.stream_keys:
            if sk.stream in primary_stream.source_streams:
                source_stream_key = sk
                break

    if primary_stream.is_virtual and source_stream_key is None:
        raise StreamEngineException("Can't find deployment for virtual stream")

    stream_data = {}
    first_time = None
    for key_index, key in enumerate(stream_request.stream_keys):
        is_main_query = key == primary_key
        annotations = []
        if stream_request.include_annotations:
            annotations = query_annotations(key, time_range)
            annotation_store.add_annotations(annotations)
        if stream_request.include_provenance:
            provenance_metadata.add_instrument_provenance(key,time_range.start, time_range.stop)

        if key.stream.is_virtual:
            log.info("Skipping fetch of virtual stream: {}".format(key.stream.name))
            continue

        # Only pad forward when we are not on the main queried stream and it is not a virtual request.
        should_pad = not is_main_query and not virtual_request
        ds = get_dataset(key, time_range, limit, provenance_metadata, should_pad, primary_deployments)
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
            pd_data = stream_data.get(dep_num, {})
            if is_main_query or (primary_stream.is_virtual and key == source_stream_key):
                primary_deployments.append(dep_num)
            for param in parameters:
                if param.name not in data_set:
                    log.error("%s (PD%s) not in cassandra dataset, cassandra and preload may be out of sync", param.name, param.id)
                    continue
                data_slice = data_set[param.name].values

                if param.id not in pd_data:
                    pd_data[param.id] = {}
                pd_data[param.id][key.as_refdes()] = {
                    'data': data_slice,
                    'source': key.as_dashed_refdes()
                }

            if 'provenance' not in pd_data:
                pd_data['provenance'] = {}
            # Add additonal delivery methods here to generate provenance for cabled streams.
            if key.method not in ['streamed',] or not stream_request.include_provenance:
                # Add non-param data to particle
                pd_data['provenance'][key.as_refdes()] = {
                    'data': data_set.provenance.values.astype('str'),
                    'source': key.as_dashed_refdes()
                }
                if stream_request.include_provenance:
                    provenance_metadata.update_provenance(fetch_l0_provenance(key, data_set.provenance.values.astype('str'), dep_num))
            else:
                # Get the ids for times and get the provenance information
                prov_ids, prov_dict = get_streaming_provenance(key, data_set['time'].values)
                provenance_metadata.update_streaming_provenance(prov_dict)
                pd_data['provenance'][key.as_refdes()] = {
                    'data' : prov_ids,
                    'source' : key.as_dashed_refdes()
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
            stream_data[dep_num] = pd_data
    # exec dpa for stream

    refdes_lengths = {} # used for checking that all data returned from a refdes is the same length
    for dep_num in primary_deployments:
        log.info("Computing DPA for deployment %d", dep_num)
        pd_data = stream_data[dep_num]

        # calculate all time params first if they are derived products
        found_time_params = {}
        for pd_arg in stream_request.found_args:
            time_param = CachedParameter.from_id(pd_arg.stream_key.stream.time_parameter)
            if time_param.parameter_type == FUNCTION and time_param.id not in found_time_params:
                calculate_derived_product(time_param, stream_request.coefficients, pd_data, primary_key, provenance_metadata, stream_request, dep_num, refdes_lengths)

                if time_param.id not in pd_data or pd_arg.stream_key.as_refdes() not in pd_data[time_param.id]:
                    raise MissingTimeException("Time param (PD%s) is missing for PD%s" % (time_param.id, pd_arg.pdid))
                else:
                    found_time_params[time_param.id] = True

        for param in primary_key.stream.parameters:
            if param.id not in pd_data:
                if param.parameter_type == FUNCTION:
                    # calculate inserts derived products directly into pd_data
                    calculate_derived_product(param, stream_request.coefficients, pd_data, primary_key, provenance_metadata, stream_request, dep_num, refdes_lengths)
                else:
                    log.warning("Required parameter not present: {}".format(param.name))
            if stream_request.qc_parameters.get(param.name) is not None \
                    and pd_data.get(param.id, {}).get(primary_key.as_refdes(), {}).get('data') is not None:
                try:
                    _qc_check(stream_request, param, pd_data, primary_key)
                except Exception as e:
                    log.exception("Unexpected error while running qc functions: {}".format(e.message))
        stream_data[dep_num] = pd_data

    sd = StreamData(stream_request, stream_data, provenance_metadata, annotation_store)
    return sd


@log_timing(log)
def _qc_check(stream_request, parameter, pd_data, primary_key):
    qcs = stream_request.qc_parameters.get(parameter.name)
    local_qc_args = defaultdict(dict)
    for function_name in qcs:
        for qcp in qcs[function_name].keys(): #vector qc parameters are the only ones with string values and need populating
            if qcs[function_name][qcp] == 'time': #populate time vectors with particle times
                local_qc_args[function_name][qcp] =  pd_data[primary_key.stream.time_parameter][primary_key.as_refdes()]['data'][:]
            elif qcs[function_name][qcp] == 'data': #populate data vectors with particle data
                local_qc_args[function_name][qcp] = pd_data[parameter.id][primary_key.as_refdes()]['data']
            elif isinstance(qcs[function_name][qcp], basestring):
                for p in stream_request.parameters: #populate stream parameter vectors with data from that stream
                    if p.name.encode('ascii', 'ignore') == qcs[function_name][qcp] and pd_data.get(p.id, {}).get(primary_key.as_refdes(), {}).get('data') is not None:
                        local_qc_args[function_name][qcp] = pd_data[p.id][primary_key.as_refdes()]['data']
                        break
            else:
                local_qc_args[function_name][qcp] = qcs[function_name][qcp]

        if 'strict_validation' not in qcs[function_name]:
            local_qc_args[function_name]['strict_validation'] = False

        module = importlib.import_module(CachedFunction.from_qc_function(function_name).owner)

        qc_name = '%s_%s' % (parameter.name.encode('ascii', 'ignore'), function_name)
        if qc_name not in pd_data:
            pd_data[qc_name] = {}
        pd_data[qc_name][primary_key.as_refdes()] = {
            'data': getattr(module, function_name)(**local_qc_args.get(function_name)),
            'source': 'qc'
        }


@log_timing(log)
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


@log_timing(log)
def calculate_derived_product(param, coeffs, pd_data, primary_key, provenance_metadata, stream_request, deployment, refdes_lengths, level=1):
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
        if PDArgument.is_pd(i):
            ref_to_name[PDArgument.from_preload(i)] = rev_func_map[i]
        elif FQNArgument.is_fqn(i):
            ref_to_name[FQNArgument.from_preload(i)] = rev_func_map[i]

    this_ref = PDArgument(param.id)
    needs = [arg for arg in param.needs if arg.pdid not in pd_data.keys()]

    calc_meta = OrderedDict()
    subs = []
    parameters = {}
    functions = {}

    # prevent loops since they are only warned against
    other = set([arg for arg in param.needs if arg != this_ref]) - set(needs)
    if this_ref in needs:
        needs.remove(this_ref)

    for arg in needs:
        found_arg = stream_request.found_args.get(arg)
        if found_arg is None:
            continue
        needed_parameter = CachedParameter.from_id(found_arg.pdid)
        if needed_parameter.parameter_type == FUNCTION:
            sub_id = calculate_derived_product(needed_parameter,
                                               coeffs,
                                               pd_data,
                                               primary_key,
                                               provenance_metadata,
                                               stream_request,
                                               deployment,
                                               refdes_lengths,
                                               level + 1)
            # Keep track of all sub function we used.
            if sub_id is not None:
                # only include sub_functions if it is in the function map. Otherwise it is calculated in the sub function
                if arg in ref_to_name:
                    functions[ref_to_name[arg]] = [sub_id]
                    subs.append(sub_id)

        else:
            # add a placeholder
            parameters[arg] = arg

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
        args, arg_meta, messages = build_func_map(param, coeffs, pd_data, parameter_key, deployment, stream_request, spaces)
        provenance_metadata.add_messages(messages)
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

        #To support netcdf aggregation we need to fill all missing values across all files in case it is defined in all other locations.
        shp = get_shape(param, parameter_key, pd_data)
        data = numpy.empty(shape=shp, dtype=param.value_encoding)
        data.fill(param.fill_value)
        if param.id not in pd_data:
            pd_data[param.id] = {}
        pd_data[param.id][parameter_key.as_refdes()] = {'data': data, 'source': 'filled'}
        log.info("{}aborting - {}".format(spaces, e.message))
    else:
        if param.id not in pd_data:
            pd_data[param.id] = {}

        if not isinstance(data, (list, tuple, numpy.ndarray)):
            data = [data]

        # confirm that data lengths are consistent for a stream key
        if parameter_key in refdes_lengths:
            if len(data) != refdes_lengths[parameter_key]:
                log.error("length of data returned does match other params in refdes")
                return calc_id
        else:
            refdes_lengths[parameter_key] = len(data)

        pd_data[param.id][parameter_key.as_refdes()] = {'data': data, 'source': 'derived'}
        calc_id = provenance_metadata.calculated_metatdata.insert_metadata(param, this_ref, calc_meta)

        log.info("{}returning data (t: {}, l: {})".format(spaces, type(data[0]).__name__, len(data)))
    log.info("{}}}".format(spaces[:-4]))

    return calc_id

def get_shape(param, base_key, pd_data):
    tp = base_key.stream.time_parameter
    try:
        main_times = pd_data[tp][base_key.as_refdes()]['data']
    except KeyError:
        raise MissingTimeException("Could not find time parameter %s for %s" % (tp, stream_key))
    # TODO Populate preload with shape information so we can return the shape values for now only filling with no dimension
    return (len(main_times),)





@log_timing(log)
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


@log_timing(log)
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
            version = ION_VERSION
            result = None
            try:
                dpa_function = getattr(module, func.function)
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
    else:
        raise StreamEngineException("Wrong number of arguments passed to function")


@log_timing(log)
def build_func_map(parameter, coefficients, pd_data, base_key, deployment, stream_request, spaces=""):
    """
    Builds a map of arguments to be passed to an algorithm
    """
    func_map = parameter.parameter_function_map
    args = {}
    arg_metadata = {}
    messages = []

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

    for key, val in func_map.iteritems():
        if PDArgument.is_pd(val) or FQNArgument.is_fqn(val) or DPIArgument.is_dpi(val):
            desired_arg = FunctionArgument.get_arg_from_val(val)
            if desired_arg in stream_request.found_args:
                pd_arg = stream_request.found_args[desired_arg]
            else:
                raise ParamUnavailableException("Can't find source for {}".format(desired_arg))

            if pd_arg.pdid not in pd_data or pd_arg.stream_key.as_refdes() not in pd_data[pd_arg.pdid]:
                raise ParamUnavailableException("{} is not available".format(desired_arg))

            data_time = pd_data[pd_arg.stream_key.stream.time_parameter][pd_arg.stream_key.as_refdes()]['data']
            if pd_arg.stream_key == base_key:
                args[key] = pd_data[pd_arg.pdid][pd_arg.stream_key.as_refdes()]['data']
            else:
                data = pd_data[pd_arg.pdid][pd_arg.stream_key.as_refdes()]['data']

                interpolated_data = interpolate_list(main_times, data_time, data)
                args[key] = interpolated_data

            # Provenance
            #############
            param_meta = {}
            param = CachedParameter.from_id(pd_arg.pdid)
            param_meta['type'] = "parameter"
            param_meta['source'] = pd_data[pd_arg.pdid][pd_arg.stream_key.as_refdes()]['source']
            param_meta['parameter_id'] = param.id
            param_meta['name'] = param.name
            param_meta['data_product_identifier'] = param.data_product_identifier
            param_meta['iterpolated'] = True
            param_meta['time_start'] = data_time[0]
            param_meta['time_startDT'] = ntp_to_datestring(data_time[0])
            param_meta['time_end'] =  data_time[-1]
            param_meta['time_endDT'] = ntp_to_datestring(data_time[-1])
            try:
                param_meta['deployments'] = list(set(pd_data['deployment'][data_stream_refdes]['data']))
            except:
                pass
            #############
            arg_metadata[key] = param_meta
        elif CCArgument.is_cc(val):
            name = val
            if name in coefficients:
                framed_CCs = coefficients[name]
                # Need to catch exceptions from the call to build the CC argument.
                try:
                    CC_argument, CC_meta = build_CC_argument(framed_CCs, main_times, deployment)
                except StreamEngineException as e:
                    to_attach = {
                        'type' : 'CCTimeError', 'parameter' : parameter, 'begin' : main_times[0], 'end' : main_times[-1],
                        'beginDT' : ntp_to_datestring(main_times[0]), 'endDT' : ntp_to_datestring(main_times[-1]),
                        'CC_present' : coefficients.keys(), 'missing_argument_name' : key
                    }
                    raise CoefficientUnavailableException(e.message, payload=to_attach)
                nan_locs = numpy.isnan(CC_argument)
                if numpy.all(nan_locs):
                    to_attach = {
                        'type' : 'CCTimeError', 'parameter' : parameter, 'begin' : main_times[0], 'end' : main_times[-1],
                        'beginDT' : ntp_to_datestring(main_times[0]), 'endDT' : ntp_to_datestring(main_times[-1]),
                        'CC_present' : coefficients.keys(), 'missing_argument_name' : key
                                 }
                    raise CoefficientUnavailableException('Coefficient %s missing for all times in range (%s, %s)' % (name, ntp_to_datestring(main_times[0]), ntp_to_datestring(main_times[-1])), payload=to_attach)
                if numpy.any(nan_locs):
                    msg = "There was not Coefficeient data for {:s} all times in deployment {:d} in range ({:s} {:s})".format(
                        name, deployment,
                        ntp_to_datestring(main_times[0]), ntp_to_datestring(main_times[-1]))
                    log.warn(spaces[:-3]+msg)
                    messages.append(msg)
                args[key] = CC_argument
                arg_metadata[key] = CC_meta
            else:
                to_attach = {
                    'type' : 'CCMissingError', 'parameter' : parameter, 'begin' : main_times[0], 'end' : main_times[-1],
                    'beginDT' : ntp_to_datestring(main_times[0]), 'endDT' : ntp_to_datestring(main_times[-1]),
                    'CC_present' : coefficients.keys(), 'missing_argument_name' : key
                }
                raise CoefficientUnavailableException('Coefficient %s not provided' % name, payload=to_attach)
        elif NumericArgument.is_num(val):
            args[key] = [func_map[key]] * len(main_times)
            arg_metadata[key] = {'type' : 'constant', 'value' : val}
        else:
            to_attach = {'type' : 'UnknownParameter', 'parameter' : parameter, 'value' : str(func_map[key]),
                         'missing_argument_name' : key}
            raise StreamEngineException('Unable to resolve parameter \'%s\' in PD%s %s' %
                                        (func_map[key], parameter.id, parameter.name), payload=to_attach)
    return args, arg_metadata, messages


@log_timing(log)
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


@log_timing(log)
def build_CC_argument(frames, times, deployment):
    st = times[0]
    et = times[-1]
    startDt = ntp_to_datestring(st)
    endDt = ntp_to_datestring(et)
    frames = [(f.get('start'), f.get('stop'), f['value'], f['deployment']) for f in frames]
    frames.sort()
    if deployment == 0:
        # If deployment is 0 we are using cabled data and only care about times.
        frames = [f for f in frames if any(in_range(f, times))]
    else:
        # filter based on times and deployment
        frames = [f for f in frames if any(in_range(f, times)) and f[3] == deployment]

    if len(frames) == 0:
        raise StreamEngineException('Unable to build cc arguments for algorithm: no cc exists that matches times and deployment')

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
    try:
        for frame in frames:
            values.append({'CC_begin' : frame[0], 'CC_stop' : frame[1], 'value' : frame[2], 'deployment' : frame[3],
                       'CC_beginDT' : ntp_to_datestring(frame[0]), 'CC_stopDT' : ntp_to_datestring(frame[1])})
            mask = in_range(frame, times)
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
                 limit=None, include_provenance=False, include_annotations=False, strict_range=False,
                 location_information={}, request_id=''):
        self.stream_keys = stream_keys
        self.time_range = time_range
        self.qc_parameters = qc_parameters if qc_parameters is not None else {}
        self.parameters = parameters
        self.coefficients = coefficients
        self.location_information = location_information
        self.needs_cc = None
        self.needs_params = None
        self.limit = limit
        self.include_provenance = include_provenance
        self.include_annotations = include_annotations
        self.strict_range = strict_range
        self._initialize(needs_only)
        self.request_id = request_id

    @log_timing(log)
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

        # get provided params
        provided_dpi = set()
        provided_pd = set()
        provided_fqn = set()
        for stream_key in self.stream_keys:
            [provided_dpi.add(p.data_product_identifier) for p in stream_key.stream.parameters]
            [provided_pd.add(p.id) for p in stream_key.stream.parameters]
            [provided_fqn.add((p.id, stream_key.stream_name)) for p in stream_key.stream.parameters]

        distinct_sensors = get_distinct_sensors()

        # remove already provided params
        provided_by_main = set()
        for arg in needs:
            if isinstance(arg, PDArgument):
                if arg.pdid in provided_pd:
                    provided_by_main.add(arg)
            elif isinstance(arg, FQNArgument):
                if (arg.pdid, arg.stream_name) in provided_fqn:
                    provided_by_main.add(arg)
            elif isinstance(arg, DPIArgument):
                if arg.dpi in provided_dpi:
                    provided_by_main.add(arg)
            else:
                raise StreamEngineException("Found invalid argument type: {}".format(type(arg)), status_code=500)

        needs = needs.difference(provided_by_main)

        ## find the available streams which provide any needed parameters
        found_args = set()

        # Add params from main stream to found_args
        fsk = self.stream_keys[0]
        found_args = found_args.union(PDArgument(p.id, fsk) for p in fsk.stream.parameters)
        found_args = found_args.union(DPIArgument(p.data_product_identifier, p.id, fsk) for p in fsk.stream.parameters)
        found_args = found_args.union(FQNArgument(p.id, fsk.stream.name, fsk) for p in fsk.stream.parameters)

        for arg in needs:
            if isinstance(arg, PDArgument):
                possible_streams = CachedParameter.from_id(arg.pdid).streams
            elif isinstance(arg, FQNArgument):
                streams_that_provide_need = CachedParameter.from_id(arg.pdid).streams
                possible_streams = [s for s in streams_that_provide_need if s.name == arg.stream_name]
            elif isinstance(arg, DPIArgument):
                possible_params = get_params_with_dpi(arg.dpi)
                possible_streams = []
                for param in possible_params:
                    for stream in param.streams:
                        possible_streams.append(stream.id)
            else:
                raise StreamEngineException("Found invalid argument type: {}".format(type(arg)), status_code=500)

            possible_streams = [CachedStream.from_id(s) for s in possible_streams]
            found_stream_key = find_stream(self.stream_keys[0], possible_streams, distinct_sensors)

            if found_stream_key is not None and found_stream_key not in self.stream_keys:
                self.stream_keys.append(found_stream_key)
                fsk = found_stream_key
                found_args = found_args.union(PDArgument(p.id, fsk) for p in fsk.stream.parameters)
                found_args = found_args.union(DPIArgument(p.data_product_identifier, p.id, fsk) for p in fsk.stream.parameters)
                found_args = found_args.union(FQNArgument(p.id, found_stream_key.stream.name, fsk) for p in fsk.stream.parameters)
            else:
                # if we couldn't find a providing stream key, the arg might be provided by a virtual stream
                for s in possible_streams:
                    if s.is_virtual:
                        found_stream_key = find_stream(self.stream_keys[0], s.source_streams, distinct_sensors)

                        if found_stream_key is not None:
                            for product_stream in found_stream_key.stream.product_streams:
                                new_sk = StreamKey(found_stream_key.subsite,
                                        found_stream_key.node,
                                        found_stream_key.sensor,
                                        found_stream_key.method,
                                        product_stream.name)

                                found_args = found_args.union(PDArgument(p.id, new_sk) for p in product_stream.parameters)
                                found_args = found_args.union(DPIArgument(p.data_product_identifier, p.id, new_sk) for p in product_stream.parameters)
                                found_args = found_args.union(FQNArgument(p.id, found_stream_key.stream.name, new_sk) for p in product_stream.parameters)

                                if new_sk not in self.stream_keys:
                                    self.stream_keys.append(new_sk)

        self.found_args = {arg:arg for arg in found_args}
        self.needs_params = needs.difference(x for x in needs if x in found_args)

        needs_cc = set()
        for sk in self.stream_keys:
            needs_cc = needs_cc.union(sk.needs_cc)

        self.needs_cc = needs_cc.difference(self.coefficients.keys())

        # deduplicated stream_keys
        self.stream_keys = list(OrderedDict.fromkeys(self.stream_keys))

        if len(self.needs_params) > 0:
            log.error('Unable to find needed arguments: %s', map(str, self.needs_params))

        if len(self.needs_cc) > 0:
            log.error('Missing calibration coefficients: %s', self.needs_cc)

    @log_timing(log)
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

def send_query_for_instrument(url):
    results = requests.get(url)
    jres = results.json()
    return jres

class ProvenanceMetadataStore(object):
    def __init__(self, request_uuid):
        self.request_uuid = request_uuid
        self._prov_set = set()
        self.calculated_metatdata = CalculatedProvenanceMetadataStore()
        self.messages = []
        self._prov_dict = {}
        self._streaming_provenance = {}
        self._instrument_provenance = {}

    def add_messages(self, messages):
        self.messages.extend(messages)

    def add_metadata(self, value):
        self._prov_set.add(value)

    def update_provenance(self, provenance):
        for i in provenance:
            self._prov_dict[i] = provenance[i]

    def update_streaming_provenance(self, stream_prov):
        for i in stream_prov:
            self._streaming_provenance[i] = stream_prov[i]

    def get_streaming_provenance(self):
        return self._streaming_provenance

    def get_provenance_dict(self):
        return self._prov_dict

    def add_instrument_provenance(self, stream_key, st, et ):
        url = app.config['ASSET_URL'] + 'assets/byReferenceDesignator/{:s}/{:s}/{:s}?startDT={:s}?endDT={:s}'.format(
            stream_key.subsite, stream_key.node, stream_key.sensor,ntp_to_ISO_date(st), ntp_to_ISO_date(et))
        self._instrument_provenance[stream_key] =  get_pool().apply_async(send_query_for_instrument, (url,))

    def get_instrument_provenance(self):
        vals = defaultdict(list)
        for key, value in self._instrument_provenance.iteritems():
            vals[key.as_three_part_refdes()].extend(value.get())
        return vals


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

    def __init__(self, stream_data, classic):
        self.stream_data = stream_data
        self.classic = classic

    def chunks(self, disk_path=None):
        try:
            if disk_path is not None:
                return self.create_raw_files(disk_path)
            else:
                return self.create_zip()
        except GeneratorExit:
            raise
        except:
            log.exception('An unexpected error occurred.')
            raise

    @log_timing(log)
    def create_raw_files(self, path):
        file_paths = list()
        base_path = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'],path)
        # ensure the directory structure is there
        if not os.path.isdir(base_path):
            os.makedirs(base_path)
        for stream_key, deployment, ds in self.stream_data.groups():
                file_path = '%s/deployment%04d_%s.nc' % (base_path, deployment, stream_key.as_dashed_refdes())
                self.to_netcdf(ds, file_path)
                file_paths.append(file_path)
        # build json return
        return json.dumps({'code' : 200, 'message' : str(file_paths) }, indent=2, separators=(',',': '))

    @log_timing(log)
    def create_zip(self):
        with tempfile.NamedTemporaryFile() as tzf:
            with zipfile.ZipFile(tzf.name, 'w') as zf:
                self.write_to_zipfile(zf)
            return tzf.read()

    @log_timing(log)
    def write_to_zipfile(self, zf):
        for stream_key, deployment, ds in self.stream_data.groups():
            with tempfile.NamedTemporaryFile() as tf:
                # interp to main times if more than one stream was in the request.
                self.to_netcdf(ds, tf.name)
                zf.write(tf.name, 'deployment%04d_%s.nc' % (deployment, stream_key.as_dashed_refdes(),))

    @log_timing(log)
    def to_netcdf(self, ds, file_path):
        if self.classic:
            for data_array_name in ds.data_vars:
                data_array = ds.get(data_array_name)
                data_type = data_array.dtype
                if data_type == numpy.int64 or data_type == numpy.uint32 or data_type == numpy.uint64:
                    ds[data_array_name] = self.convert_data_array(data_array, data_type, numpy.str)
                elif data_type == numpy.uint16:
                    ds[data_array_name] = self.convert_data_array(data_array, data_type, numpy.int32)
            ds.to_netcdf(path=file_path, format="NETCDF4_CLASSIC")
        else:
            ds.to_netcdf(file_path)

    @log_timing(log)
    def convert_data_array(self, data_array, orig_data_type, dest_data_type):
        data_array.attrs['original_dtype'] = str(orig_data_type)
        new_data_array = data_array.astype(dest_data_type)
        new_data_array.attrs = data_array.attrs
        new_data_array.name = data_array.name
        return new_data_array

@log_timing(log)
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
