import logging
import ion_functions

import util.stream_request
from jsonresponse import JsonResponse
from util.common import log_timing, StreamKey, TimeRange
from util.netcdf_generator import NetcdfGenerator

if hasattr(ion_functions, '__version__'):
    ION_VERSION = ion_functions.__version__
else:
    ION_VERSION = 'unversioned'

log = logging.getLogger(__name__)


def get_stream_request(streams, start, stop, coefficients, uflags, qc_parameters, limit=None,
                       include_provenance=False, include_annotations=False, strict_range=False,
                       location_information=None, request_uuid=''):
    stream_key = StreamKey.from_dict(streams[0])
    parameters = streams[0].get('parameters', [])
    time_range = TimeRange(start, stop)
    stream_request = util.stream_request.StreamRequest(stream_key, parameters, coefficients, time_range, uflags,
                                                       qc_parameters=qc_parameters,
                                                       limit=limit,
                                                       include_provenance=include_provenance,
                                                       include_annotations=include_annotations,
                                                       strict_range=strict_range,
                                                       location_information=location_information,
                                                       request_id=request_uuid)
    return stream_request


@log_timing(log)
def get_particles(streams, start, stop, coefficients, uflags, qc_parameters, limit=None,
                  include_provenance=False, include_annotations=False, strict_range=False,
                  location_information=None, request_uuid=''):
    stream_request = get_stream_request(streams, start, stop, coefficients, uflags,
                                        qc_parameters=qc_parameters, limit=limit,
                                        include_provenance=include_provenance,
                                        include_annotations=include_annotations,
                                        strict_range=strict_range,
                                        location_information=location_information,
                                        request_uuid=request_uuid)
    stream_request.fetch_raw_data()
    # do_qc_stuff(stream_key, stream_data, stream_request.parameters, qc_stream_parameters)
    stream_request.calculate_derived_products()
    return JsonResponse(stream_request).json()


@log_timing(log)
def get_netcdf(streams, start, stop, coefficients, uflags, qc_parameters, limit=None,
               include_provenance=False, include_annotations=False, strict_range=False,
               location_information=None, request_uuid='', disk_path=None, classic=False):
    stream_request = get_stream_request(streams, start, stop, coefficients, uflags,
                                        qc_parameters=qc_parameters, limit=limit,
                                        include_provenance=include_provenance,
                                        include_annotations=include_annotations,
                                        strict_range=strict_range,
                                        location_information=location_information,
                                        request_uuid=request_uuid)
    stream_request.fetch_raw_data()
    # do_qc_stuff(stream_key, stream_data, stream_request.parameters, qc_stream_parameters)
    stream_request.calculate_derived_products()
    return NetcdfGenerator(stream_request, classic, disk_path).write()


@log_timing(log)
def get_needs(streams, request_id):
    """
    Returns a list of required calibration constants for a list of streams
    """
    stream_key = StreamKey.from_dict(streams[0])
    parameters = streams[0].get('parameters', [])
    stream_request = util.stream_request.StreamRequest(stream_key, parameters, {}, None, None,
                                                       collapse_times=False, request_id=request_id)
    return stream_request.needs_cc

# def get_shape(param, base_key, pd_data):
#     tp = base_key.stream.time_parameter
#     try:
#         main_times = pd_data[tp][base_key.as_refdes()]['data']
#     except KeyError:
#         raise MissingTimeException("Could not find time parameter %s for %s" % (tp, base_key))
#
#     return len(main_times),
