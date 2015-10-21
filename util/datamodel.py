"""
This module is for classes and functions related to the data structures
stream_engine uses to model science data.  Data is currently being backed by a
dictionary representation boiling down to raw numpy ndarrays, but in the future
data will be in xray Datasets.
"""
import uuid

__author__ = 'Stephen Zakrewsky'


from common import CachedParameter, get_stream_key_with_param, MissingTimeException, ntp_to_ISO_date
import datetime
from engine import app
import json
import logging
import numpy as np
import traceback
import xray
import xray_interpolation as xinterp


log = logging.getLogger(__name__)


def _open_new_ds(stream_key, deployment, request_uuid, provenance_metadata=None, annotation_store=None):
    # set up file level attributes
    attrs = {
        'subsite': stream_key.subsite,
        'node': stream_key.node,
        'sensor': stream_key.sensor,
        'collection_method': stream_key.method,
        'stream': stream_key.stream.name,
        'deployment' : deployment,
        'title' : '{:s} for {:s}'.format(app.config['NETCDF_TITLE'], stream_key.as_dashed_refdes()),
        'institution' : '{:s}'.format(app.config['NETCDF_INSTITUTION']),
        'source' : '{:s}'.format(stream_key.as_dashed_refdes()),
        'history' : '{:s} {:s}'.format(datetime.datetime.utcnow().isoformat(), app.config['NETCDF_HISTORY_COMMENT']),
        'references' : '{:s}'.format(app.config['NETCDF_REFERENCE']),
        'comment' : '{:s}'.format(app.config['NETCDF_COMMENT']),
        'Conventions' : '{:s}'.format(app.config['NETCDF_CONVENTIONS']),
        'Metadata_Conventions' : '{:s}'.format(app.config['NETCDF_METADATA_CONVENTIONS']),
        'feature_Type' : '{:s}'.format(app.config['NETCDF_FEATURE_TYPE']),
        'featureType' : '{:s}'.format(app.config['NETCDF_FEATURE_TYPE']),
        'cdm_data_type' : '{:s}'.format(app.config['NETCDF_CDM_DATA_TYPE']),
        'nodc_template_version' : '{:s}'.format(app.config['NETCDF_NODC_TEMPLATE_VERSION']),
        'standard_name_vocabulary' : '{:s}'.format(app.config['NETCDF_STANDARD_NAME_VOCABULARY']),
        'summary' : '{:s}'.format(app.config['NETCDF_SUMMARY']),
        'uuid' : '{:s}'.format(str(request_uuid)),
        'requestUUID' : '{:s}'.format(str(request_uuid)),
        'id' : '{:s}'.format(stream_key.as_dashed_refdes()),
        'naming_authority' : '{:s}'.format(app.config['NETCDF_NAMING_AUTHORITY']),
        'creator_name' : '{:s}'.format(app.config['NETCDF_CREATOR_NAME']),
        'creator_url' : '{:s}'.format(app.config['NETCDF_CREATOR_URL']),
        'creator_email' : '{:s}'.format(app.config['NETCDF_CREATOR_EMAIL']),
        'project' : '{:s}'.format(app.config['NETCDF_PROJECT']),
        'processing_level' : '{:s}'.format(app.config['NETCDF_PROCESSING_LEVEL']),
        'keywords_vocabulary' : '{:s}'.format(app.config['NETCDF_KEYWORDS_VOCABULARY']),
        'keywords' : '{:s}'.format(app.config['NETCDF_KEYWORDS']),
        'acknowledgement' : '{:s}'.format(app.config['NETCDF_ACKNOWLEDGEMENT']),
        'contributor_name' : '{:s}'.format(app.config['NETCDF_CONTRIBUTOR_NAME']),
        'contributor_role' : '{:s}'.format(app.config['NETCDF_CONTRIBUTOR_ROLE']),
        'date_created' : '{:s}'.format(datetime.datetime.utcnow().isoformat()),
        'date_modified' : '{:s}'.format(datetime.datetime.utcnow().isoformat()),
        'publisher_name' : '{:s}'.format(app.config['NETCDF_PUBLISHER_NAME']),
        'publisher_url' : '{:s}'.format(app.config['NETCDF_PUBLISHER_URL']),
        'publisher_email' : '{:s}'.format(app.config['NETCDF_PUBLISHER_EMAIL']),
        'license' : '{:s}'.format(app.config['NETCDF_LICENSE']),
    }

    init_data = {}
    return xray.Dataset(attrs=attrs)


def _get_time_data(pd_data, stream_key):
    """
    Get the time data for the stream.  Can handle virtual streams.
    :param pd_data: data structure
    :param stream_key: stream key
    :return: tuple of time data as array and the time parameter parameter key
    """
    if stream_key.stream.is_virtual:
        source_stream = stream_key.stream.source_streams[0]
        stream_key = get_stream_key_with_param(pd_data, source_stream, source_stream.time_parameter)

    tp = stream_key.stream.time_parameter
    try:
        return pd_data[tp][stream_key.as_refdes()]['data'], tp
    except KeyError:
        raise MissingTimeException("Could not find time parameter %s for %s" % (tp, stream_key))


def _group_by_stream_key(ds, pd_data, stream_key, location_information, deployment, provenance_metadata, annotation_store):
    time_data, time_parameter = _get_time_data(pd_data, stream_key)
    # sometimes we will get duplicate timestamps
    # INITIAL solution is to remove any duplicate timestamps
    # and the corresponding data by creating a mask to match
    # only valid INCREASING times
    mask = np.diff(np.insert(time_data, 0, 0.0)) != 0
    time_data = time_data[mask]
    attrs = {'units': 'seconds since 1900-01-01 0:0:0', 'standard_name': 'time',
             'long_name': 'time',
             'axis': 'T',
             'calendar': app.config["NETCDF_CALENDAR_TYPE"]}
    ds['time'] = ('obs', time_data, attrs)
    # put in lat and lon here
    for param_id in pd_data:
        if (
            param_id == time_parameter or
            stream_key.as_refdes() not in pd_data[param_id]
           ):
            continue
        param = CachedParameter.from_id(param_id)
        # param can be None if this is not a real parameter,
        # like deployment for deployment number
        param_name = param_id if param is None else param.name

        data = pd_data[param_id][stream_key.as_refdes()]['data'][mask]
        if param is not None:
            data = data.astype(param.value_encoding)
        else:
            if param_name in app.config['INTERNAL_OUTPUT_MAPPING']:
                data = data.astype(app.config['INTERNAL_OUTPUT_MAPPING'][param_name])

        if param_name in app.config['INTERNAL_OUTPUT_EXCLUDE_LIST']:
            continue


        dims = ['obs']
        if len(data.shape) > 1:
            for index, dimension in enumerate(data.shape[1:]):
                name = '%s_dim_%d' % (param_name, index)
                dims.append(name)

        if param_name in ['lat', 'lon', 'depth']:
            array_attrs = {}
        else:
            array_attrs = {'coordinates' : 'time lat lon depth'}
        if param:
            if param.unit is not None:
                array_attrs['units'] = param.unit
                if param.unit.startswith('seconds since'):
                    array_attrs['calendar'] =  app.config["NETCDF_CALENDAR_TYPE"]
            if param.fill_value is not None:
                array_attrs['_FillValue'] = param.fill_value
            # Long name needs to be display name to comply with cf 1.6.
            # http://cfconventions.org/Data/cf-conventions/cf-conventions-1.6/build/cf-conventions.html#long-name
            if param.display_name is not None:
                array_attrs['long_name'] = param.display_name
            elif param.name is not None:
                array_attrs['long_name'] = param.name
            else:
                log.warn('Could not produce long_name attribute for {:s} defaulting to parameter name'.format(str(param_name)))
                array_attrs['long_name'] = param_name
            if param.standard_name is not None:
                array_attrs['standard_name'] = param.standard_name
            if param.description is not None:
                array_attrs['comment'] = param.description
            if param.data_product_identifier is not None:
                array_attrs['data_product_identifier'] = param.data_product_identifier
        else:
            # To comply with cf 1.6 giving long name the same as parameter name
            array_attrs['long_name'] = param_name

        ds[param_name] = (dims, data, array_attrs)

    fix_lat_lon_depth(ds, stream_key, deployment, location_information )
    # Add in l0 provenance here:
    if provenance_metadata is not None:
        prov = provenance_metadata.get_provenance_dict()
        values = {}
        for k,v in prov.iteritems():
            values[k]= v['file_name'] + " " + v['parser_name'] + " " + v['parser_version']
        prov_array = [values[d] for d in ds.provenance.values]
        log.warn(prov_array)
        prov_array = np.array(prov_array).astype(str)
        log.warn(prov_array)
        ds['l0_provenance_data'] = ('obs', prov_array,{'long_name': 'l0_provenance_values', 'coordinates': 'time lat lon depth'} )

        streaming = provenance_metadata.get_streaming_provenance()
        if len(streaming) > 0:
            s = json.dumps(streaming)
            da = np.array([s for _ in range(ds.obs.size)]).astype(str)
            ds['streaming_provenance'] = ('obs', da, {'long_name': 'Streaming Provenance Information', 'coordinates': 'time lat lon depth'})

        s = json.dumps(provenance_metadata.calculated_metatdata.get_dict())
        da = np.array([s for _ in range(ds.obs.size)]).astype(str)
        ds['computed_provenance'] = ('obs', da,  {'long_name': 'Computed Provenance Information', 'coordinates': 'time lat lon depth'})

        s = json.dumps(provenance_metadata.get_query_dict())
        da = np.array([s for _ in range(ds.obs.size)]).astype(str)
        ds['query_parameter_provenance'] = ('obs', da, {'long_name': 'Query Parameter Provenance Information', 'coordinates': 'time lat lon depth'})
        if len(provenance_metadata.messages) > 0:
            s = json.dumps(provenance_metadata.messages)
            da = np.array([s for _ in range(ds.obs.size)]).astype(str)
            ds['provenance_messages'] = ('obs', da,  {'long_name': 'Provenance Messages', 'coordinates': 'time lat lon depth'})

    if annotation_store is not None:
        annote = annotation_store.get_json_representation()
        s = json.dumps(annote)
        da = np.array([s for _ in range(ds.obs.size)]).astype(str)
        ds['annotations'] = ('obs', da, {'long_name': 'Data Annotations', 'coordinates': 'time lat lon depth'})


def fix_lat_lon_depth(ds, stream_key, deployment, location_information):
    location_vals = {}
    for loc in location_information.get(stream_key.as_three_part_refdes(), []):
        if loc['deployment'] == deployment:
            location_vals = loc
            break
    if 'lat' not in ds.variables:
        lat = location_vals.get('lat')
        if lat is None:
            log.warn('No latitude!! Using fill value')
            lat = 90.0
        latarr = np.empty(ds.time.size)
        latarr.fill(lat)
        ds['lat'] = ('obs', latarr, {'axis': 'Y', 'units': 'degrees_north', 'standard_name': 'latitude'})
    else:
        ds['lat'].attr['axis'] = 'Y'
        ds['lat'].standard_name = 'latitude'

    if 'lon' not in ds.variables:
        lon = location_vals.get('lon')
        if lon is None:
            log.warn('No longitude!! Using fill value')
            lon = -180.0
        lonarr = np.empty(ds.time.size)
        lonarr.fill(lon)
        ds['lon'] = ('obs', lonarr, {'axis': 'X', 'units': 'degrees_east', 'standard_name': 'longitude'})
    else:
        ds['lon'].attr['axis'] = 'X'
        ds['lon'].standard_name = 'longitude'

    if 'depth' not in ds.variables:
        depth = location_vals.get('depth', 0.0)
        if depth is None:
            log.warn("Depth not present using fill value")
            depth = 0.0
        deptharr = np.empty(ds.time.size)
        deptharr.fill(depth)
        attrs = {'standard_name': app.config["Z_STANDARD_NAME"], 'long_name': app.config["Z_LONG_NAME"], 'units': 'm',
                 'positive': app.config['Z_POSITIVE'], 'axis': 'Z'}
        ds['depth'] = ('obs', deptharr, attrs)



def _add_dynamic_attributes(ds, stream_key, location_information, deployment):
    time_data = ds['time']
    # Do a final update to insert the time_coverages, and geospatial lat and lons
    ds.attrs['time_coverage_start'] = ntp_to_ISO_date(time_data.values[0])
    ds.attrs['time_coverage_end'] = ntp_to_ISO_date(time_data.values[-1])
    # Take an estimate of the number of seconds between values in the data range.
    if len(time_data.values > 0):
        total_time = time_data.values[-1]  - time_data.values[0]
        hz = total_time / float(len(time_data.values))
        ds.attrs['time_coverage_resolution'] = 'P{:.2f}S'.format(hz)
    else:
        ds.attrs['time_coverage_resolution'] = 'P0S'

    location_vals = {}
    for loc in location_information.get(stream_key.as_three_part_refdes(), []):
        if loc['deployment'] == deployment:
            location_vals = loc
            break
    if 'location_name' in location_vals:
        ds.attrs['location_name'] = str(location_vals['location_name'])
    ds.attrs['geospatial_lat_min']  = min(ds.variables['lat'].values)
    ds.attrs['geospatial_lat_max']  = max(ds.variables['lat'].values)
    ds.attrs['geospatial_lon_min']  = min(ds.variables['lon'].values)
    ds.attrs['geospatial_lon_max']  = max(ds.variables['lon'].values)
    ds.attrs['geospatial_lat_units']  = 'degrees_north'
    ds.attrs['geospatial_lat_resolution']  = app.config["GEOSPATIAL_LAT_LON_RES"]
    ds.attrs['geospatial_lon_units']  = 'degrees_east'
    ds.attrs['geospatial_lon_resolution']  = app.config["GEOSPATIAL_LAT_LON_RES"]
    depth_units = str(location_vals.get('depth_units', app.config["Z_DEFAULT_UNITS"]))
    ds.attrs['geospatial_vertical_units']  = depth_units
    ds.attrs['geospatial_vertical_resolution']  = app.config['Z_RESOLUTION']
    ds.attrs['geospatial_vertical_positive']  = app.config['Z_POSITIVE']


class StreamData(object):

    def __init__(self, stream_request, data, provenance_metadata, annotation_store):
        self.stream_request = stream_request
        self.stream_keys = stream_request.stream_keys
        self.data = data
        self.location_information = stream_request.location_information
        if stream_request.include_annotations:
            self.annotation_store = annotation_store
        else:
            self.annotation_store = None
        if stream_request.include_provenance:
            self.provenance_metadata = provenance_metadata
        else:
            self.provenance_metadata = None
        self.deployments = sorted(data.keys())
        self.deployment_streams = self._build_deployment_stream_map(stream_request, data)
        self.deployment_times = {}

    def _build_deployment_stream_map(self, sr, data):
        dp_map = {}
        for deployment in data:
            streams = set()
            dep_data = data[deployment]
            for sk in sr.stream_keys:
                tp = sk.stream.time_parameter
                if tp in dep_data:
                    if sk.as_refdes() in dep_data[tp]:
                        streams.add(sk.as_refdes())
            dp_map[deployment] = streams
        return dp_map

    def groups(self, stream_key=None, deployment=None):
        if stream_key is None:
            stream_keys = self.stream_keys
        else:
            stream_keys = [stream_key]

        if deployment is None:
            deployments = self.deployments
        else:
            deployments = [deployment]

        request_id = self.stream_request.request_id
        for sk in stream_keys:
            for d in deployments:
                if self.check_stream_deployment(sk, d):
                    pd_data = self.data[d]
                    ds = _open_new_ds(sk, d, request_id, self.provenance_metadata, self.annotation_store)
                    _group_by_stream_key(ds, pd_data, sk, self.location_information, d, self.provenance_metadata, self.annotation_store)
                    _add_dynamic_attributes(ds, sk, self.location_information, d)
                    times = self.deployment_times.get(d, None)
                    if times is not None:
                        ds = xinterp.interp1d_Dataset(ds, time=times)
                    yield sk, d, ds

    def check_stream_deployment(self, stream_key, deployment):
        if deployment not in self.data:
            return False
        if stream_key.as_refdes() not in self.deployment_streams[deployment]:
            return False
        return True

    def get_time_data(self, stream_key):
        deployment_times = {}
        for d in self.deployments:
            pd_data = self.data[d]
            deployment_times[d] = _get_time_data(pd_data, stream_key)[0]
        return deployment_times
