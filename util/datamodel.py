"""
This module is for classes and functions related to the data structures
stream_engine uses to model science data.  Data is currently being backed by a
dictionary representation boiling down to raw numpy ndarrays, but in the future
data will be in xray Datasets.
"""
__author__ = 'Stephen Zakrewsky'


from common import CachedParameter, get_stream_key_with_param, MissingTimeException
import datetime
from engine import app
import json
import logging
import numpy as np
import traceback
import xray
import xray_interpolation as xinterp


log = logging.getLogger(__name__)


def _open_new_ds(stream_key, provenance_metadata=None, annotation_store=None):
    # set up file level attributes
    attrs = {
        'subsite': stream_key.subsite,
        'node': stream_key.node,
        'sensor': stream_key.sensor,
        'collection_method': stream_key.method,
        'stream': stream_key.stream.name,
        'title' : '{:s} for {:s}'.format(app.config['NETCDF_TITLE'], stream_key.as_dashed_refdes()),
        'institution' : '{:s}'.format(app.config['NETCDF_INSTITUTION']),
        'source' : '{:s}'.format(stream_key.as_dashed_refdes()),
        'history' : '{:s} {:s}'.format(datetime.datetime.utcnow().isoformat(), app.config['NETCDF_HISTORY_COMMENT']),
        'references' : '{:s}'.format(app.config['NETCDF_REFERENCE']),
        'comment' : '{:s}'.format(app.config['NETCDF_COMMENT']),
        'Conventions' : '{:s}'.format(app.config['NETCDF_CONVENTIONS'])
    }

    init_data = {}
    if provenance_metadata is not None:
        prov = provenance_metadata.get_provenance_dict()
        keys = []
        values = []
        for k,v in prov.iteritems():
            keys.append(k)
            values.append(v['file_name'] + " " + v['parser_name'] + " " + v['parser_version'])

        if len(keys) > 0:
            init_data['l0_provenance_keys'] = xray.DataArray(np.array(keys), dims=['l0_provenance'],
                                                             attrs={'long_name': 'l0 Provenance Keys'})
        if len(values) > 0:
            init_data['l0_provenance_data'] = xray.DataArray(np.array(values), dims=['l0_provenance'],
                                                             attrs={'long_name': 'l0 Provenance Entries'})
        comp_prov = [json.dumps(provenance_metadata.calculated_metatdata.get_dict())]
        if len(comp_prov) > 0:
            init_data['computed_provenance'] = xray.DataArray(comp_prov, dims=['computed_provenance_dim'],
                                                              attrs={'long_name': 'Computed Provenance Information'})

        query_prov = [json.dumps(provenance_metadata.get_query_dict())]
        if len(query_prov) > 0:
            init_data['query_parameter_provenance'] = xray.DataArray(query_prov,
                                                                     dims=['query_parameter_provenance_dim'],
                                                                     attrs={
                                                                     'long_name': 'Query Parameter Provenance Information'})
        if len(provenance_metadata.messages) > 0:
            init_data['provenance_messages'] = xray.DataArray(provenance_metadata.messages,
                                                              dims=['provenance_messages'],
                                                              attrs={'long_name': 'Provenance Messages'})

    if annotation_store is not None:
        annote = annotation_store.get_json_representation()
        annote_data = [json.dumps(x) for x in annote]
        if len(annote_data) > 0:
            init_data['annotations'] = xray.DataArray(np.array(annote_data), dims=['dataset_annotations'],
                                                      attrs={'long_name': 'Data Annotations'})
    return xray.Dataset(init_data, attrs=attrs)


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


def _group_by_stream_key(ds, pd_data, stream_key):
    time_data, time_parameter = _get_time_data(pd_data, stream_key)
    # sometimes we will get duplicate timestamps
    # INITIAL solution is to remove any duplicate timestamps
    # and the corresponding data by creating a mask to match
    # only valid INCREASING times
    mask = np.diff(np.insert(time_data, 0, 0.0)) != 0
    time_data = time_data[mask]
    time_data = xray.Variable('time',time_data,  attrs={'units' : 'seconds since 1900-01-01 0:0:0',
                                                        'standard_name' : 'time',
                                                        'long_name'  : 'time',
                                                        'calendar' : 'standard'})

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
            try:
                # In order to comply with CF1.6 we must use "classic" netcdf4.  This donesn't allow unsigned values
                if param.value_encoding not in ['uint8', 'uint16', 'uint32', 'uint64']:
                    data = data.astype(param.value_encoding)
                else:
                    log.warn("Netcdf4 Classic does not allow unsigned integers")
            except ValueError:
                log.warning(
                    'Unable to transform data %s named %s of type %s to preload value_encoding %s, using fill_value instead\n%s' % (
                    param_id, param_name, data.dtype, param.value_encoding,
                    traceback.format_exc()))
                data = np.full(data.shape, param.fill_value, param.value_encoding)

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

        ds.update({param_name: xray.DataArray(data, dims=dims, coords=coords, attrs=array_attrs)})


class StreamData(object):

    def __init__(self, stream_request, data, provenance_metadata, annotation_store):
        self.stream_keys = stream_request.stream_keys
        self.data = data
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

        for sk in stream_keys:
            for d in deployments:
                if self.check_stream_deployment(sk, d):
                    pd_data = self.data[d]
                    ds = _open_new_ds(sk, self.provenance_metadata, self.annotation_store)
                    _group_by_stream_key(ds, pd_data, sk)
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
