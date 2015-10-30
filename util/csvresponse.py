import json
import logging
import os
import tempfile
import csv
import numpy
import time
import xray

from engine.routes import app
from util.common import StreamEngineException

log = logging.getLogger(__name__)

class CSVGenerator(object):
    """
    Class to generate csv files for a stream engine response
    """

    def __init__(self, stream_data, delimiter, stream_param_map):
        self.stream_data = stream_data
        self.delimiter= delimiter
        self.stream_param_map = stream_param_map

    def chunks(self, disk_path=None):
        try:
            if disk_path is not None:
                return self.create_async_csv(disk_path)
            else:
                return self.create_sync_delimited()
        except GeneratorExit:
            raise
        except:
            log.exception("Unexpected error during csv generation")
            raise

    def get_suffix(self):
        """
        Get the suffix we should use to name file. Default to csv
        :return:
        """
        suffix_map ={',' : '.csv', '\t' : ".tsv"}
        if self.delimiter in suffix_map:
            return suffix_map[self.delimiter]
        else:
            log.warn('%s is not in suffix map returning using default csv', self.delimiter)
            return '.csv'

    def create_async_csv(self, path):
        file_paths = list()
        base_path = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'], path)
        if not os.path.isdir(base_path):
            os.makedirs(base_path)
        for stream_key, deployment, ds in self.stream_data.groups():
            file_path = '%s/deployment%04d_%s%s' % (base_path, deployment, stream_key.as_dashed_refdes(), self.get_suffix())
            with open(file_path, 'w') as fileout:
                to_use, attr = self._get_async_parameters(ds)
                self._write_csv_out(fileout, ds, to_use, attr)
            file_paths.append(file_path)
        return json.dumps({"code" : 200, "message" : str(file_paths)}, indent=2, separators=(',', ': '))


    def create_sync_delimited(self):
        # Currently this code assumes that the request only contained one stream. Otherwise code will break
        # in the future we may squash all of the differing streams into one result.
        if len(self.stream_param_map) > 1:
            raise StreamEngineException("Should not have more than one stream in request for delimited data", 500)
        datasets = []
        output_vars = []
        for sk, deployment, ds in self.stream_data.groups(self.stream_param_map.keys()[0]):
            ds, output_vars = self._fix_for_sync(ds, self.stream_param_map[sk])
            datasets.append(ds)
        final_data = xray.concat(datasets, dim='obs')
        final_data['obs'].values = numpy.arange(0, final_data['obs'].size, dtype=numpy.int32)
        key_vars = ['subsite', 'node', 'sensor', 'stream']
        # output columns in the correct order
        output_vars = output_vars[:4] + key_vars + output_vars[4:]
        final_data.attrs['subsite'] = datasets[0].attrs['subsite']
        final_data.attrs['node'] = datasets[0].attrs['node']
        final_data.attrs['sensor'] = datasets[0].attrs['sensor']
        final_data.attrs['stream'] = datasets[0].attrs['stream']
        with tempfile.NamedTemporaryFile() as tf:
            self._write_csv_out(tf.file, final_data, output_vars, set(key_vars))
            tf.seek(0)
            return tf.read()


    def _fix_for_sync(self, ds, params):
        for p in params:
            if p.name not in ds:
                log.warn("Dataset missing %s (%s) using fill value", p.name, p.id)
                arr = numpy.empty(len(ds.time), dtype=p.value_encoding)
                arr.fill(p.fill_value)
                dims = ['obs']
                if len(arr.shape) > 1:
                    for index, dim in enumerate(arr.shape[1:]):
                        name = "{:s}_dim_{:d}".format(p.name, index)
                        dims.append(name)
                ds.update({p.name : xray.DataArray(arr, dims=dims, attrs={'long_name' : p.name})})
        req = ['time', 'lat', 'lon', 'depth', 'deployment']
        to_use = [p.name for p in params if p.name  not in req]
        req.extend(to_use)
        return ds[req], req

    def _get_async_parameters(self, ds):
        to_exclude = set(['bin','id', 'l0_provenance_keys', 'l0_provenance_data','streaming_provenance', 'computed_provenance', 'query_parameter_provenance',
                          'provenance_messages', 'annotations'])
        to_use = ['time', 'lat', 'lon', 'depth', 'subsite', 'node', 'sensor', 'stream', 'deployment']
        not_time_based = []
        # variables gotten from dataset attributes
        attrs = set(['subsite', 'node', 'sensor', 'stream'])
        for param in ds.data_vars:
            if param in to_use or param in to_exclude:
                continue
            else:
                if 'obs' in ds[param].dims:
                    to_use.append(param)
                else:
                    not_time_based.append(param)
        log.warn("Following variables are not time based and will not be included in csv: %s",  not_time_based)
        return to_use, attrs

    def _write_csv_out(self, fileout,ds, to_use, attrs):
        # CSV variables to exclude
        writer = csv.DictWriter(fileout, to_use, delimiter=self.delimiter)
        writer.writeheader()
        attr_values = {a: ds.attrs[a] for a in attrs}
        data = {}
        for param in to_use:
            if param not in attrs:
                data[param] = ds[param].values
        data['time'] = ds.time.values
        for index in xrange(ds.time.size):
            row = {}
            for param in to_use:
                if param in attrs:
                    row[param] = attr_values[param]
                else:
                    row[param] = data[param][index]
                #TODO QC
            writer.writerow(row)
        fileout.flush()
