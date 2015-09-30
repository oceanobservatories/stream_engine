import json
import logging
import os
import tempfile
import zipfile
import csv

from engine.routes import app

log = logging.getLogger(__name__)

class CSVGenerator(object):
    """
    Class to generate csv files for a stream engine response
    """


    def __init__(self, stream_data, delimiter=','):
        self.stream_data = stream_data
        self.delimiter= delimiter
    def chunks(self, disk_path=None):
        try:
            if disk_path is not None:
                return self.create_async_csv(disk_path)
            else:
                return self.create_zip_csv()
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
                self._write_csv_out(fileout, stream_key, deployment, ds)
            file_paths.append(file_path)
        return json.dumps({"code" : 200, "message" : str(file_paths)}, indent=2, separators=(',', ': '))


    def create_zip_csv(self):
        with tempfile.NamedTemporaryFile() as tzf:
            with zipfile.ZipFile(tzf.name, 'w') as zf:
                self.write_to_zipfile(zf)
            return tzf.read()

    def write_to_zipfile(self, zf):
        for stream_key, deployment, ds in self.stream_data.groups():
            with tempfile.NamedTemporaryFile() as tf:
                self._write_csv_out(tf.file, stream_key, deployment, ds)
                zf.write(tf.name, 'deployment%04d_%s%s' % (deployment, stream_key.as_dashed_refdes(),self.get_suffix(),))

    def _write_csv_out(self, fileout, stream_key, deployment, ds):
        # CSV variables to exclude
        to_exclude = set(['bin','id', 'l0_provenance_keys', 'l0_provenance_data','streaming_provenance', 'computed_provenance', 'query_parameter_provenance',
                          'provenance_messages', 'annotations'])
        to_use = ['time', 'subsite', 'node', 'sensor', 'stream', 'deployment']
        not_time_based = []
        # variables gotten from dataset attributes
        attrs = set(['subsite', 'node', 'sensor', 'stream', 'deployment'])
        for param in ds.data_vars.iterkeys():
            if param == 'time' or param in to_exclude:
                continue
            else:
                if 'time' in ds[param].dims:
                    to_use.append(param)
                else:
                    not_time_based.append(param)
        log.warn("Following variables are not time based and will not be included in csv: %s",  not_time_based)
        writer = csv.DictWriter(fileout, to_use, delimiter=self.delimiter)
        writer.writeheader()
        for index in xrange(len(ds['time'])):
            row = {}
            for param in to_use:
                if param in attrs:
                    row[param] = ds.attrs[param]
                else:
                    row[param] = ds[param].values[index]

                #TODO QC
                # qc_postfixes = ['qc_results', 'qc_executed']
                # for qc_postfix in qc_postfixes:
                #     qc_key = '%s_%s' % (param.name, qc_postfix)
                #     if qc_key in ds:
                #         particle[qc_key] = ds[qc_key].values[index]
            writer.writerow(row)
        fileout.flush()
