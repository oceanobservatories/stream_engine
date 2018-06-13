import json
import logging
import os
import shutil
import tempfile
import zipfile

from engine import app
from util.common import ntp_to_short_iso_datestring, get_annotation_filename, WriteErrorException

log = logging.getLogger(__name__)

# get pressure parameters -- used for filtering
PRESSURE_DPI = app.config.get('PRESSURE_DPI')
INT_PRESSURE_NAME = app.config.get('INT_PRESSURE_NAME')

class CsvGenerator(object):
    """
    Class to generate csv files for a stream engine response
    """
    suffix_map = {',': '.csv', '\t': ".tsv"}

    def __init__(self, stream_request, delimiter):
        self.stream_request = stream_request
        self.delimiter = delimiter

    def to_csv(self):
        return self._create_zip()
        
    def _create_zip(self):
        with tempfile.NamedTemporaryFile() as tzf:
            with zipfile.ZipFile(tzf.name, 'w') as zf:
                temp_dir = tempfile.mkdtemp()
                file_paths = self._create_files(temp_dir)
                for file_path in file_paths:
                    zf.write(file_path, os.path.basename(file_path))
                shutil.rmtree(temp_dir)
            return tzf.read()

    def to_csv_files(self, path):
        file_paths = []
        base_path = os.path.join(app.config['LOCAL_ASYNC_DIR'], path)

        if not os.path.isdir(base_path):
            try:
                os.makedirs(base_path)
            except OSError:
                if not os.path.isdir(base_path):
                    raise WriteErrorException('Unable to create local output directory: %s' % path)

        file_paths = self._create_files(base_path)
        return json.dumps({"code": 200, "message": str(file_paths)}, indent=2)

    def _create_filter_list(self,keys):
        """
        generates a set of parameters to filter (remove) from the package
        :param keys: list of keys from the dataset.
        :return: set containing keys to filter from the dataset.
        """
        # default parameters -- move to class variable?
        default = ['time', 'deployment', 'lat', 'lon']

        # initialize param list to include requested and default parameters
        missing_params = []
        params_to_include = []
        # check for and remove any missing params from the requested list
        for param in self.stream_request.requested_parameters:
            if param.name not in keys:
                missing_params.append(param.name)
            else:
                params_to_include.append(param.name)
        if missing_params:
            log.warning('one or more selected parameters (%s) not found in the dataset',missing_params)

        # add the default params to the inclusion list
        params_to_include.extend(default)

        # Determine if there is interpolated pressure parameter. if so include it
        pressure_params = [(sk,param) for sk in self.stream_request.external_includes
                            for param in self.stream_request.external_includes[sk]
                            if param.data_product_identifier == PRESSURE_DPI]
        if pressure_params:
            params_to_include.append(INT_PRESSURE_NAME)

        # create the list of fields to remove from the dataset
        # start with fields we never want to include
        drop = {'id', 'annotations'}
        for key in keys:
            # remove any "extra" keys while keeping qc params and removing 'provenance' params
            if (key not in params_to_include and not self._is_qc_param(key)) or 'provenance' in key:
                drop.add(key)
        return drop

    @staticmethod
    def _is_qc_param(param):
        return 'qc_executed' in param or 'qc_results' in param

    def _create_csv(self, dataset, filehandle):
        """
        Performs the steps needed to filter the dataset and
        write the filtered dataset to a CSV file for packaging.
        :param dataset: data set to filter and write to file
        :param filehandle: filehandle of CSV file
        :return: None
        """
        # generate the filtering list
        drop = self._create_filter_list(dataset.data_vars.keys())

        # filter the dataset
        dataset = dataset.drop(drop.intersection(dataset))

        # write the CSV -- note to_csv() returns none
        dataset.to_dataframe().to_csv(path_or_buf=filehandle, sep=self.delimiter)

    def _create_files(self, base_path):
        file_paths = []
        
        # annotation data will be written to a JSON file
        if self.stream_request.include_annotations:
            anno_fname = get_annotation_filename(self.stream_request)
            anno_json = os.path.join(base_path, anno_fname)
            file_paths.append(anno_json)
            self.stream_request.annotation_store.dump_json(anno_json)
        
        stream_key = self.stream_request.stream_key
        stream_dataset = self.stream_request.datasets[stream_key]
        for deployment, ds in stream_dataset.datasets.iteritems():
            times = ds.time.values
            start = ntp_to_short_iso_datestring(times[0])
            end = ntp_to_short_iso_datestring(times[-1])
            
            # provenance types will be written to JSON files
            if self.stream_request.include_provenance:
                prov_fname = 'deployment%04d_%s_provenance_%s-%s.json' % (deployment,
                                                                          stream_key.as_dashed_refdes(), start, end)
                prov_json = os.path.join(base_path, prov_fname)
                file_paths.append(prov_json)
                stream_dataset.provenance_metadata.dump_json(prov_json)

            filename = 'deployment%04d_%s_%s-%s%s' % (deployment, stream_key.as_dashed_refdes(), start, end,
                                                      self._get_suffix())
            file_path = os.path.join(base_path, filename)

            with open(file_path, 'w') as filehandle:
                self._create_csv(ds, filehandle)
            file_paths.append(file_path)
        return file_paths
        
    def _get_suffix(self):
        """
        Get the suffix we should use to name file. Default to csv
        :return:
        """
        if self.delimiter in self.suffix_map:
            return self.suffix_map[self.delimiter]
        else:
            log.warn('%s is not in suffix map returning using default csv', self.delimiter)
            return '.csv'
