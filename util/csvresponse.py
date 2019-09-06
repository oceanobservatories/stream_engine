import json
import logging
import os
import shutil
import tempfile
import zipfile

from engine import app
from util.common import ntp_to_short_iso_datestring, get_annotation_filename, WriteErrorException, is_qc_parameter

log = logging.getLogger(__name__)

# get pressure parameters -- used for filtering
PRESSURE_DPI = app.config.get('PRESSURE_DPI')
INT_PRESSURE_NAME = app.config.get('INT_PRESSURE_NAME')

# default parameters to request
DEFAULT_PARAMETERS = ['time', 'deployment', 'lat', 'lon', 'm_gps_lat', 'm_gps_lon']

# Glider GPS based lat/lon strings - for filtering
GLIDER_GPS_BASED_LAT = 'glider_gps_position-m_gps_lat'
GLIDER_GPS_BASED_LON = 'glider_gps_position-m_gps_lon'
GLIDER_DR_LAT = 'glider_gps_position-m_lat'
GLIDER_DR_LON = 'glider_gps_position-m_lon'


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

    def _create_filter_list(self, keys):
        """
        Generates a set of parameters to filter (remove) from the package
        :param keys: list of keys from the dataset.
        :return: set containing keys to filter from the dataset.
        """
        # initialize param list to include requested and default parameters
        missing_params = []
        params_to_include = list(DEFAULT_PARAMETERS)

        # check for and remove any missing params from the requested list
        for param in self.stream_request.requested_parameters:
            if param.name not in keys:
                missing_params.append(param.name)
            else:
                params_to_include.append(param.name)
        if missing_params:
            log.warning('one or more selected parameters (%s) not found in the dataset', missing_params)

        # Determine if there is interpolated pressure parameter. if so include it
        pressure_params = [(sk, param) for sk in self.stream_request.external_includes
                           for param in self.stream_request.external_includes[sk]
                           if param.data_product_identifier == PRESSURE_DPI]
        if pressure_params:
            params_to_include.append(INT_PRESSURE_NAME)

        # include any externals
        for extern in self.stream_request.external_includes:
            for param in self.stream_request.external_includes[extern]:
                name = '-'.join((extern.stream_name, param.name))
                params_to_include.append(name)

        # create the list of fields to remove from the dataset
        # start with fields we never want to include
        drop = {'id', 'annotations'}
        for key in keys:
            # remove any "extra" keys while keeping relevant qc params and removing 'provenance' params
            if is_qc_parameter(key):
                # drop QC param if not related to requested param
                if (not key.split('_qc_')[0] in params_to_include) \
                        and (not key.split('_qartod_')[0] in params_to_include):
                    drop.add(key)
            elif key not in params_to_include or 'provenance' in key:
                # drop "extra" params and "provenance" params
                drop.add(key)
        return drop

    def _create_csv(self, dataset, filehandle):
        """
        Performs the steps needed to filter the dataset and
        write the filtered dataset to a CSV file for packaging.
        :param dataset: data set to filter and write to file
        :param filehandle: filehandle of CSV file
        :return: None
        """
        # for a glider, get the lat lon
        if self.stream_request.stream_key.is_glider:
            if GLIDER_GPS_BASED_LAT in dataset.data_vars.keys() and GLIDER_GPS_BASED_LON in dataset.data_vars.keys():
                lat_data = dataset.data_vars[GLIDER_GPS_BASED_LAT]
                dataset = dataset.drop(GLIDER_GPS_BASED_LAT)
                dataset['m_gps_lat'] = lat_data
                lon_data = dataset.data_vars[GLIDER_GPS_BASED_LON]
                dataset = dataset.drop(GLIDER_GPS_BASED_LON)
                dataset['m_gps_lon'] = lon_data
            if GLIDER_DR_LAT in dataset.data_vars.keys() and GLIDER_DR_LON in dataset.data_vars.keys():
                lat_data = dataset.data_vars[GLIDER_DR_LAT]
                dataset = dataset.drop(GLIDER_DR_LAT)
                dataset['lat'] = lat_data
                lon_data = dataset.data_vars[GLIDER_DR_LON]
                dataset = dataset.drop(GLIDER_DR_LON)
                dataset['lon'] = lon_data

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
