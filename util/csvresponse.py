import json
import logging
import os
import shutil
import tempfile
import zipfile

from engine import app
from util.common import ntp_to_short_iso_datestring, WriteErrorException

log = logging.getLogger(__name__)


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

    def _create_csv(self, dataset, filehandle):
        # Drop fields we never want to output
        drop = {'bin', 'id', 'annotations'}
        dataset = dataset.drop(drop.intersection(dataset))

        # Drop all data with provenance in the name
        drop_prov = {k for k in dataset if 'provenance' in k}
        dataset = dataset.drop(drop_prov)

        # Write as CSV
        return dataset.to_dataframe().to_csv(path_or_buf=filehandle, sep=self.delimiter)
        
    def _create_files(self, base_path):
        file_paths = []
        
        # annotation data will be written to a JSON file
        if self.stream_request.include_annotations:
            time_range_string = str(self.stream_request.time_range).replace(" ", "")
            anno_fname = 'annotations_%s.json' % time_range_string
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
