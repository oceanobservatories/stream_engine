import json
import logging
import os

from engine import app

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
        stream_key = self.stream_request.stream_key
        dataset = self.stream_request.datasets[stream_key]
        return self._create_csv(dataset, None)

    def to_csv_files(self, path):
        file_paths = []
        base_path = os.path.join(app.config['ASYNC_DOWNLOAD_BASE_DIR'], path)

        if not os.path.isdir(base_path):
            os.makedirs(base_path)

        stream_key = self.stream_request.stream_key
        dataset = self.stream_request.datasets[stream_key]

        for deployment, ds in dataset.groupby('deployment'):
            refdes = stream_key.as_dashed_refdes()
            filename = 'deployment{:04d}_{:s}{:s}'.format(deployment, refdes, self._get_suffix())
            file_path = os.path.join(base_path, filename)
            with open(file_path, 'w') as filehandle:
                self._create_csv(ds, filehandle)
            file_paths.append(file_path)
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
