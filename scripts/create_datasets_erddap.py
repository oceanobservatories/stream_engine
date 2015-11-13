__author__ = 'ataylor'

import os

ASYNC_BASE_DIR = '/opt/ooi/async'
# NEED TO ENSURE THAT USER EXECUTING THIS SCRIPT HAS PERMISSION TO EDIT ERRDAP datasets file and that
# ERRDAP is configured to reload all datasets every time this script is run.
ERRDAP_DATSETS_FILE = '/usr/local/apache-tomcat-7.0.59/content/erddap/datasets.xml'

if __name__ == '__main__':
    datasets_string = ['\n']
    for directory, _, files in os.walk(ASYNC_BASE_DIR):
        for f in files:
            if f.endswith('_erddap.xml'):
                print directory
                with open(os.path.join(directory, f)) as dataset_file:
                    datasets_string.extend(dataset_file.readlines())
                    datasets_string.append('\n')
    datasets_string.append('\n')
    with open(ERRDAP_DATSETS_FILE, 'w') as master_ds_file:
        master_ds_file.write('<?xml version="1.0" encoding="ISO-8859-1" ?>\n<erddapDatasets>')
        for i in datasets_string:
            master_ds_file.write(i)
        master_ds_file.write('</erddapDatasets>')



