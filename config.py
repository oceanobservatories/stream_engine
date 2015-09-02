DEBUG = False
STREAM_ENGINE_VERSION = "0.7.6"

CASSANDRA_CONTACT_POINTS = ['127.0.0.1']
CASSANDRA_KEYSPACE = 'ooi'
CASSANDRA_CONNECT_TIMEOUT = 60
CASSANDRA_FETCH_SIZE = 1000
CASSANDRA_DEFAULT_TIMEOUT = 60
CASSANDRA_QUERY_CONSISTENCY = 'QUORUM'

SAN_BASE_DIRECTORY = '/opt/ooi/SAN/'
SAN_CASS_OVERWRITE = True # When loading data back into cassandra should we allow writing to an already present databin.
ANNOTATION_URL = 'http://localhost:12580/annotations/find/'

UNBOUND_QUERY_START  = 3471292800 # Where to start unbound queries 2010-01-01T00:00:00.000Z

POOL_SIZE = 4

LOGGING_CONFIG='logging.conf'

NETCDF_TITLE = "Data produced by Stream Engine version {:s}".format(STREAM_ENGINE_VERSION)
NETCDF_INSTITUTION = "Ocean Observatories Initiative"
NETCDF_HISTORY_COMMENT = "generated from Stream Engine"
NETCDF_REFERENCE = "More information can be found at http://oceanobservatories.org/"
NETCDF_COMMENT = ""
NETCDF_CONVENTIONS = "CF-1.6"


COLLAPSE_TIMES = True

UI_FULL_BIN_LIMIT = 30 # Limit on the amount of full Cassandra bins to read before resorting to less accurate sampling

ASYNC_DOWNLOAD_BASE_DIR='/opt/ooi/async'

PREFERRED_DATA_LOCATION = 'san' # 'san' or 'cass': If data is present in a time bin on both the SAN and Cassandra this option chooses
                                # which value to take if the number of entries match.  Otherwise the location with the most data is chosen.

QC_RESULTS_STORAGE_SYSTEM = 'none' # 'log' to write qc results to a file, 'cass' to write qc results to a database