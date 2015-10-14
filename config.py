DEBUG = False
STREAM_ENGINE_VERSION = "0.7.20"

CASSANDRA_CONTACT_POINTS = ['127.0.0.1']
CASSANDRA_KEYSPACE = 'ooi'
CASSANDRA_CONNECT_TIMEOUT = 60
CASSANDRA_FETCH_SIZE = 1000
CASSANDRA_DEFAULT_TIMEOUT = 60
CASSANDRA_QUERY_CONSISTENCY = 'QUORUM'

SAN_BASE_DIRECTORY = '/opt/ooi/SAN/'
SAN_CASS_OVERWRITE = True # When loading data back into cassandra should we allow writing to an already present databin.
ANNOTATION_URL = 'http://localhost:12580/annotations/find/'
ASSET_URL = 'http://localhost:12573/'

UNBOUND_QUERY_START  = 3471292800 # Where to start unbound queries 2010-01-01T00:00:00.000Z

POOL_SIZE = 4

LOGGING_CONFIG='logging.conf'

NETCDF_TITLE = "Data produced by Stream Engine version {:s}".format(STREAM_ENGINE_VERSION)
NETCDF_INSTITUTION = "Ocean Observatories Initiative"
NETCDF_HISTORY_COMMENT = "generated from Stream Engine"
NETCDF_REFERENCE = "More information can be found at http://oceanobservatories.org/"
NETCDF_COMMENT = ""
NETCDF_CONVENTIONS = "CF-1.6"
NETCDF_METADATA_CONVENTIONS = "Unidata Dataset Discovery v1.0"
NETCDF_FEATURE_TYPE = "timeSeries"
NETCDF_CDM_DATA_TYPE = "Station"
NETCDF_NODC_TEMPLATE_VERSION = "NODC_NetCDF_TimeSeries_Orthogonal_Template_v1.1"
NETCDF_STANDARD_NAME_VOCABULARY = "NetCDF Climate and Forecast (CF) Metadata Convention Standard Name Table 29"
NETCDF_SUMMARY = ""
NETCDF_NAMING_AUTHORITY = "org.oceanobservatories"
NETCDF_CREATOR_NAME = "Ocean Observatories Initiative"
NETCDF_CREATOR_URL = "http://oceanobservatories.org/"
NETCDF_CREATOR_EMAIL = ""
NETCDF_PROJECT = "Ocean Observatories Initiative"
NETCDF_PROCESSING_LEVEL = "L2"
NETCDF_KEYWORDS_VOCABULARY = ""
NETCDF_KEYWORDS = ""
NETCDF_ACKNOWLEDGEMENT = ""
NETCDF_CONTRIBUTOR_NAME = ""
NETCDF_CONTRIBUTOR_ROLE= ""
NETCDF_PUBLISHER_NAME = "Ocean Observatories Initiative"
NETCDF_PUBLISHER_URL = "http://oceanobservatories.org/"
NETCDF_PUBLISHER_EMAIL = ""
NETCDF_LICENSE= ""
NETCDF_CALENDAR_TYPE = "gregorian"

Z_AXIS_NAME = "depth"
Z_POSITIVE = "down"
Z_DEFAULT_UNITS = 'meters'
Z_LONG_NAME = 'Deployment depth of sensor below sea surface'
Z_STANDARD_NAME = 'depth'
Z_RESOLUTION = 0.1
GEOSPATIAL_LAT_LON_RES = 0.1


COLLAPSE_TIMES = True

UI_FULL_RETURN_RATIO = 2.0 # If the ratio of total data to requested points is less than this value all of the data is returned
UI_FULL_SAMPLE_RATIO= 5.0 # If the ratio of total data to requested points is less than this value all and the total data in
                        # Cassandra is less than the full sample limit a linear sample from all of the data is returned
UI_FULL_SAMPLE_LIMIT= 5000 # If the ratio of total data to requested points is less than the UI_FULL_SAMPLE_RATIO and
                            # the total number of data points is less than this value a linear sampling of all of the data is returned
                            # Otherwise a sampling method is used pre query.
UI_HARD_LIMIT = 20000     # set a hard limit for the maximum size a limited query can be.  All data can be accessed using an async query.

ASYNC_DOWNLOAD_BASE_DIR='/opt/ooi/async'

PREFERRED_DATA_LOCATION = 'san' # 'san' or 'cass': If data is present in a time bin on both the SAN and Cassandra this option chooses
                                # which value to take if the number of entries match.  Otherwise the location with the most data is chosen.

QC_RESULTS_STORAGE_SYSTEM = 'none' # 'log' to write qc results to a file, 'cass' to write qc results to a database

LOOKBACK_QUERY_LIMIT = 100 # Number of cassandra rows used to the correct deployment for padding of streams which provide cal coefficients and other needed data

DPA_VERSION_VARIABLE = "version" # The name of the variable that contains the version string for the ion_functions at the package level.

INTERNAL_OUTPUT_EXCLUDE_LIST = ['bin', ]
INTERNAL_OUTPUT_MAPPING = {'deployment' : 'int32', 'id' : 'str'}
