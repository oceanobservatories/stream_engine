import os
_basedir = os.path.abspath(os.path.dirname(__file__))

DEBUG = False

SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(_basedir, 'preload.db')
DATABASE_CONNECT_OPTIONS = {}

CASSANDRA_CONTACT_POINTS = ['127.0.0.1']
CASSANDRA_KEYSPACE = 'ooi2'

SPREADSHEET_KEY = '1jIiBKpVRBMU5Hb1DJqyR16XCkiX-CuTsn1Z1VnlRV4I'
USE_CACHED_SPREADSHEET = False