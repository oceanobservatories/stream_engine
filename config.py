import os
_basedir = os.path.abspath(os.path.dirname(__file__))

DEBUG = False

SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(_basedir, 'preload.db')
DATABASE_CONNECT_OPTIONS = {}

THREADS_PER_PAGE = 8

ALGORITHM_POOL_SIZE = 8

CASSANDRA_CONTACT_POINTS = ['127.0.0.1']
CASSANDRA_KEYSPACE = 'ooi2'