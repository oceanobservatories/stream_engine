import os
_basedir = os.path.abspath(os.path.dirname(__file__))

DEBUG = False

SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(_basedir, 'preload.db')
DATABASE_CONNECT_OPTIONS = {}

THREADS_PER_PAGE = 8