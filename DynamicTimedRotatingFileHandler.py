import logging
import random
import os

class DynamicTimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler, object):
    def __init__(self,fileName,when,interval,backupCount):
        # get logging path from env var, default to current directory
        path = os.getenv('OOI_GUNICORN_LOG_LOC', ".")
        super(DynamicTimedRotatingFileHandler,self).__init__(path+"/"+fileName,when,interval,backupCount)
