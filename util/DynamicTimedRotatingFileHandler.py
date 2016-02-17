from logging.handlers import TimedRotatingFileHandler
import os

class DynamicTimedRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(self, file_name, when, interval, backup_count):
        # get logging path from env var, default to current directory
        path = os.getenv('OOI_GUNICORN_LOG_LOC', '.')
        super(DynamicTimedRotatingFileHandler, self).__init__(
                os.path.join(path, file_name), when, interval, backup_count)
