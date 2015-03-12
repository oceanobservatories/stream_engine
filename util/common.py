from functools import wraps
import time
from engine import app

FUNCTION = 'function'


def log_timing(func):
    @wraps(func)
    def inner(*args, **kwargs):
        app.logger.debug('Entered method: %s', func)
        start = time.time()
        results = func(*args, **kwargs)
        elapsed = time.time() - start
        app.logger.debug('Completed method: %s in %.2f', func, elapsed)
        return results

    return inner


def parse_pdid(pdid_string):
    try:
        return int(pdid_string.split()[0][2:])
    except ValueError:
        app.logger.warn('Unable to parse PDID: %s', pdid_string)
        return None


class DataUnavailableException(Exception):
    pass


class DataNotReadyException(Exception):
    pass


class CoefficientUnavailableException(Exception):
    pass


class UnknownEncodingException(Exception):
    pass