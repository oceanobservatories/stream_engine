#############################################################################################################
# Disable the @timed_cache() decorator on the util.metadata_service.stream.build_stream_dictionary() method #
#############################################################################################################
import util.common
from functools import wraps


def mock_timed_cache(unused):
    def wrapper(func):
        @wraps(func)
        def inner():
            return func()
        return inner
    return wrapper

util.common.timed_cache = mock_timed_cache
