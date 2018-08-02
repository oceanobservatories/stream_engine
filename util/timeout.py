#!/usr/bin/env python
import os
import signal
import pickle
import inspect
import logging

from engine import app
from functools import wraps
from util.common import StreamEngineException, TimedOutException


log = logging.getLogger(__name__)


def sigalarm_handler(signum, frame):
    raise TimedOutException("Data processing timed out after %s seconds")


def set_timeout(timeout=None):
    # If timeout is a supplied argument to the wrapped function
    # use it, otherwise use the default timeout
    if timeout is None or not isinstance(timeout, int):
        timeout = app.config['REQUEST_TIMEOUT_SECONDS']

    def inner(func):
        @wraps(func)
        def decorated_function(*args, **kwargs):
            signal.signal(signal.SIGALRM, sigalarm_handler)
            signal.alarm(timeout)
            try:
                result = func(*args, **kwargs)
            except TimedOutException:
                raise StreamEngineException("Data processing timed out after %s seconds" %
                                            timeout, status_code=408)
            finally:
                signal.alarm(0)

            return result

        return decorated_function
    return inner


def set_inactivity_timeout(is_active=lambda _: False, poll_period=None, max_runtime=None):
    """
    Decorator to poll whether a function is active and when it becomes inactive, to terminate it. Function activity is
    checked by an is_active function passed to the decorator. Activity is checked at the end of every poll_period
    seconds. Also, at the close of each poll period, if the cumulative runtime of the function exceeds max_runtime
    seconds, the function is terminated. The is_active function has access to poll_period, max_runtime, and all
    arguments passed to the wrapped function.
    :param is_active: function used to determine if the decorated function is still active
                      (e.g. if aggregation is progressing)
    :param poll_period: the frequency, in seconds, at which the is_active function is called and the decorated
                        function's runtime is checked against max_runtime
    :param max_runtime: the maximum amount of time, in seconds, that the decorated function should be allowed to run;
                        only checked at the end of each poll_period, so ideally max_runtime would be a multiple of
                        poll_period, otherwise the exact max_runtime will NOT be respected; a value of None indicates
                        no max_runtime (i.e. never terminate function for running too long if it is still active)
    :return: the object returned by the decorated function; WARNING: this will not work right for return values that
             cannot be serialized with the pickle module - in such cases an exception will be thrown (i.e. PickleError)
    """
    # if max_runtime is None, leave it as None
    if max_runtime and not isinstance(max_runtime, int):
        max_runtime = app.config['DEFAULT_MAX_RUNTIME']
    if poll_period is None or not isinstance(poll_period, int):
        poll_period = app.config['DEFAULT_ACTIVITY_POLL_PERIOD']
    timeout_handler = sigalarm_handler

    def inner(func):
        @wraps(func)
        def decorated_function(*args, **kwargs):
            # Combine args, kwargs, and the new variables max_runtime and poll_period into one keyword dictionary for
            # potential use in the is_active function
            arguments = inspect.getcallargs(func, *args, **kwargs)
            arguments['max_runtime'] = max_runtime
            arguments['poll_period'] = poll_period
            
            # Fork so there are two processes: the child runs the decorated function while the parent monitors the
            # child's activity and terminates it if necessary.
            read_fd, write_fd = os.pipe()
            processid = os.fork()
            if processid == 0:
                # child process - run the decorated function
                os.close(read_fd)
                result = func(*args, **kwargs)
                # use pickle to serialize the return object and send it to the parent over a pipe
                result_string = pickle.dumps(result)
                with os.fdopen(write_fd, 'w') as w:
                    w.write(result_string)
                # the child is done - don't keep the extra process around
                os._exit(0)
            else:
                # parent process - monitor progress of function
                runtime = 0
                os.close(write_fd)
                
                # Define SIGTERM handler inside this function to be able to access processid variable
                # make sure the child is killed so everything shutsdown properly
                def sigterm_handler(signum, frame):
                    os.kill(processid, signal.SIGKILL)
                    # exit similar to os._exit, but allowing some cleanup
                    raise SystemExit
                    
                signal.signal(signal.SIGTERM, sigterm_handler)
                signal.signal(signal.SIGALRM, timeout_handler)
                
                # Whatever happens while monitoring the child process, make sure two things happen:
                # 1. If an exception (other than a handled TimedOutException) is raised, kill the child process
                # 2. Once the monitoring loop is exited, clear the timeout alarm so it doesn't interrupt later code
                try:
                    # set a timeout for the poll_period - this will trigger a TimedOutException
                    signal.alarm(poll_period)
                    
                    with os.fdopen(read_fd) as r:
                        # Wrap the try-except block in a loop so that the contents of the try clause are retried until
                        # one of the following conditions is met:
                        # 1. The results are read from pipe and the child waited for successfully
                        # 2. A TimedOutException occurs and the decorated function has run for more than max_runtime
                        # 3. A TimedOutException occurs and the decorated function is deemed inactive by the is_active function
                        # 4. An unhandled exception is raised
                        # 5. A signal (e.g. SIGTERM) is received by the parent process
                        while True:
                            try:
                                result_string = r.read()
                                os.waitpid(processid, 0)
                                break
                            except TimedOutException:
                                runtime += poll_period
                                if (not max_runtime or runtime < max_runtime) and is_active(**arguments):
                                    log.info("Function %s has run %s seconds and is still active." % (func.__name__, runtime))
                                    # extend the timeout for another poll_period seconds
                                    signal.alarm(poll_period)
                                else:
                                    # raise an exception that will go to EDEX
                                    raise StreamEngineException("Function %s timed out after %s seconds" % 
                                                                (func.__name__, runtime), status_code=408)
                except Exception:
                    os.kill(processid, signal.SIGKILL)
                    raise
                finally:
                    signal.alarm(0)
                
                # convert the serialized representation of the result back to an object
                result = pickle.loads(result_string)
                # return the return value of the decorated function
                return result
                
        return decorated_function
    return inner
