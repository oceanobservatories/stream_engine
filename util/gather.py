import re
import math
import logging
import platform

from subprocess import Popen, PIPE
from concurrent.futures import ThreadPoolExecutor

from engine import app
from util.common import log_timing


MAX_RSYNC_WORKERS = app.config.get('MAX_RSYNC_WORKERS', 1)
MAX_RETRY_COUNT = app.config.get('MAX_RETRY_COUNT', 1)


log = logging.getLogger(__name__)
logging.basicConfig()
log.setLevel(logging.DEBUG)
executor = ThreadPoolExecutor(max_workers=MAX_RSYNC_WORKERS)

total_size_re = re.compile(r'total size is (\d+)')
prefixes = ('', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')


def human_readable(number):
    """
    Given a number, return a scaled representation with an SI prefix
    :param number:
    :return: scaled result plus the appropriate SI prefix
    """
    if number <= 1000:
        return '%s ' % number
    power = min(len(prefixes) - 1, int(math.floor(math.log(number, 1000))))
    prefix = prefixes[power]
    reduced = float(number) / 1000**power
    return '%0.2f %s' % (reduced, prefix)


@log_timing(log)
def _rsync(remote_host, remote_folder, target_folder):
    """
    Rsync all files from remote_host:remote_folder to target_folder
    removing them from the remote host upon completion
    :param remote_host:
    :param remote_folder:
    :param target_folder:
    :return:
    """
    log.debug('executing: _rsync(%r, %r, %r)', remote_host, remote_folder, target_folder)
    remote_name = '%s:%s/.' % (remote_host, remote_folder)
    options = ['-dv', '--remove-source-files']
    args = ['rsync'] + options + [remote_name, target_folder]
    return Popen(args, stdout=PIPE, stderr=PIPE, close_fds=True)


@log_timing(log)
def _rmdir(remote_host, remote_folder):
    """
    Ssh to the specified remote host and remove the remote folder specified, if empty
    :param remote_host:
    :param remote_folder:
    :return:
    """
    log.debug('executing: _rmdir(%r, %r)', remote_host, remote_folder)
    args = ['ssh', remote_host, 'rmdir', remote_folder]
    return Popen(args, stdout=PIPE, stderr=PIPE, close_fds=True)


def rsync(remote_host, remote_folder, target_folder, retry_count=MAX_RETRY_COUNT):
    for i in range(1, retry_count+1):
        ident = 'rsync(%r, %r, %r) (%d/%d)' % (remote_host, remote_folder, target_folder, i, retry_count)
        p = _rsync(remote_host, remote_folder, target_folder)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            match = total_size_re.search(stdout)
            total = 0
            if match:
                total = int(match.group(1))

            log.info('%r returned success (0), transferred %sbytes', ident, human_readable(total))
            return p.returncode

        if p.returncode == 23 and 'No such file or directory' in stderr:
            log.info('%s returned (23) no files found on remote', ident)
            return 0

        log.warn('%s returned failure: %d', ident, p.returncode)
        log.warn('%s captured STDERR: %r', ident, stderr)
    log.error('Failed to rsync files (%r %r %r)', remote_host, remote_folder, target_folder)


def rmdir(remote_host, remote_folder, retry_count=MAX_RETRY_COUNT):
    for i in range(1, retry_count+1):
        ident = 'rmdir(%r, %r) (%d/%d)' % (remote_host, remote_folder, i, retry_count)
        p = _rsync(remote_host, remote_folder)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            log.info('%r returned success (0)', ident)
            return p.returncode

        log.warn('%s returned failure: %d', ident, p.returncode)
        log.warn('%s captured STDERR: %r', ident, stderr)
    log.error('Failed to rsync files (%r %r)', remote_host, remote_folder)


def gather_files(host_list, folder_name):
    """
    Gather all files in the specified folder on the specified hosts
    Skip this host if included.
    :param host_list:
    :param folder_name:
    :return:
    """
    if host_list:
        this_node = platform.node()
        if this_node in host_list:
            host_list.remove(this_node)

        # Fetch remote files
        futures = [executor.submit(rsync, host, folder_name, folder_name) for host in host_list]
        [f.result() for f in futures]

        # Cleanup remote folders
        futures = [executor.submit(rmdir, host, folder_name) for host in host_list]
        [f.result() for f in futures]
