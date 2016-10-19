import re
import math
import logging

from subprocess import Popen, PIPE
from concurrent.futures import ThreadPoolExecutor
from util.common import log_timing

MAX_RSYNC_WORKERS = 4
MAX_RETRY_COUNT = 3


log = logging.getLogger(__name__)
logging.basicConfig()
log.setLevel(logging.DEBUG)
executor = ThreadPoolExecutor(max_workers=MAX_RSYNC_WORKERS)

total_size_re = re.compile(r'total size is (\d+)')


suffixes = ('', 'k', 'm', 'g', 't', 'p', 'e', 'z', 'y')


def human_readable(number):
    if number <= 1000:
        return '%s ' % number
    power = min(len(suffixes)-1, int(math.floor(math.log(number, 1000))))
    suffix = suffixes[power]
    reduced = float(number) / 1000**power
    return '%0.2f%s' % (reduced, suffix)


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

            log.info('%r returned success (0), transferred %s bytes', ident, human_readable(total))
            return p.returncode

        if p.returncode == 23 and 'No such file or directory' in stderr:
            log.info('%s returned (23) no files found on remote', ident)
            return 0

        log.warn('%s returned failure: %d', ident, p.returncode)
        log.warn('%s captured STDERR: %r', ident, stderr)
    log.error('Failed to rsync files (%r %r %r)', remote_host, remote_folder, target_folder)


def gather_files(host_list, folder_name):
    if host_list:
        futures = []
        for host in host_list:
            futures.append(executor.submit(rsync, host, folder_name, folder_name))

        for f in futures:
            f.result()
