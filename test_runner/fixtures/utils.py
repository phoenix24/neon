import os
import shutil
import subprocess

from typing import Any, List
from fixtures.log_helper import log


def get_self_dir() -> str:
    """ Get the path to the directory where this script lives. """
    return os.path.dirname(os.path.abspath(__file__))


def mkdir_if_needed(path: str) -> None:
    """ Create a directory if it doesn't already exist

    Note this won't try to create intermediate directories.
    """
    try:
        os.mkdir(path)
    except FileExistsError:
        pass
    assert os.path.isdir(path)


def subprocess_capture(capture_dir: str, cmd: List[str], **kwargs: Any) -> str:
    """ Run a process and capture its output

    Output will go to files named "cmd_NNN.stdout" and "cmd_NNN.stderr"
    where "cmd" is the name of the program and NNN is an incrementing
    counter.

    If those files already exist, we will overwrite them.
    Returns basepath for files with captured output.
    """
    assert type(cmd) is list
    base = os.path.basename(cmd[0]) + '_{}'.format(global_counter())
    basepath = os.path.join(capture_dir, base)
    stdout_filename = basepath + '.stdout'
    stderr_filename = basepath + '.stderr'

    with open(stdout_filename, 'w') as stdout_f:
        with open(stderr_filename, 'w') as stderr_f:
            log.info('(capturing output to "{}.stdout")'.format(base))
            subprocess.run(cmd, **kwargs, stdout=stdout_f, stderr=stderr_f)

    return basepath


_global_counter = 0


def global_counter() -> int:
    """ A really dumb global counter.

    This is useful for giving output files a unique number, so if we run the
    same command multiple times we can keep their output separate.
    """
    global _global_counter
    _global_counter += 1
    return _global_counter


def lsn_to_hex(num: int) -> str:
    """ Convert lsn from int to standard hex notation. """
    return "{:X}/{:X}".format(num >> 32, num & 0xffffffff)


def lsn_from_hex(lsn_hex: str) -> int:
    """ Convert lsn from hex notation to int. """
    l, r = lsn_hex.split('/')
    return (int(l, 16) << 32) + int(r, 16)


def print_gc_result(row):
    log.info("GC duration {elapsed} ms".format_map(row))
    log.info(
        "  total: {layers_total}, needed_by_cutoff {layers_needed_by_cutoff}, needed_by_branches: {layers_needed_by_branches}, not_updated: {layers_not_updated}, removed: {layers_removed}"
        .format_map(row))


# path to etcd binary or None if not present.
def etcd_path():
    return shutil.which("etcd")
