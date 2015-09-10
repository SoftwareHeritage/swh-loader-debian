# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import subprocess

from swh.core.hashutil import hashfile


def extract_src_pkg(dsc_path, destdir):
    """extract a Debian source package to a given directory

    Note that after extraction the target directory will be the root of the
    extract package, rather than containing it.

    Args:
        dsc_path: path to .dsc file
        destdir: directory where to extract the package

    Returns:
        None

    """
    logging.debug('extract Debian source package %s' % dsc_path)

    destdir_tmp = destdir + '.tmp'
    logfile = destdir + '.log'

    cmd = ['dpkg-source', '--no-copy', '--no-check', '-x',
           dsc_path, destdir_tmp]
    with open(logfile, 'w') as log:
        subprocess.check_call(cmd, stdout=log, stderr=subprocess.STDOUT)

    os.rename(destdir_tmp, destdir)


def load_content_from_dir(storage, srcpkg_dir):
    hashes = {}
    for root, _dirs, files in os.walk(srcpkg_dir):
        for name in files:
            path = os.path.join(root, name)
            hashes[path] = hashfile(path)
            hashes[path]['length'] = os.path.getsize(path)

    storage.load_content(hashes)
