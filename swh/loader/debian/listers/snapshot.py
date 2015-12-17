#!/usr/bin/python3
# -*- encoding: utf-8 -*-
#
# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import os
import shutil

from deb822 import Dsc
from debian.debian_support import Version
import psycopg2


class SnapshotDebianOrg:
    """Methods to use the snapshot.debian.org mirror"""

    def __init__(
            self,
            connstr='service=snapshot',
            basedir='/srv/storage/space/mirrors/snapshot.debian.org',
    ):
        self.db = psycopg2.connect(connstr)
        self.basedir = basedir

    def _hash_to_path(self, hash):
        """Convert a hash to a file path on disk"""
        depth = 2
        fragments = [hash[2*i:2*(i+1)] for i in range(depth)]
        dirname = os.path.join(self.basedir, 'files', *fragments)

        return os.path.join(dirname, hash)

    def list_packages(self, count=1, previous_name=''):
        """List `count` source package names present in the snapshot.d.o db
           starting with previous_name (excluded)"""

        package_names_query = """
        select distinct(name)
        from srcpkg
        where name > %s
        order by name
        limit %s
        """

        with self.db.cursor() as cur:
            cur.execute(package_names_query, (previous_name, count))
            return [name for (name,) in cur]

    def list_package_files(self, names):
        """Retrieve the file metadata for all the versions of the
        given source packages.
        """

        files_query = """
        select srcpkg.srcpkg_id as src_id, srcpkg.name as src_name,
               srcpkg.version as src_version, file.hash, file.name
        from srcpkg
        left join file_srcpkg_mapping using (srcpkg_id)
        left join file using (hash)
        where srcpkg.name in %s
        """

        res = {}

        db_names = tuple(names)

        with self.db.cursor() as cur:
            cur.execute(files_query, (db_names,))
            for srcpkg_id, srcpkg_name, srcpkg_version, hash, name in cur:
                if srcpkg_id not in res:
                    res[srcpkg_id] = {
                        'id': srcpkg_id,
                        'name': srcpkg_name,
                        'version': Version(srcpkg_version),
                        'files': [],
                    }
                if hash and name:
                    res[srcpkg_id]['files'].append({
                        'hash': hash,
                        'name': name,
                    })

        return res

    def list_files_by_name(self, files):
        """List the files by name"""
        files_query = """
        select distinct name, hash
        from file
        where name in %s
        """

        ret = defaultdict(list)
        if not files:
            return ret

        with self.db.cursor() as cur:
            cur.execute(files_query, (tuple(files),))
            for name, hash in cur:
                ret[name].append(hash)

        return ret

    def copy_files_to_dirs(self, files, pool, log=None):
        """Copy the files from the snapshot storage to the directory
           `dirname`, via `pool`.

           - Step 1: copy hashed files to pool
           - Step 2: link hashed files from pool to destdir with the given name

        Args:
            - files: iterable of {hash, name, destdir} dictionaries
            - pool: the pool directory where hashed files are stored

        Raises:
            - FileNotFoundError if a hashed file doesn't exist at the source

        """

        hashes = set(file['hash'] for file in files)

        if log:
            log.debug("%d files to copy" % len(hashes))

        cnt = 0
        for hash in hashes:
            dst1 = os.path.join(pool, hash)
            if not os.path.exists(dst1):
                src = self._hash_to_path(hash)
                shutil.copy(src, dst1)
            cnt += 1
            if cnt % 100 == 0:
                if log:
                    log.debug("%d files copied" % cnt)

        if cnt % 100 != 0:
            if log:
                log.debug("%d files copied" % cnt)

        for file in files:
            src1 = os.path.join(pool, file['hash'])
            dst = os.path.join(file['destdir'], file['name'])
            if not os.path.exists(dst):
                os.link(src1, dst)

    def prepare_origins(self, package_names, storage, log=None):
        """Prepare the origins for the given packages.

        Args:
            package_names: a list of source package names
            storage: an instance of swh.storage.Storage

        Returns:
            a name -> origin dict where origin is itself a dict with the
            following keys:
                id: id of the origin
                type: deb
                url: the snapshot.debian.org URL for the package
        """
        ret = {}
        for name in package_names:
            origin = {
                'type': 'deb',
                'url': 'http://snapshot.debian.org/package/%s/' % name,
            }
            origin['id'] = storage.origin_add_one(origin)
            ret[name] = origin

        return ret

    def prepare_packages(self, packages, basedir, log=None):
        """Prepare all the source packages from `packages` for processing.

           Step 1: create a pool as basedir/.pool
           Step 2: for each version of each package, create a directory
                   basedir/package_version
           Step 3: copy all the files for each package version
                   to basedir/package_version/ using copy_files_to_dirs (and
                   the previously created pool)
           Step 4: parse all the dsc files and retrieve the remaining files

           Args:
               packages: a list of source package names
               basedir: the base directory for file copies
               log: a logging.Logger object
           Returns:
               an id -> source_package mapping, where each source_package is
               a dict with the following keys:
                   id: the id of the source package in snapshot.debian.org
                   name: the source package name
                   version: the version of the source package
                   files: a list of the files the source package uses
                          (with hash and name)
                   dsc: the full path to the package's dsc file.
        """

        src_packages = self.list_package_files(packages)

        ret = {}

        pool = os.path.join(basedir, '.pool')
        os.makedirs(pool, exist_ok=True)

        pkgs_with_really_missing_files = defaultdict(list)

        files = []
        for id, pkg in src_packages.items():
            srcpkg_name = pkg['name']
            srcpkg_version = str(pkg['version'])
            srcpkg_files = pkg['files']

            dirname = os.path.join(basedir, '%s_%s' % (srcpkg_name,
                                                       srcpkg_version))
            os.makedirs(dirname, exist_ok=True)

            if ':' in srcpkg_version:
                dsc_version = srcpkg_version.split(':', 1)[1]
            else:
                dsc_version = srcpkg_version
            intended_dsc = '%s_%s.dsc' % (srcpkg_name, dsc_version)

            for file in srcpkg_files:
                file = file.copy()
                file['destdir'] = dirname
                files.append(file)

            ret_pkg = pkg.copy()
            ret_pkg['dsc'] = os.path.join(dirname, intended_dsc)
            ret[id] = ret_pkg

        self.copy_files_to_dirs(files, pool, log)

        for id, pkg in ret.items():
            if not os.path.exists(pkg['dsc']):
                intended_dsc = os.path.basename(pkg['dsc'])
                pkgs_with_really_missing_files[id].append(intended_dsc)

        missing_files = []
        for id, pkg in ret.items():
            if id in pkgs_with_really_missing_files:
                continue
            destdir = os.path.dirname(pkg['dsc'])
            with open(pkg['dsc'], 'rb') as fh:
                dsc = Dsc(fh)
            for file in dsc['Files']:
                if not os.path.isfile(os.path.join(destdir, file['name'])):
                    missing_files.append((destdir, file, id))

        missing_file_names = set(f[1]['name'] for f in missing_files)
        retrieved_files = self.list_files_by_name(missing_file_names)

        missing_files_to_copy = []

        for destdir, file, id in missing_files:
            filename = file['name']
            missing_hashes = retrieved_files[filename]
            if len(missing_hashes) != 1:
                pkgs_with_really_missing_files[id].append(filename)
                continue
            missing_file = file.copy()
            missing_file['hash'] = missing_hashes[0]
            missing_file['destdir'] = destdir
            missing_files_to_copy.append(missing_file)

        self.copy_files_to_dirs(missing_files_to_copy, pool, log)

        for pkg_id, filenames in pkgs_with_really_missing_files.items():
            pkg = ret[pkg_id]
            del ret[pkg_id]
            if log:
                log.warn('Missing files in package %s_%s: %s' %
                         (pkg['name'], pkg['version'], ', '.join(filenames)),
                         extra={
                             'swh_type': 'deb_snapshot_missing_files',
                             'swh_id': pkg['id'],
                             'swh_name': pkg['name'],
                             'swh_version': str(pkg['version']),
                             'swh_missing_files': filenames,
                         })

        return ret
