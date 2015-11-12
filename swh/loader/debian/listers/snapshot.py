#!/usr/bin/python3
# -*- encoding: utf-8 -*-
#
# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil

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

    def copy_files_to_dirs(self, files, pool):
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

        print("%d files to copy" % len(hashes))

        cnt = 0
        for hash in hashes:
            dst1 = os.path.join(pool, hash)
            if not os.path.exists(dst1):
                src = self._hash_to_path(hash)
                shutil.copy(src, dst1)
            cnt += 1
            if cnt % 100 == 0:
                print("%d files copied" % cnt)

        if cnt % 100 != 0:
            print("%d files copied" % cnt)

        for file in files:
            src1 = os.path.join(pool, file['hash'])
            dst = os.path.join(file['destdir'], file['name'])
            if not os.path.exists(dst):
                os.link(src1, dst)

    def copy_package_files(self, packages, basedir):
        """Copy all the files for the packages `packages` in `basedir`.

           Step 1: create a pool as basedir/.pool
           Step 2: for each package version, create a directory
                   basedir/package_version
           Step 3: copy all the files for each package version
                   to basedir/package_version/ using copy_files_to_dirs (and
                   the previously created pool)

           Args:
               - packages: an id -> source_package mapping
                 where each source_package is a dict containing:
                    - name (str): source package name
                    - version (debian_support.Version): source package
                      version
                    - files (list): list of {hash, filename} dicts
               - basedir: the base directory for file copies
           Returns:
               - an id -> source_package mapping, where each
                 source_package has been augmented with the full path to its
                 dsc file in the 'dsc' key.
        """

        src_packages = self.list_package_files(packages)

        files = []
        ret = {}

        pool = os.path.join(basedir, '.pool')
        os.makedirs(pool, exist_ok=True)

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

        self.copy_files_to_dirs(files, pool)

        return ret
