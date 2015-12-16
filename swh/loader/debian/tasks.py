# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import glob
import os
import shutil
import tempfile

import dateutil

from swh.core.config import load_named_config
from swh.core.scheduling import Task
from swh.storage import get_storage

from .listers.snapshot import SnapshotDebianOrg
from .loader import (
    process_source_packages, try_flush_partial, flush_occurrences,
    flush_release, flush_revision)


DEFAULT_CONFIG = {
    'snapshot_connstr': ('str', 'service=snapshot'),
    'snapshot_basedir': ('str', '/home/ndandrim/tmp/snapshot.d.o'),
    'storage_class': ('str', 'local_storage'),
    'storage_args': ('list', [
        'dbname=softwareheritage-dev',
        '/tmp/swh-loader-debian/objects',
    ]),
    'content_packet_size': ('int', 10000),
    'content_packet_length': ('int', 1024 ** 3),
    'content_max_length_one': ('int', 100 * 1024**2),
    'directory_packet_size': ('int', 25000),
    'keyrings': ('list', glob.glob('/usr/share/keyrings/*')),
}


class LoadSnapshotPackages(Task):
    task_queue = 'swh_loader_debian'

    @property
    def config(self):
        if not hasattr(self, '__config'):
            self.__config = load_named_config(
                'loader/debian.ini',
                DEFAULT_CONFIG,
            )
        return self.__config

    def run(self, *package_names):
        """Load the history of the given package from snapshot.debian.org"""

        snapshot = SnapshotDebianOrg(
            connstr=self.config['snapshot_connstr'],
            basedir=self.config['snapshot_basedir'],
        )

        storage = get_storage(
            self.config['storage_class'],
            self.config['storage_args'],
        )

        swh_authority_dt = open(
            os.path.join(self.config['snapshot_basedir'], 'TIMESTAMP')
        ).read()

        swh_authority = {
            'authority': '5f4d4c51-498a-4e28-88b3-b3e4e8396cba',
            'validity': dateutil.parser.parse(swh_authority_dt),
        }

        tmpdir = tempfile.mkdtemp()
        os.makedirs(os.path.join(tmpdir, 'source'))

        pkgs = snapshot.prepare_packages(
            package_names,
            os.path.join(tmpdir, 'source'),
            log=self.log,
        )
        origins = snapshot.prepare_origins(package_names, storage)

        sorted_pkgs = []
        for p in pkgs.values():
            p['origin_id'] = origins[p['name']]['id']
            sorted_pkgs.append(p)

        sorted_pkgs.sort(key=lambda p: (p['name'], p['version']))

        partial = {}
        for partial in process_source_packages(
                sorted_pkgs,
                self.config['keyrings'],
                tmpdir,
                log=self.log,
        ):

            try_flush_partial(
                storage, partial,
                content_packet_size=self.config['content_packet_size'],
                content_packet_length=self.config['content_packet_length'],
                content_max_length_one=self.config['content_max_length_one'],
                directory_packet_size=self.config['directory_packet_size'],
                log=self.log,
            )

        if partial:
            try_flush_partial(
                storage, partial,
                content_packet_size=self.config['content_packet_size'],
                content_packet_length=self.config['content_packet_length'],
                content_max_length_one=self.config['content_max_length_one'],
                directory_packet_size=self.config['directory_packet_size'],
                force=True,
                log=self.log,
            )

            packages = flush_revision(storage, partial, log=self.log)
            packages_w_revisions = flush_release(storage, packages)
            flush_occurrences(storage, packages_w_revisions, [swh_authority])

        shutil.rmtree(tmpdir)
