# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import glob
import os
import shutil
import tempfile

import dateutil

from swh.core.scheduling import Task
from swh.storage.storage import Storage

from .listers.snapshot import SnapshotDebianOrg
from .loader import (
    process_source_packages, try_flush_partial, flush_occurrences,
    flush_release, flush_revision)


DEFAULT_CONFIG = {
    'snapshot_connstr': 'service=snapshot',
    'snapshot_basedir': '/home/ndandrim/tmp/snapshot.d.o',
    'storage_args': [
        'dbname=softwareheritage-dev',
        '/tmp/swh-loader-debian/objects',
    ],
    'content_packet_size': 10000,
    'content_packet_length': 1024 ** 3,
    'content_max_length_one': 100 * 1024**2,
    'directory_packet_size': 25000,
    'keyrings': glob.glob('/usr/share/keyrings/*'),
}


class LoadSnapshotPackage(Task):
    task_queue = 'swh_loader_debian'

    def run(self, *package_names):
        """Load the history of the given package from snapshot.debian.org"""

        config = DEFAULT_CONFIG

        snapshot = SnapshotDebianOrg(
            connstr=config['snapshot_connstr'],
            basedir=config['snapshot_basedir'],
        )

        storage = Storage(
            *config['storage_args']
        )

        swh_authority_dt = open(
            os.path.join(config['snapshot_basedir'], 'TIMESTAMP')
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
            if os.path.exists(p['dsc']):
                p['origin_id'] = origins[p['name']]['id']
                sorted_pkgs.append(p)

        sorted_pkgs.sort(key=lambda p: (p['name'], p['version']))

        partial = {}
        for partial in process_source_packages(
                sorted_pkgs,
                config['keyrings'],
                log=self.log,
        ):

            try_flush_partial(
                storage, partial,
                content_packet_size=config['content_packet_size'],
                content_packet_length=config['content_packet_length'],
                content_max_length_one=config['content_max_length_one'],
                directory_packet_size=config['directory_packet_size'],
                log=self.log,
            )

        if partial:
            try_flush_partial(
                storage, partial,
                content_packet_size=config['content_packet_size'],
                content_packet_length=config['content_packet_length'],
                content_max_length_one=config['content_max_length_one'],
                directory_packet_size=config['directory_packet_size'],
                force=True,
                log=self.log,
            )

            packages = flush_revision(storage, partial, log=self.log)
            packages_w_revisions = flush_release(storage, packages)
            flush_occurrences(storage, packages_w_revisions, [swh_authority])

        shutil.rmtree(tmpdir)
