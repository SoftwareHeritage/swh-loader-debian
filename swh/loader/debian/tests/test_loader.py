# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from unittest import TestCase

import pytest
import requests_mock

from swh.core.tests.db_testing import SingleDbTestFixture
from swh.model.hashutil import hash_to_bytes
from swh.storage.schemata.distribution import SQLBase
from swh.loader.core.tests import BaseLoaderTest
from swh.loader.debian.loader import get_file_info, DebianLoader

from . import TEST_LOADER_CONFIG

RESOURCES_PATH = os.path.join(os.path.dirname(__file__), 'resources')


class DebianLoaderTest(DebianLoader):
    def parse_config_file(self, *args, **kwargs):
        return TEST_LOADER_CONFIG


@pytest.mark.fs
class TestFileInfo(TestCase):

    def test_get_file_info(self):
        path = '%s/%s' % (RESOURCES_PATH, 'onefile.txt')

        actual_info = get_file_info(path)

        expected_info = {
            'name': 'onefile.txt',
            'length': 62,
            'sha1': '135572f4ac013f49e624612301f9076af1eacef2',
            'sha1_git': '1d62cd247ef251d52d98bbd931d44ad1f967ea99',
            'sha256': '40f1a3cbe9355879319759bae1a6ba09cbf34056e79e951cd2dc0adbff169b9f',  # noqa
            'blake2s256': '4072cf9a0017ad7705a9995bbfbbc098276e6a3afea8d84ab54bff6381c897ab',  # noqa
        }

        self.assertEqual(actual_info, expected_info)


@pytest.mark.fs
class TestDebianLoader(SingleDbTestFixture, BaseLoaderTest):
    TEST_DB_NAME = 'test-lister-debian'
    TEST_DB_DUMP = []

    def setUp(self):
        super().setUp(archive_name='',
                      start_path=os.path.dirname(__file__),
                      uncompress_archive=False)
        self.loader = DebianLoaderTest()
        SQLBase.metadata.create_all(self.loader.db_engine)
        self.storage = self.loader.storage
        make_uri = ('file://' + RESOURCES_PATH + '/').__add__
        self.debian_src_name = 'hello_2.10-1+deb9u1.debian.tar.xz'
        self.orig_src_name = 'hello_2.10.orig.tar.gz'
        self.dsc_name = 'hello_2.10-1+deb9u1.dsc'
        self.files = {
            self.dsc_name: {
                'name': self.dsc_name,
                'uri': make_uri(self.dsc_name),
                'size': 1866,
            },
            self.debian_src_name: {
                'name': self.debian_src_name,
                'uri': make_uri(self.debian_src_name),
                'size': 6156,
            },
            self.orig_src_name: {
                'name': self.orig_src_name,
                'uri': make_uri(self.orig_src_name),
                'size': 725946,
            },
        }
        self._fill_db()

    def tearDown(self):
        SQLBase.metadata.drop_all(self.loader.db_engine)
        self.loader.db_session.close()
        self.loader.db_engine.dispose()  # close the connection pool
        super().tearDown()

    def _fill_db(self):
        from swh.storage.schemata.distribution import \
            Area, Distribution, Package
        dist = Distribution(name='Debian', type='deb', mirror_uri='devnull://')
        area = Area(distribution=dist, name='main')
        pkg = Package(area=area, name='hello', version='2.10-1+deb9u1',
                      directory='dir', files=self.files)
        self.loader.db_session.add_all([area, pkg])
        self.loader.db_session.commit()
        self.pkg_id = pkg.id

    def _load(self):
        self.loader.load(
            origin={'url': self.repo_url, 'type': 'deb'},
            date='2018-12-14 16:45:00+00',
            packages={
                'stretch/main/2.10-1+deb9u1': {
                    'id': self.pkg_id,
                    'name': 'hello',
                    'version': '2.10-1+deb9u1',
                    'revision_id': None,
                    'files': self.files,
                }
            }
        )

    def test_load(self):
        with requests_mock.Mocker() as m:
            for file_ in self.files.values():
                path = os.path.join(RESOURCES_PATH, file_['name'])
                with open(path, 'rb') as fd:
                    m.get(file_['uri'], content=fd.read())
            self._load()

        self.assertCountSnapshots(1)
        self.assertCountReleases(0)  # FIXME: Why not 1?
        self.assertCountRevisions(1)
        self.assertCountDirectories(14)
        self.assertCountContents(315)

        # Check the root dir was loaded, and contains 'src/'
        root_hash = 'c906789049d2327a69b81cca6a1c1737321c836f'
        ls_root = list(self.storage.directory_ls(
            hash_to_bytes(root_hash)))
        src_dirs = [x for x in ls_root if x['name'] == b'src']
        self.assertEqual(len(src_dirs), 1, src_dirs)

        # Check 'src/hello.c' exists
        src_dir_hash = src_dirs[0]['target']
        ls_src = list(self.storage.directory_ls(src_dir_hash))
        hello_c = [x for x in ls_src if x['name'] == b'hello.c']
        self.assertEqual(len(hello_c), 1, hello_c)

        # Check 'src.hello.c' was loaded and has the right hash
        hello_c_hash = 'b60a061ac9dd25b29d57b756b5959aadc1fe6386'
        self.assertEqual(hello_c[0]['sha1'], hash_to_bytes(hello_c_hash))

        missing = list(self.storage.content_missing(
            [{'sha1': hash_to_bytes(hello_c_hash)}]))
        self.assertEqual(missing, [])
