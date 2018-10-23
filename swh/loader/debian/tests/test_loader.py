# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest import TestCase

from nose.plugins.attrib import attr

from swh.loader.debian.loader import get_file_info

RESOURCES_PATH = './swh/loader/debian/tests/resources'


@attr('fs')
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
