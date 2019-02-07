# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import hashlib
import logging
import os
import re
import subprocess
import tempfile

from dateutil.parser import parse as parse_date
from debian.changelog import Changelog
from debian.deb822 import Dsc
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from swh.loader.core.loader import BufferedLoader
from swh.storage.schemata.distribution import Package
from swh.model import hashutil
from swh.model.from_disk import Directory
from swh.model.identifiers import identifier_to_bytes, snapshot_identifier

from . import converters


UPLOADERS_SPLIT = re.compile(r'(?<=\>)\s*,\s*')


log = logging.getLogger(__name__)


class DebianLoaderException(Exception):
    pass


class PackageDownloadFailed(DebianLoaderException):
    """Raise this exception when a package extraction failed"""
    pass


class PackageExtractionFailed(DebianLoaderException):
    """Raise this exception when a package extraction failed"""
    pass


def _debian_to_hashlib(hashname):
    """Convert Debian hash names to hashlib-compatible names"""
    return {
        'md5sum': 'md5',
    }.get(hashname, hashname)


def download_package(package):
    """Fetch a source package in a temporary directory and check the checksums
    for all files"""

    tempdir = tempfile.TemporaryDirectory(
        prefix='swh.loader.debian.%s.' % package['name']
    )

    for filename, fileinfo in copy.deepcopy(package['files']).items():
        uri = fileinfo.pop('uri')
        hashes = {
            hashname: hashlib.new(_debian_to_hashlib(hashname))
            for hashname in fileinfo
            if hashname not in ('name', 'size')
        }

        r = requests.get(uri, stream=True)
        if r.status_code != 200:
            raise PackageDownloadFailed(
                'Download of %s returned status_code %s: %s' %
                (uri, r.status_code, r.text)
            )

        size = 0
        with open(os.path.join(tempdir.name, filename), 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                size += len(chunk)
                f.write(chunk)
                for hash in hashes.values():
                    hash.update(chunk)

        downloadinfo = {
            'name': filename,
            'size': size,
        }
        for hashname, hash in hashes.items():
            downloadinfo[hashname] = hash.hexdigest()

        if fileinfo != downloadinfo:
            raise PackageDownloadFailed(
                'Checksums mismatch: fetched %s, expected %s' %
                (downloadinfo, fileinfo)
            )

    return tempdir


def extract_package(package, tempdir):
    """Extract a Debian source package to a given directory

    Note that after extraction the target directory will be the root of the
    extracted package, rather than containing it.

    Args:
        package (dict): package information dictionary
        tempdir (str): directory where the package files are stored

    Returns:
        tuple: path to the dsc (str) and extraction directory (str)

    """
    dsc_name = None
    for filename in package['files']:
        if filename.endswith('.dsc'):
            if dsc_name:
                raise PackageExtractionFailed(
                    'Package %s_%s references several dsc files' %
                    (package['name'], package['version'])
                )
            dsc_name = filename

    dsc_path = os.path.join(tempdir.name, dsc_name)
    destdir = os.path.join(tempdir.name, 'extracted')
    logfile = os.path.join(tempdir.name, 'extract.log')

    log.debug('extract Debian source package %s in %s' %
              (dsc_path, destdir), extra={
                  'swh_type': 'deb_extract',
                  'swh_dsc': dsc_path,
                  'swh_destdir': destdir,
              })

    cmd = ['dpkg-source',
           '--no-copy', '--no-check',
           '--ignore-bad-version',
           '-x', dsc_path,
           destdir]

    try:
        with open(logfile, 'w') as stdout:
            subprocess.check_call(cmd, stdout=stdout, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        logdata = open(logfile, 'r').read()
        raise PackageExtractionFailed('dpkg-source exited with code %s: %s' %
                                      (e.returncode, logdata)) from None

    return dsc_path, destdir


def get_file_info(filepath):
    """Retrieve the original file information from the file at filepath.

    Args:
        filepath: the path to the original file

    Returns:
        dict: information about the original file, in a dictionary with the
        following keys

        - name: the file name
        - sha1, sha1_git, sha256: original file hashes
        - length: original file length
    """

    name = os.path.basename(filepath)
    if isinstance(name, bytes):
        name = name.decode('utf-8')

    hashes = hashutil.MultiHash.from_path(filepath).hexdigest()
    hashes['name'] = name
    hashes['length'] = os.path.getsize(filepath)
    return hashes


def get_package_metadata(package, dsc_path, extracted_path):
    """Get the package metadata from the source package at dsc_path,
    extracted in extracted_path.

    Args:
        package: the package dict (with a dsc_path key)
        dsc_path: path to the package's dsc file
        extracted_path: the path where the package got extracted

    Returns:
        dict: a dictionary with the following keys:

        - history: list of (package_name, package_version) tuples parsed from
          the package changelog
        - source_files: information about all the files in the source package

    """
    ret = {}

    with open(dsc_path, 'rb') as dsc:
        parsed_dsc = Dsc(dsc)

    source_files = [get_file_info(dsc_path)]

    dsc_dir = os.path.dirname(dsc_path)
    for filename in package['files']:
        file_path = os.path.join(dsc_dir, filename)
        file_info = get_file_info(file_path)
        source_files.append(file_info)

    ret['original_artifact'] = source_files

    # Parse the changelog to retrieve the rest of the package information
    changelog_path = os.path.join(extracted_path, 'debian/changelog')
    with open(changelog_path, 'rb') as changelog:
        try:
            parsed_changelog = Changelog(changelog)
        except UnicodeDecodeError:
            log.warn('Unknown encoding for changelog %s,'
                     ' falling back to iso' %
                     changelog_path.decode('utf-8'), extra={
                         'swh_type': 'deb_changelog_encoding',
                         'swh_name': package['name'],
                         'swh_version': str(package['version']),
                         'swh_changelog': changelog_path.decode('utf-8'),
                     })

            # need to reset as Changelog scrolls to the end of the file
            changelog.seek(0)
            parsed_changelog = Changelog(changelog, encoding='iso-8859-15')

    package_info = {
        'name': package['name'],
        'version': str(package['version']),
        'changelog': {
            'person': converters.uid_to_person(parsed_changelog.author),
            'date': parse_date(parsed_changelog.date),
            'history': [(block.package, str(block.version))
                        for block in parsed_changelog][1:],
        }
    }

    maintainers = [
        converters.uid_to_person(parsed_dsc['Maintainer'], encode=False),
    ]
    maintainers.extend(
        converters.uid_to_person(person, encode=False)
        for person in UPLOADERS_SPLIT.split(parsed_dsc.get('Uploaders', ''))
    )
    package_info['maintainers'] = maintainers

    ret['package_info'] = package_info

    return ret


def process_package(package):
    """Process a source package into its constituent components.

    The source package will be decompressed in a temporary directory.

    Args:
        package (dict): a dict with the following keys:

            - name: source package name
            - version: source package version
            - dsc: the full path of the package's DSC file.

    Returns:
        tuple: A tuple with two elements:

        - package: the original package dict augmented with the following keys:

            - metadata: the metadata from get_package_metadata
            - directory: the sha1_git of the root directory of the package

        - objects: a dictionary of the parsed directories and files, both
          indexed by id

    Raises:
        FileNotFoundError: if the dsc file does not exist
        PackageExtractionFailed: if package extraction failed

    """
    log.info("Processing package %s_%s" %
             (package['name'], str(package['version'])),
             extra={
                 'swh_type': 'deb_process_start',
                 'swh_name': package['name'],
                 'swh_version': str(package['version']),
             })

    tempdir = download_package(package)
    dsc, debdir = extract_package(package, tempdir)

    directory = Directory.from_disk(path=os.fsencode(debdir), save_path=True)
    metadata = get_package_metadata(package, dsc, debdir)

    return directory, metadata, tempdir


class DebianLoader(BufferedLoader):
    """A loader for Debian packages"""

    CONFIG_BASE_FILENAME = 'loader/debian'
    ADDITIONAL_CONFIG = {
        'lister_db_url': ('str', 'postgresql:///lister-debian'),
    }

    def __init__(self, config=None):
        super().__init__(logging_class=None, config=config)
        self.db_engine = create_engine(self.config['lister_db_url'])
        self.mk_session = sessionmaker(bind=self.db_engine)
        self.db_session = self.mk_session()

    def load(self, *, origin, date, packages):
        return super().load(origin=origin, date=date, packages=packages)

    def prepare_origin_visit(self, *, origin, date, packages):
        self.origin = origin
        self.visit_date = date

    def prepare(self, *, origin, date, packages):
        self.packages = packages

        # Deduplicate branches according to equivalent files
        branches_files = {}
        branches_revs = {}
        equiv_branch = {}
        for branch, package in packages.items():
            if 'files' not in package:
                # already loaded, use default values
                branches_revs[branch] = identifier_to_bytes(
                    package['revision_id']
                )
                equiv_branch[branch] = branch
                continue

            for eq_branch, files in branches_files.items():
                if package['files'] == files:
                    equiv_branch[branch] = eq_branch
                    if (not branches_revs[eq_branch]
                            and package['revision_id']):
                        branches_revs[eq_branch] = identifier_to_bytes(
                            package['revision_id']
                        )
                    break
            else:
                # No match: new entry
                equiv_branch[branch] = branch
                branches_files[branch] = package['files']
                if package['revision_id']:
                    branches_revs[branch] = identifier_to_bytes(
                        package['revision_id']
                    )
                else:
                    branches_revs[branch] = None

        self.equivs = {
            'branches': equiv_branch,
            'revisions': branches_revs,
        }

        self.versions_to_load = [
            (branch, self.packages[branch])
            for branch in sorted(branches_revs)
            if not branches_revs[branch]
        ]

        self.version_idx = 0
        self.done = self.version_idx >= len(self.versions_to_load)

        self.current_data = {}
        self.tempdirs = []
        self.partial = False

    def fetch_data(self):
        if self.done:
            return False

        branch, package = self.versions_to_load[self.version_idx]
        self.version_idx += 1

        try:
            directory, metadata, tempdir = process_package(package)
            self.tempdirs.append(tempdir)
            self.current_data = directory.collect()
            revision = converters.package_metadata_to_revision(
                package, directory, metadata
            )

            self.current_data['revision'] = {
                revision['id']: revision,
            }

            self.equivs['revisions'][branch] = revision['id']

        except DebianLoaderException:
            log.exception('Package %s_%s failed to load' %
                          (package['name'], package['version']))
            self.partial = True

        self.done = self.version_idx >= len(self.versions_to_load)
        return not self.done

    def store_data(self):
        self.maybe_load_contents(
            self.current_data.get('content', {}).values())
        self.maybe_load_directories(
            self.current_data.get('directory', {}).values())
        self.maybe_load_revisions(
            self.current_data.get('revision', {}).values())
        self.current_data = {}

        if self.done:
            self.flush()
            self.update_packages()
            self.generate_and_load_snapshot()

    def update_packages(self):
        for branch in self.packages:
            package = self.packages[branch]
            if package['revision_id']:
                continue
            rev = self.equivs['revisions'][self.equivs['branches'][branch]]
            if not rev:
                continue

            db_package = self.db_session.query(Package)\
                                        .filter(Package.id == package['id'])\
                                        .one()
            db_package.revision_id = rev

        self.db_session.commit()

    def generate_and_load_snapshot(self):
        """Create a SWH archive "snapshot" of the package being loaded, and send it to
        the archive.


        """
        branches = {}
        for branch in self.packages:
            rev = self.equivs['revisions'][self.equivs['branches'][branch]]
            if rev:
                target = {
                    'target_type': 'revision',
                    'target': rev,
                }
            else:
                self.partial = True
                target = None

            branches[branch.encode('utf-8')] = target
        snapshot = {'branches': branches}
        snapshot['id'] = identifier_to_bytes(snapshot_identifier(snapshot))
        self.maybe_load_snapshot(snapshot)

    def load_status(self):
        status = 'eventful' if self.versions_to_load else 'uneventful'

        return {
            'status': status if not self.partial else 'failed',
        }

    def visit_status(self):
        return 'partial' if self.partial else 'full'

    def cleanup(self):
        for d in self.tempdirs:
            d.cleanup()


if __name__ == '__main__':
    import click
    import logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(process)d %(message)s'
    )

    @click.command()
    @click.option('--origin-url', required=1,
                  help='Origin url to associate')
    @click.option('--packages', help='Debian packages to load')
    @click.option('--visit-date', default=None,
                  help='Visit date time override')
    def main(origin_url, packages, visit_date):
        """Loading debian package tryout."""
        origin = {'url': origin_url, 'type': 'deb'}
        if not packages:
            packages = {
                "buster/main/3.2.3-1": {
                    "id": 178584,
                    "name": "alex",
                    "version": "3.2.3-1",
                    "revision_id": "e8b2fe173ab909aa49d40b59292a44c2668e8a26"
                },
                "jessie/main/3.1.3-1": {
                    "id": 230994,
                    "name": "alex",
                    "version": "3.1.3-1",
                    "revision_id": "9a7c853d4cb2521f59108d8d5f21f26a800038ca"
                },
            }
        DebianLoader().load(origin=origin, date=visit_date, packages=packages)

    main()
