# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import os
import re
import subprocess
import shutil
import tempfile
import traceback

from dateutil.parser import parse as parse_date
from debian.changelog import Changelog
from debian.deb822 import Dsc

from swh.core import hashutil
from swh.loader.dir.git.git import (
    walk_and_compute_sha1_from_directory, ROOT_TREE_KEY)

from . import converters


UPLOADERS_SPLIT = re.compile(r'(?<=\>)\s*,\s*')


class PackageExtractionFailed(Exception):
    """Raise this exception when a package extraction failed"""
    pass


def extract_src_pkg(dsc_path, destdir, log=None):
    """Extract a Debian source package to a given directory

    Note that after extraction the target directory will be the root of the
    extracted package, rather than containing it.

    Args:
        dsc_path: path to .dsc file
        destdir: directory where to extract the package
        log: a logging.Logger object

    Returns:
        None

    """
    if log:
        log.debug('extract Debian source package %s in %s' %
                  (dsc_path, destdir.decode('utf-8')), extra={
                      'swh_type': 'deb_extract',
                      'swh_dsc': dsc_path,
                      'swh_destdir': destdir.decode('utf-8'),
                  })

    destdir_tmp = b''.join([destdir, b'.tmp'])
    logfile = b''.join([destdir, b'.log'])

    cmd = ['dpkg-source',
           '--no-copy', '--no-check',
           '--ignore-bad-version',
           '-x', dsc_path,
           destdir_tmp]

    try:
        with open(logfile, 'w') as stdout:
            subprocess.check_call(cmd, stdout=stdout, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError:
        if log:
            data = open(logfile, 'r').read()
            log.warn('extracting Debian package %s failed: %s' %
                     (dsc_path, data),
                     extra={
                         'swh_type': 'deb_extract_failed',
                         'swh_dsc': dsc_path,
                         'swh_log': data,
                     })
        raise PackageExtractionFailed()

    os.rename(destdir_tmp, destdir)


def get_file_info(filepath):
    """Retrieve the original file information from the file at filepath.

    Args:
        filepath: the path to the original file
    Returns:
        A dict with the information about the original file:
            name: the file name
            sha1, sha1_git, sha256: the hashes for the original file
            length: the length of the original file
    """

    name = os.path.basename(filepath)
    if isinstance(name, bytes):
        name = name.decode('utf-8')

    ret = {
        'name': name,
    }

    hashes = hashutil.hashfile(filepath)
    for hash in hashes:
        ret[hash] = hashutil.hash_to_hex(hashes[hash])

    ret['length'] = os.lstat(filepath).st_size

    return ret


def get_gpg_info_signature(gpg_info):
    """Extract the signature date from a deb822.GpgInfo object

    Args:
        gpg_info: a deb822.GpgInfo object
    Returns: a dictionary with the following keys:
        signature_date: a timezone-aware datetime.DateTime object with the date
                        the signature was made
        signature_keyid: the keyid of the signing key
        signature_uid: the uid of the signing key, if found
    """

    uid = None

    if 'VALIDSIG' in gpg_info:
        key_id = gpg_info['VALIDSIG'][0]
        timestamp = gpg_info['VALIDSIG'][2]

        for key in gpg_info.uidkeys:
            if key in gpg_info:
                uid = gpg_info[key][-1]
                break

    elif 'ERRSIG' in gpg_info:
        key_id = gpg_info['ERRSIG'][0]
        timestamp = gpg_info['ERRSIG'][4]
    else:
        raise ValueError('Cannot find signature in gpg_info '
                         'object. Keys: %s' % gpg_info.keys())

    dt = datetime.datetime.utcfromtimestamp(int(timestamp))
    dt = dt.replace(tzinfo=datetime.timezone.utc)

    ret = {
        'date': dt,
        'keyid': key_id,
    }

    ret['person'] = converters.uid_to_person(uid, encode=False)

    return ret


def get_package_metadata(package, extracted_path, keyrings, log=None):
    """Get the package metadata from the source package at dsc_path,
    extracted in extracted_path.

    Args:
        package: the package dict (with a dsc_path key)
        extracted_path: the path where the package got extracted
        keyrings: a list of keyrings to use for gpg actions
        log: a logging.Logger object

    Returns: a dict with the following keys
        history: list of (package_name, package_version) tuples parsed from
                 the package changelog
        source_files: information about all the files in the source package

    """
    ret = {}

    # Parse the dsc file to retrieve all the original artifact files
    dsc_path = package['dsc']
    with open(dsc_path, 'rb') as dsc:
        parsed_dsc = Dsc(dsc)

    source_files = [get_file_info(dsc_path)]

    dsc_dir = os.path.dirname(dsc_path)
    for file in parsed_dsc['files']:
        file_path = os.path.join(dsc_dir, file['name'])
        file_info = get_file_info(file_path)
        source_files.append(file_info)

    ret['original_artifact'] = source_files

    # Parse the changelog to retrieve the rest of the package information
    changelog_path = os.path.join(extracted_path, b'debian/changelog')
    with open(changelog_path, 'rb') as changelog:
        try:
            parsed_changelog = Changelog(changelog)
        except UnicodeDecodeError:
            if log:
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
        'lister_metadata': {
            'lister': 'snapshot.debian.org',
            'id': package['id'],
        },
        'changelog': {
            'person': converters.uid_to_person(parsed_changelog.author),
            'date': parse_date(parsed_changelog.date),
            'history': [(block.package, str(block.version))
                        for block in parsed_changelog][1:],
        }
    }

    try:
        gpg_info = parsed_dsc.get_gpg_info(keyrings=keyrings)
        package_info['pgp_signature'] = get_gpg_info_signature(gpg_info)
    except ValueError:
        if log:
            log.info('Could not get PGP signature on package %s_%s' %
                     (package['name'], package['version']),
                     extra={
                         'swh_type': 'deb_missing_signature',
                         'swh_name': package['name'],
                         'swh_version': str(package['version']),
                     })
        package_info['pgp_signature'] = None

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


def process_source_package(package, keyrings, log=None):
    """Process a source package into its constituent components.

    The source package will be decompressed in a temporary directory.

    Args:
        package: a dict with the following keys:
            name: source package name
            version: source package version
            dsc: the full path of the package's DSC file.
        keyrings: a list of keyrings to use for gpg actions
        log: a logging.Logger object

    Returns:
        A tuple with two elements:
        package: the original package dict augmented with the following keys:
            metadata: the metadata from get_package_metadata
            basedir: the temporary directory in which the package was
                     decompressed
            directory: the sha1_git of the root directory of the package
        objects: a dictionary of the parsed directories and files, both indexed
                 by id

    Raises:
        - FileNotFoundError if the dsc file does not exist.
        - PackageExtractionFailed if the package extraction failed
    """
    if log:
        log.info("Processing package %s_%s" %
                 (package['name'], str(package['version'])),
                 extra={
                     'swh_type': 'deb_process_start',
                     'swh_name': package['name'],
                     'swh_version': str(package['version']),
                 })

    if not os.path.exists(package['dsc']):
        raise FileNotFoundError('%s does not exist' % package['dsc'])

    basedir = tempfile.mkdtemp()
    debdir = os.path.join(basedir, '%s_%s' % (package['name'],
                                              package['version']))

    # the swh.loader.dir internals legitimately want bytes for paths
    debdir = debdir.encode('utf-8')

    extract_src_pkg(package['dsc'], debdir, log=log)

    parsed_objects = walk_and_compute_sha1_from_directory(debdir)
    root_tree = parsed_objects[ROOT_TREE_KEY][0]

    package = package.copy()
    package['basedir'] = basedir
    package['directory'] = root_tree['sha1_git']
    package['metadata'] = get_package_metadata(package, debdir, keyrings,
                                               log=log)

    return package, converters.dedup_objects(parsed_objects)


def process_source_packages(packages, keyrings, log=None):
    """Execute process_source_package, but for lists of packages.

    Args:
        packages: an iterable of packages as expected by process_source_package
        keyrings: a list of keyrings to use for gpg actions
        log: a logging.Logger object

    Returns: a generator of partial results.

    Partial results have the following keys:
        objects: the accumulator for merge_objects. This member can be mutated
                 to clear the pending contents and directories.
        tempdirs: the temporary directories processed so far. This list can be
                  flushed if temporary directories are removed on the fly.
        packages: the list of packages processed so far.

    """

    objects = {
        'content': {},
        'directory': {},
        'content_seen': set(),
        'directory_seen': set(),
    }

    ret_packages = []
    tempdirs = []

    for package in packages:
        try:
            ret_package, package_objs = process_source_package(
                package, keyrings, log=log)
        except PackageExtractionFailed:
            continue
        except Exception as e:
            if log:
                e_type = e.__class__.__name__
                e_exc = traceback.format_exception(
                    e.__class__,
                    e,
                    e.__traceback__,
                )
                log.warn("Could not process package %s_%s: %s" %
                         (package['name'], str(package['version']), e_exc),
                         extra={
                             'swh_type': 'deb_process_failed',
                             'swh_name': package['name'],
                             'swh_version': str(package['version']),
                             'swh_exception_type': e_type,
                             'swh_exception': e_exc,
                         })
            continue
        ret_packages.append(ret_package)
        converters.merge_objects(objects, package_objs)
        tempdirs.append(ret_package['basedir'])

        yield {
            'objects': objects,
            'packages': ret_packages,
            'tempdirs': tempdirs,
        }


def flush_content(storage, partial_result, content_max_length_one, log=None):
    """Flush the contents from a partial_result to storage

    Args:
        storage: an instance of swh.storage.Storage
        partial_result: a partial result as yielded by process_source_packages
        content_max_length_one: the maximum length of a persisted content
        log: a logging.Logger object

    This function mutates partial_result to empty the content dict
    """
    contents = partial_result['objects']['content']

    missing_ids = storage.content_missing(contents.values(),
                                          key_hash='sha1_git')

    if missing_ids:
        full_contents = (
            converters.shallow_content_to_content(contents[i],
                                                  content_max_length_one)
            for i in missing_ids)

        storage.content_add(full_contents)

    partial_result['objects']['content'] = {}


def flush_directory(storage, partial_result, log=None):
    """Flush the directories from a partial_result to storage

    Args:
        storage: an instance of swh.storage.Storage
        partial_result: a partial result as yielded by process_source_packages
        log: a logging.Logger object

    This function mutates partial_result to empty the directory dict
    """
    storage.directory_add(partial_result['objects']['directory'].values())
    partial_result['objects']['directory'] = {}


def flush_revision(storage, partial_result, log=None):
    """Flush the revisions from a partial_result to storage

    Args:
        storage: an instance of swh.storage.Storage
        partial_result: a partial result as yielded by process_source_packages
        log: a logging.Logger object
    Returns:
        The package objects augmented with a revision argument
    """
    packages = [package.copy() for package in partial_result['packages']]
    revisions = []
    for package in packages:
        revision = converters.package_to_revision(package, log=log)
        revisions.append(revision)
        package['revision'] = revision

    storage.revision_add(revisions)

    return packages


def flush_release(storage, packages, log=None):
    """Flush the revisions from a partial_result to storage

    Args:
        storage: an instance of swh.storage.Storage
        packages: a list of packages as returned by flush_revision
        log: a logging.Logger object
    Returns:
        The package objects augmented with a release argument
    """
    releases = []
    for package in packages:
        release = converters.package_to_release(package)
        releases.append(release)
        package['release'] = release

    storage.release_add(releases)

    return packages


def flush_occurrences(storage, packages, default_occurrences, log=None):
    """Flush the occurrences from a partial_result to storage
    Args:
        storage: an instance of swh.storage.Storage
        packages: a list of packages as returned by flush_release
        default_occurrences: a list of occurrences with default values
        log: a logging.Logger object
    Returns:
        The written occurrence objects
    """
    occurrences = []

    for package in packages:
        for default_occurrence in default_occurrences:
            occurrence = default_occurrence.copy()
            occurrence['revision'] = package['revision']['id']
            occurrence['branch'] = str(package['version'])
            occurrence['origin'] = package['origin_id']
            occurrences.append(occurrence)

    storage.occurrence_add(occurrences)

    return occurrences


def remove_tempdirs(partial_result, log=None):
    """Remove the temporary files for the packages listed"""
    for tempdir in partial_result['tempdirs']:
        if os.path.isdir(tempdir):
            shutil.rmtree(tempdir)

    # Use the slice trick to empty the list in-place
    partial_result['tempdirs'][:] = []


def try_flush_partial(storage, partial_result,
                      content_packet_size=10000,
                      content_packet_length=1024 * 1024 * 1024 * 1024,
                      content_max_length_one=100 * 1024 * 1024,
                      directory_packet_size=25000, force=False, log=None):
    """Conditionally flush the partial result to storage.

    Args:
        storage: an instance of swh.storage.Storage
        partial_result: a partial result as yielded by process_source_packages
        content_packet_size: the number of contents that triggers a flush
        content_packet_length: the cumulated size of contents that triggers a
                               flush
        content_max_length_one: the maximum length of a persisted content
        directory_packet_size: the number of directories that triggers a flush
        force: force a flush regardless of packet sizes
        log: a logging.Logger object
    """
    n_content = len(partial_result['objects']['content'])
    n_directory = len(partial_result['objects']['directory'])

    if force:
        # Don't compute the length if we don't care
        len_contents = 0
    else:
        len_contents = sum(
            content['length']
            for content in partial_result['objects']['content'].values()
            if content['length'] <= content_max_length_one
        )

    # Flush both contents and directories at once to be able to clear
    # tempfiles while we work
    if force or n_content >= content_packet_size or \
       len_contents >= content_packet_length or \
       n_directory >= directory_packet_size:
        flush_content(storage, partial_result, content_max_length_one, log=log)
        flush_directory(storage, partial_result, log=log)
        remove_tempdirs(partial_result, log=log)
