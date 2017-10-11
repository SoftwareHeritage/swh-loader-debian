# Copyright (C) 2015-2017 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import datetime
import email.utils
import logging

from swh.model import identifiers

log = logging.getLogger(__name__)

ROBOT_AUTHOR = {
    'name': b'Software Heritage',
    'email': b'robot@softwareheritage.org',
    'fullname': b'Software Heritage <robot@softwareheritage.org>',
}


def package_metadata_to_revision(package, directory, metadata):
    """Convert a package dictionary to a revision suitable for storage.

    Args:
        package: a dictionary with the following keys:

            - metadata: the metadata for the package, containing::

                package_info:
                    changelog
                    pgp_signature

            - directory

    Returns:
        A revision suitable for persistence in swh.storage
    """

    message = 'Synthetic revision for Debian source package %s version %s' % (
        package['name'], package['version'])

    def prepare(obj):
        if isinstance(obj, list):
            return [prepare(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: prepare(v) for k, v in obj.items()}
        elif isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode('utf-8')
        else:
            return copy.deepcopy(obj)

    author = metadata['package_info']['changelog']['person']
    date = metadata['package_info']['changelog']['date']

    ret = {
        'author': author,
        'date': date,
        'committer': author,
        'committer_date': date,
        'type': 'dsc',
        'directory': directory.hash,
        'message': message.encode('utf-8'),
        'synthetic': True,
        'parents': [],
        'metadata': prepare(metadata),
    }

    rev_id = bytes.fromhex(identifiers.revision_identifier(ret))
    ret['id'] = rev_id

    return ret


def package_to_release(package):
    """Convert a package dictionary to a revision suitable for storage.

    Args:
        package: a dictionary with the following keys:

            - metadata: the metadata for the package, containing::

                  package_info
                      changelog
                          person
                          date

            - revision

    Returns:
        A revision suitable for persistence in swh.storage
    """
    package_info = package['metadata']['package_info']

    message = 'Synthetic release for Debian source package %s version %s' % (
        package['name'], package['version'])

    ret = {
        'author': package_info['changelog']['person'],
        'date': package_info['changelog']['date'],
        'target': package['revision']['id'],
        'target_type': 'revision',
        'message': message.encode('utf-8'),
        'synthetic': True,
        'name': str(package['version']),
    }

    rev_id = bytes.fromhex(identifiers.release_identifier(ret))
    ret['id'] = rev_id

    return ret


def uid_to_person(uid, encode=True):
    """Convert an uid to a person suitable for insertion.

    Args:
        uid: an uid of the form "Name <email@ddress>"
        encode: whether to convert the output to bytes or not

    Returns:
        dict: a dictionary with the following keys:

        - name: the name associated to the uid
        - email: the mail associated to the uid
    """

    ret = {
        'name': '',
        'email': '',
        'fullname': uid,
    }

    name, mail = email.utils.parseaddr(uid)

    if name and email:
        ret['name'] = name
        ret['email'] = mail
    else:
        ret['name'] = uid

    if encode:
        for key in list(ret):
            ret[key] = ret[key].encode('utf-8')

    return ret
