# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import email.utils
import os

from swh.loader.dir.converters import tree_to_directory
from swh.loader.dir.git.git import GitType


def blob_to_shallow_content(obj, other_objs):
    """Convert a blob as sent by swh.walker.dir to a blob ready to be sent
    (without contents)

        Args:
            - obj: The blob object returned by swh.walker.dir
            - other_objs: unused.

        Returns: A "shallow_content": a content, without the data,
            which saves memory space.

    """
    ret = obj.copy()
    if 'length' not in ret:
        ret['length'] = os.lstat(obj['path']).st_size

    ret['perms'] = obj['perms'].value
    ret['type'] = obj['type'].value

    return ret


def shallow_content_to_content(obj, content_max_length_one):
    """Add the necessary data to the shallow_content created by the
       previous function

       Args:
           - obj: shallow_content dict as returned by blob_to_shallow_content
           - content_max_length_one: length limit of a persisted content

       Returns:
           A content suitable for persistence in swh.storage
    """

    content = obj.copy()

    if content['length'] > content_max_length_one:
        content['status'] = 'absent'
        content['reason'] = 'Content too large'
    elif 'data' not in content:
        content['status'] = 'visible'
        content['data'] = open(content['path'], 'rb').read()

    del content['path']

    return content


def dedup_objects(objects, remove_duplicates=True):
    """Deduplicate the objects from dictionary `objects`.

    Args:
        - objects: a dictionary of objects indexed by path
        - remove_duplicates: if True, remove the duplicate objects
                             from the filesystem

    Returns: A dictionary, indexed by object type, of dictionaries
        indexed by object id of deduplicated objects.

    """
    converter_map = {
        GitType.TREE: tree_to_directory,
        GitType.BLOB: blob_to_shallow_content,
    }

    type_map = {
        GitType.TREE: 'directory',
        GitType.BLOB: 'content',
    }

    ret = defaultdict(dict)
    for members in objects.values():
        for member in members:
            conv = converter_map[member['type']](member, objects)
            ret_type = type_map[member['type']]
            ret_key = conv.get('sha1_git') or conv['id']
            if ret_key not in ret[ret_type]:
                ret[ret_type][ret_key] = conv
            elif remove_duplicates and 'path' in conv:
                # Nuke duplicate files
                os.unlink(conv['path'])

    return ret


def merge_objects(accumulator, updates, remove_duplicates=True):
    """Merge the objects from `updates` in `accumulator`.

    This function mutates accumulator. It is designed so that the
    "content" and "directory" members of accumulator can be flushed
    periodically, for instance to send the data to the database in
    chunks. "content_seen" and "directory_seen" contain all the ids of
    the objects that have been seen so far.

    - Args:
          - accumulator: a dict to accumulate several updates in, with keys:
              - content (dict)
              - directory (dict)
              - content_seen (set)
              - directory_seen (set)
          - updates: the objects to add to accumulator (has two keys,
                     content and directory)
          - remove_duplicates: if True, removes the objects from updates that
                               have already be seen in accumulator.

    - Returns: None (accumulator is mutated).

    """

    for key in ['content', 'directory']:
        seen_key = key + '_seen'
        cur_updates = updates[key]
        to_update = accumulator[key]
        seen = accumulator[seen_key]
        for update_key in cur_updates:
            if update_key not in seen:
                to_update[update_key] = cur_updates[update_key]
                seen.add(update_key)
            elif remove_duplicates and key == 'content':
                # Nuke the files that haven't changed since a previous run...
                os.unlink(cur_updates[update_key]['path'])


def uid_to_person(uid, key=None):
    """Convert an uid to a person suitable for insertion.

    Args:
        uid: an uid of the form "Name <email@ddress>"
        key: the key in which the values are stored

    Returns: a dictionary with keys:
        key_name (or name if key is None): the name associated to the uid
        key_email (or email if key is None): the mail associated to the uid
    """

    if key is not None:
        name_key = '%s_name' % key
        mail_key = '%s_email' % key
    else:
        name_key = 'name'
        mail_key = 'mail'

    ret = {
        name_key: '',
        mail_key: '',
    }

    name, mail = email.utils.parseaddr(uid)

    if name and email:
        ret[name_key] = name
        ret[mail_key] = mail
    else:
        ret[name_key] = uid

    return ret
