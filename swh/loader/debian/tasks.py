# Copyright (C) 2017 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.scheduler.task import Task

from .loader import DebianLoader


class LoadDebianPackage(Task):
    task_queue = 'swh_loader_debian'

    def run_task(self, *, origin, date, packages):
        loader = DebianLoader()
        return loader.load(origin=origin, date=date, packages=packages)
