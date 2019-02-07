# Copyright (C) 2017 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from celery import current_app as app

from .loader import DebianLoader


@app.task(name=__name__ + '.LoadDebianPackage')
def load_debian_packages(origin, date, packages):
    return DebianLoader().load(origin=origin, date=date, packages=packages)
