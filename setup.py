# setup.py
# Copyright 2011 Roger Marsh
# Licence: See LICENCE (BSD licence)

import sys
from distutils.core import setup

from basesup import _basesup_version

setup(
    name='-'.join(
        ('basesup',
         ''.join(
             ('py',
              '.'.join(
                  (str(sys.version_info[0]),
                   str(sys.version_info[1]))))),
         )),
    version=_basesup_version,
    description='Database definition classes',
    author='solentware.co.uk',
    author_email='roger.marsh@solentware.co.uk',
    url='http://www.solentware.co.uk',
    packages=[
        'basesup',
        'basesup.api', 'basesup.gui', 'basesup.tools',
        ],
    package_data={
        'basesup': ['README', 'LICENCE'],
        },
    long_description='''Database definition classes

    Provides access to a database using the bsddb interface to Berkeley DB or
    the dptdb interface to DPT.

    Records are stored as pickled class instances and indexed with values
    derived from the instance attributes.
    ''',
    )
