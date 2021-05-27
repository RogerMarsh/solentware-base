# setup.py
# Copyright 2011 Roger Marsh
# Licence: See LICENCE (BSD licence)

from distutils.core import setup

from version import _basesup_version

setup(
    name='basesup',
    version=_basesup_version,
    description='Database definition classes',
    author='solentware.co.uk',
    author_email='roger.marsh@solentware.co.uk',
    url='http://www.solentware.co.uk',
    package_dir={'basesup':''},
    packages=[
        'basesup',
        'basesup.api', 'basesup.gui', 'basesup.tools',
        ],
    package_data={
        'basesup': ['LICENCE'],
        },
    long_description='''Database definition classes

    Provides access to a database using the bsddb interface to Berkeley DB, the
    dptdb interface to DPT, or the sqlite3 interface to sqlite.

    Records are stored as pickled class instances and indexed with values
    derived from the instance attributes.
    ''',
    )
