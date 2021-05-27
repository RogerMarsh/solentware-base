# setup.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

import distutils.command
import sys

from setuptools import setup
import setuptools.command

commands = set(distutils.command.__all__ + setuptools.command.__all__)
del setuptools.command
del distutils.command

args = sys.argv
del sys

if __name__ == '__main__':

    # Include samples and tests if 'sdist', 'develop', or 'clean' are the only
    # commands.
    commands.remove('sdist')
    commands.remove('develop')
    commands.remove('clean')
    packages = ['solentware_base',
                'solentware_base.core',
                'solentware_base.tools',
                ]
    for a in args[1:]:
        if a in commands:
            break
    else:
        packages.extend(['solentware_base.samples',
                         'solentware_base.tests',
                         'solentware_base.core.tests',])

    long_description = open('README').read()

    setup(
        name='solentware-base',
        version='4.1',
        description=' '.join(
            ('Bitmapped record number databases using Python interfaces to',
             'Berkeley DB, SQLite, UnQLite, Vedis, and DPT.',
             )),
        author='Roger Marsh',
        author_email='roger.marsh@solentware.co.uk',
        url='http://www.solentware.co.uk',
        packages=packages,
        long_description=long_description,
        license='BSD',
        classifiers=[
            'License :: OSI Approved :: BSD License',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Operating System :: OS Independent',
            'Topic :: Database',
            'Intended Audience :: Developers',
            'Development Status :: 3 - Alpha',
            ],
        )
