# setup.py
# Copyright 2011 Roger Marsh
# Licence: See LICENCE (BSD licence)

from setuptools import setup

if __name__ == '__main__':

    long_description = open('README').read()

    setup(
        name='solentware-base',
        version='3.0.1',
        description='Database Record definition classes',
        author='Roger Marsh',
        author_email='roger.marsh@solentware.co.uk',
        url='http://www.solentware.co.uk',
        package_dir={'solentware_base':''},
        packages=[
            'solentware_base',
            'solentware_base.api',
            'solentware_base.minorbases',
            'solentware_base.about',
            'solentware_base.test',
            'solentware_base.tools',
            'solentware_base.api.test',
            'solentware_base.minorbases.test',
            ],
        package_data={
            'solentware_base.about': ['LICENCE', 'CONTACT'],
            },
        long_description=long_description,
        license='BSD',
        classifiers=[
            'License :: OSI Approved :: BSD License',
            'Programming Language :: Python :: 3.6',
            'Operating System :: OS Independent',
            'Topic :: Software Development',
            'Topic :: Database :: Front-Ends',
            'Intended Audience :: Developers',
            'Development Status :: 4 - Beta',
            ],
        )
