# setup.py
# Copyright 2011 Roger Marsh
# Licence: See LICENCE (BSD licence)

from setuptools import setup

if __name__ == '__main__':

    long_description = open('README').read()

    setup(
        name='basesup',
        version='0.17',
        description='Database Record definition classes',
        author='Roger Marsh',
        author_email='roger.marsh@solentware.co.uk',
        url='http://www.solentware.co.uk',
        package_dir={'basesup':''},
        packages=[
            'basesup',
            'basesup.api', 'basesup.gui', 'basesup.tools',
            'basesup.about',
            ],
        package_data={
            'basesup.about': ['LICENCE', 'CONTACT'],
            },
        long_description=long_description,
        license='BSD',
        classifiers=[
            'License :: OSI Approved :: BSD License',
            'Programming Language :: Python :: 3.3',
            'Programming Language :: Python :: 3.4',
            'Operating System :: OS Independent',
            'Topic :: Software Development',
            'Topic :: Database :: Front Ends',
            'Intended Audience :: Developers',
            'Development Status :: 4 - Beta',
            ],
        )
