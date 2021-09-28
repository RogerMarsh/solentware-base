# setup.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)
"""solentware-base setup file."""

from setuptools import setup

if __name__ == "__main__":

    long_description = open("README").read()

    setup(
        name="solentware-base",
        version="4.1.5",
        description=" ".join(
            (
                "Bitmapped record number databases using Python interfaces to",
                "Berkeley DB, SQLite, UnQLite, Vedis, and DPT.",
            )
        ),
        author="Roger Marsh",
        author_email="roger.marsh@solentware.co.uk",
        url="http://www.solentware.co.uk",
        packages=[
            "solentware_base",
            "solentware_base.core",
            "solentware_base.tools",
        ],
        long_description=long_description,
        license="BSD",
        classifiers=[
            "License :: OSI Approved :: BSD License",
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Operating System :: OS Independent",
            "Topic :: Database",
            "Intended Audience :: Developers",
            "Development Status :: 3 - Alpha",
        ],
    )
