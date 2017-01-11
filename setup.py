'''
LICENSING
-------------------------------------------------

hgx: Lightweight integration library for Hypergolix.
    Copyright (C) 2016 Muterra, Inc.
    
    Contributors
    ------------
    Nick Badger
        badg@muterra.io | badg@nickbadger.com | nickbadger.com

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public
    License as published by the Free Software Foundation; either
    version 2.1 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the
    Free Software Foundation, Inc.,
    51 Franklin Street,
    Fifth Floor,
    Boston, MA  02110-1301 USA

------------------------------------------------------
'''
# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from pathlib import Path
import os
import sys


# This is very gross, but we need to conditionally rename the source directory
# to hgx so that we don't conflict with hypergolix itself (also, so that the
# hgx package is available under a different namespace)
# if sys.argv[1] == 'install':
#     thisdir = Path(os.path.dirname(__file__))
#     packagedir_hypergolix = thisdir / 'hypergolix'
#     packagedir_hgx = thisdir / 'hgx'
    
#     packagedir_hypergolix.rename(packagedir_hgx)

long_description = \
    '''Hypergolix is "programmable Dropbox". Run it as a daemonized background
    process, and use hgx to integrate it with apps.'''

setup(
    name='hgx',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version='0.1.0',

    description='Lightweight integration library for Hypergolix.',
    long_description=long_description,

    # The project's main homepage.
    url='https://github.com/Muterra/py_hypergolix',

    # Author details
    author='Muterra, Inc',
    author_email='badg@muterra.io',

    # Choose your license
    license='LGPL',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development',
        'Topic :: Utilities',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: GNU Lesser General Public License v2 or ' +
        'later (LGPLv2+)',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.5',
    ],

    # What does your project relate to?
    keywords='hypergolix, IoT, internet of things, golix, encryption, ' +
             'security, privacy, private, identity, sharing',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),

    # Alternatively, if you want to distribute just a my_module.py, uncomment
    # this:
    #   py_modules=["my_module"],

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=[
        'websockets>=3.2',
        'loopa>=0.0.2',
        'golix>=0.1.6',
        'certifi'
    ],

    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e .[dev,test]
    extras_require={},

    # If there are data files included in your packages that need to be
    # installed, specify them here.  If using Python 2.6 or less, then these
    # have to be included in MANIFEST.in as well.
    package_data={
    },
)
