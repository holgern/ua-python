# -*- coding: utf-8 -*-
"""Packaging logic for beem."""
import codecs
import io
import os
import sys

from setuptools import setup

# Work around mbcs bug in distutils.
# http://bugs.python.org/issue10945

try:
    codecs.lookup('mbcs')
except LookupError:
    ascii = codecs.lookup('ascii')
    codecs.register(lambda name, enc=ascii: {True: enc}.get(name == 'mbcs'))

VERSION = '0.1.0'

tests_require = ['mock >= 2.0.0', 'pytest', 'pytest-mock', 'parameterized']

requires = [
    "beem",
    "dataset",
    "pymongo"
]


def write_version_py(filename):
    """Write version."""
    cnt = """\"""THIS FILE IS GENERATED FROM beem SETUP.PY.\"""
version = '%(version)s'
"""
    with open(filename, 'w') as a:
        a.write(cnt % {'version': VERSION})


def get_long_description():
    """Generate a long description from the README file."""
    descr = []
    for fname in ('README.rst',):
        with io.open(fname, encoding='utf-8') as f:
            descr.append(f.read())
    return '\n\n'.join(descr)


if __name__ == '__main__':

    # Rewrite the version file everytime
    write_version_py('steemua/version.py')

    setup(
        name='ua-python',
        version=VERSION,
        description='Steem UA library',
        long_description=get_long_description(),
        download_url='https://github.com/holgern/ua-python/tarball/' + VERSION,
        url='http://www.github.com/holgern/ua-python',
        keywords=['steem', 'library', 'ua'],
        packages=[
            "steemua",
        ],
        classifiers=[
            'License :: OSI Approved :: MIT License',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Development Status :: 4 - Beta',
            'Intended Audience :: Developers',
        ],
        install_requires=requires,
        entry_points={
        },
        setup_requires=['pytest-runner'],
        tests_require=tests_require,
        include_package_data=True,
    )
