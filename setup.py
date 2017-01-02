#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import inspect
import os
import sys

import setuptools
from setuptools import setup
from setuptools.command.test import test

if sys.version_info < (3, 5, 0):
    sys.stderr.write('FATAL: end2end needs to be run with Python 3.5+\n')
    sys.exit(1)

__location__ = os.path.join(os.getcwd(), os.path.dirname(inspect.getfile(inspect.currentframe())))


def read_version(package):
    with open(os.path.join(package, '__init__.py'), 'r') as fd:
        for line in fd:
            if line.startswith('__version__ = '):
                return line.split()[-1].strip().strip("'")


NAME = 'nakadi-end2end'
MAIN_PACKAGE = 'end2end'
VERSION = read_version(MAIN_PACKAGE)
DESCRIPTION = 'Measuring time for message end-to-end processing'
LICENSE = 'Apache License 2.0'
URL = 'https://github.com/zalando-incubator/nakadi-end2end'
AUTHOR = 'Dmitry Sorokin'
EMAIL = 'dmitriy.sorokin@zalando.de'
KEYWORDS = 'measuring message processing time'

# Add here all kinds of additional classifiers as defined under
# https://pypi.python.org/pypi?%3Aaction=list_classifiers
CLASSIFIERS = [
    'Development Status :: 3 - Alpha',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'Intended Audience :: System Administrators',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: POSIX :: Linux',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: Implementation :: CPython',
]

CONSOLE_SCRIPTS = [
    'end2end-daemon = end2end.main:start'
]


class PyTest(test):
    def run_tests(self):
        try:
            import pytest
        except:
            raise RuntimeError('py.test is not installed, run: pip install pytest')
        params = {'args': self.test_args}
        errno = pytest.main(**params)
        sys.exit(errno)


def read(fname):
    with open(os.path.join(__location__, fname)) as f:
        return f.read()


def setup_package():
    command_options = {'test': {'test_suite': ('setup.py', 'tests')}}

    setup(
        name=NAME,
        version=VERSION,
        url=URL,
        description=DESCRIPTION,
        author=AUTHOR,
        author_email=EMAIL,
        license=LICENSE,
        keywords=KEYWORDS,
        classifiers=CLASSIFIERS,
        test_suite='tests',
        packages=setuptools.find_packages(exclude=['tests', 'tests.*']),
        install_requires=[req for req in read('requirements.txt').split('\\n') if req != ''],
        cmdclass={'test': PyTest},
        tests_require=['pytest-cov', 'pytest'],
        command_options=command_options,
        entry_points={
            'console_scripts': CONSOLE_SCRIPTS
        }
    )

if __name__ == '__main__':
    setup_package()
