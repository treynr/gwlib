#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup
import os

NAME = 'gwlib'
REQUIRED = ['psycopg2']

about = {}
here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, NAME, '__version__.py')) as fl:
    exec(fl.read(), about)

setup(
    name=NAME,
    version=about['__version__']
    description='GeneWeaver python library',
    author='TR',
    python_requires='>=2.7.5',
    install_requires=REQUIRED,
    license='GPLv3',
    packages=['gwlib']
)
