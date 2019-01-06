#!/usr/bin/env python
# -*- coding: utf-8 -*-

## file: test_config.py
## desc: Unit tests for config.py.
## auth: TR

from gwlib import config

config.load_config('tests/test.cfg')

def test_config_db():

    assert config.get_db('database') == 'test_db'
    assert config.get_db('host') == '127.0.0.1'
    assert config.get_db('user') == 'test_user'
    assert config.get_db('password') == 'test_password'
    assert config.get_db('port') == '5432'
