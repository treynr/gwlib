#!/usr/bin/env python2

## file: config.py
## desc: Configuration file parsing.
## auth: TR

import ConfigParser
import os

## Path to the configuration file
CONFIG_PATH = './gwlib.cfg'

## Global config object
CONFIG = None

def generate_sample_config(fp=CONFIG_PATH):
    """
    Creates a sample config file at the default config filepath.
    """

    with open(fp, 'w') as fl:

        print >> fl, '\n'.join([
            '## gwlib configuration',
            '#',
            '',
            '[database]',
            'database = dbname',
            'host = 127.0.0.1',
            'user = dbuser',
            'password = dbpass',
            'port = 5432',
            '',
            '[logging]',
            '## If a filepath is given, logs will be saved to the given file',
            'filepath = dj.log',
            '## If console is true, log output to stderr',
            'console = true'
        ])

def check_integrity(cfg=CONFIG):
    """
    Checks to make sure the required key-value pairs are present in the config file.
    """

    if not cfg:
        return (False, 'Config file failed to load for some unknown reason')

    if not cfg.has_section('database'):
        return (False, 'Config file is missing the database section')

    options = ['database', 'host', 'user', 'password', 'port']

    for opt in options:

        if not cfg.has_option('database', opt):
            return (False, 'Config file is missing the {} option'.format(opt))

    return (True, '')

def load_config(fp=CONFIG_PATH):
    """
    Attempts to load and parse a configuration file.
    """

    global CONFIG

    ## The config doesn't exist so we create a sample one and inform the user
    if not os.path.exists(fp):
        generate_sample_config(fp)

        return (
            False,
            'Could not find a config file so one was created for you @ {}'.format(fp)
        )

    CONFIG = ConfigParser.RawConfigParser(allow_no_value=True)

    CONFIG.read(fp)

    return (True, CONFIG)

def get(section, option, cfg=None):
    """
    Retrieves the value for an option from a config section.
    """

    if not cfg:
        global CONFIG

        cfg = CONFIG

    if not cfg:
        return ''

    return cfg.get(section, option)

def get_db(option, cfg=None):
    """
    Retrieves an option from the database section.
    """

    return get('database', option, cfg)

def get_log(option, cfg=None):
    """
    Retrieves an option from the logging section.
    """

    return get('logging', option, cfg)

