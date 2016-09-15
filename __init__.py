#!/usr/bin/env python2

## file:    __init__.py
## desc:    Module initialization and db connection stuff. 
## vers:    0.6.147
## auth:    TR
#

from os import path
import ConfigParser

from batch import BatchReader
from batch import BatchWriter
from log import Log
import db
import util

## Config is in the same folder as the rest of the GW library
CONFIG_PATH = 'gwlib.cfg'
VERSION = '0.6.144'

        ## CONFIGURATION ##
        ###################

def __create_config():
    """
    Creates a default config containing database connection info.
    """

    with open(CONFIG_PATH, 'w') as fl:

        print >> fl, '## GWLib DB Configuration'
        print >> fl, '#'
        print >> fl, ''
        print >> fl, '[db]'
        print >> fl, '## Postgres server address'
        print >> fl, 'host = 127.0.0.1'
        print >> fl, '## Postgres database name'
        print >> fl, 'database = dbname'
        print >> fl, 'port = 5432'
        print >> fl, 'user = someguy'
        print >> fl, 'password = somepassword'
        print >> fl, '## Force psycopg2 to commit after every statement'
        print >> fl, 'autocommit = false'
        print >> fl, '## GeneWeaver usr_id to use when inserting genesets'
        print >> fl, 'usr_id = 3507787'
        print >> fl, ''

def __load_config():
    """
    Attempts to load the lib config. If it doesn't exist, one is created. Makes
    no attempt to check if the config is correct since any errors will prevent
    the application from loading.

    :ret object: the parsed config as a ConfigParser object
    """

    if not path.exists(CONFIG_PATH):
        __create_config()

        print '[!] A new config was created for the DB library.'
        print '[!] You should probably fill it out.'

    parser = ConfigParser.RawConfigParser()

    parser.read(CONFIG_PATH)

    return parser

def __get(section, option):
    """
    Retrieves the value for an option under the specified section.

    :type section: str
    :arg section: config section an option falls under

    :type option: str
    :arg option: option 

    :ret str: the value associated with the given section, option combo
    """

    return PARSER.get(section, option)

def initialize_db():
    """
    Reads in the config file containing DB auth info and initializes the
    connection. If the config file doesn't exist, one is created. Only needs 
    to be called when using the DB module. 
    """
    pass

## Only time this really ever fails is when the config is bad or the
## postgres server isn't running.
try:
    parser = __load_config()
    ##
    host = parser.get('db', 'host')
    database = parser.get('db', 'database')
    user = parser.get('db', 'user')
    password = parser.get('db', 'password')
    port = parser.get('db', 'port')
    ##
    constr = "host='%s' dbname='%s' user='%s' password='%s' port='%s'" 
    constr = constr % (host, database, user, password, port)
    db.conn = db.psycopg2.connect(constr)

    db.conn.autocommit = parser.getboolean('db', 'autocommit')

except Exception as e:
    print '[!] Oh noes, failed to connect to the db. Will attempt to continue.'
    print 'The exception:'
    print e

    #exit()

