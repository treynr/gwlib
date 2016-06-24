#!/usr/bin/env python2

## file:    __init__.py
## desc:    Module initialization and db connection stuff. 
## vers:    0.5.112
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

        ## CONFIGURATION ##
        ###################

def create_config():
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

def load_config():
    """
    Attempts to load the lib config. If it doesn't exist, one is created. Makes
    no attempt to check if the config is correct since any errors will prevent
    the application from loading.

    :ret object: the parsed config as a ConfigParser object
    """

    if not path.exists(CONFIG_PATH):
        create_config()

        print '[!] A new config was created for the DB library.'
        exit()

    parser = ConfigParser.RawConfigParser()

    parser.read(CONFIG_PATH)

    return parser

def get(section, option):
    """
    Retrieves the value for an option under the specified section.

    :type section: str
    :arg section: config section an option falls under

    :type option: str
    :arg option: option 

    :ret str: the value associated with the given section, option combo
    """

    return PARSER.get(section, option)

## Only time this really ever fails is when the config is bad or the postgres
## server isn't running. Executed on import.
try:
    parser = load_config()
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
    print '[!] Oh noes, failed to connect to the db'
    print 'The exception:'
    print e

    exit()

