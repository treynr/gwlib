#!/usr/bin/env python

## file:    ncbi.py
## desc:    Simple wrapper for the NCBI E-utilities API.
##          Currently only supports fetch and search functionality.
## auth:    TR
#

import json
import time
import urllib as url
import urllib2 as url2

## Email and tool names are required for NCBI eutils usage
NAME = 'geneweaver.org'
EMAIL = 'timothy_reynolds@baylor.edu'

## Data export tag so we know how the data was generated
#DTAG = reduce(lambda x, y: x + ' ' + y, sys.argv)

## NCBI E-utils URLs 
EBASE = 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/'
ESEARCH = EBASE + 'esearch.fcgi?'
EFETCH = EBASE + 'efetch.fcgi?'

def rate(new_rate=None):
    """
    The rate function holds a static rate variable (a timestamp) used by the  
    limit_rate funciton.

    arguments
        new_rate: a float, the current processor time used as the new rate

    returns
        a float indicating the current rate
    """

    if 'rate' not in dir(rate):
        rate.rate = 0

    if new_rate:
        rate.rate = new_rate

    return rate.rate

def limit_rate():
    """
    Limits the rate of NCBI calls to three a second. Anything faster and the
    banhammer is thrown at us. 
    """

    if rate() == 0:
        rate(time.clock())

    else:
        start = rate()
        elapsed = time.clock() - start

        ## Checks to see if time between the last rate call is ~1/3 of a second
        ## and if it's less, the script sleeps the difference
        if elapsed < 0.34:
            time.sleep(0.34 - elapsed)

        rate(time.clock())


def check_api_defaults(db, opts):
    """
    Checks and sets default E-Utility options where necessary.

    :type db: str
    :arg db: the database 
    """

    if not opts.get('email', None):
        opts['email'] = _EMAIL

    if not opts.get('tool', None):
        opts['tool'] = _NAME

    if not opts.get('retmax', None):
        opts['retmax'] = 5000

    return opts

def check_search_defaults(opts):
    """
    """

    ## These are supported rettype/retmode values. Default values are indicated
    ## by a value of 1.
    rettype = {'uilist': 1, 'count': 0}
    retmode = {'json': 1, 'xml': 0}

    if not opts.get('rettype', None) or opts['rettype'] not in rettype:
        opts['rettype'] = 'uilist'

    if not opts.get('retmode', None) or opts['retmode'] not in retmode:
        opts['retmode'] = 'json'

    return opts

def check_fetch_defaults(opts):
    """
    EFetch supports a wide range of rettype and retmode parameters. Since there
    are lots of possible values, and I'm lazy, this function only checks to see
    if retmode/rettype are set.
    """

    if not opts.get('rettype', None):
        opts['rettype'] = 'uilist'

    if not opts.get('retmode', None):
        opts['retmode'] = 'text'

    return opts

def e_fetch(uids, db, opts={}):
    """
    Uses the EFetch API to fetch records for a given list of UIDs from a given
    NCBI database.

    :type uids: list
    :arg uids: NCBI UIDs

    :type db: str
    :arg db: database name

    :type opts: dict
    :arg opts: key, value pairs of options to use with EFetch

    :ret: a string of the results
    """

    opts = check_api_defaults(db, opts)
    opts = check_fetch_defaults(opts)

    if not uids or not db:
        return {}

    opts['id'] = ','.join(map(lambda s: str(s), uids))
    opts['db'] = db

    api_data = url.urlencode(opts)
    res = ''

    ## API calls limited to three per second
    limit_rate()

    for attempt in range(5):
        try:
            req = url2.Request(EFETCH, api_data)
            res = url2.urlopen(req)

        except url2.HTTPError as e:
            continue

        res = res.read()

        break

    ## for-else construct, if the loop doesn't break (in our case this
    ## indicates success) then this statement is executed
    else:
        pass

    return res

def e_search(term, db, opts={}):
    """
    Uses the ESearch API to search a given database for a given term. The
    default return result depends on the database queried. 

    :type term:
    :arg term:

    :type db:
    :arg db:

    :type opts:
    :arg opts:

    :ret: a JSON object
    """

    opts = check_api_defaults(db, opts)
    opts = check_search_defaults(opts)

    ## Term cannot be empty, but if an organism is provided we can serach
    ## all db entries related to that organism
    if not term and opts.get('organism', None):
        term = opts['organism'] + '[organism]'

    elif not term or not db:
        return {}

    opts['term'] = term
    opts['db'] = db

    api_data = url.urlencode(opts)
    res = ''

    ## API calls limited to three per second
    limit_rate()

    for attempt in range(5):
        try:
            req = url2.Request(ESEARCH, api_data)
            res = url2.urlopen(req)

        except url2.HTTPError as e:
            continue

        res = res.read()

        break

    ## for-else construct, if the loop doesn't break (in our case this
    ## indicates success) then this statement is executed
    else:
        pass

    return json.loads(res)

