#!/usr/bin/env python

import util

VERSION = (1, 0, 0)

def get_build():
    try:
        return '({})'.format(util.get_git_info())
    except Exception:
        return ''

__version__ = '{} {}'.format('.'.join(map(str, VERSION)), get_build()).strip()
