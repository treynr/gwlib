#!/usr/bin/env python
# -*- coding: utf-8 -*-

from gwlib import util

VERSION = (1, 2, 0)

def get_build():
    try:
        return '-' + util.get_git_info()
    except Exception:
        return ''

__version__ = '{}{}'.format('.'.join(map(str, VERSION)), get_build()).strip()
