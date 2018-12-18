#!/usr/bin/env python

## file: log.py
## desc: Simple output logging class.
## auth: TR
#

from collections import namedtuple
import sys
import datetime as dt
import itertools as it
import logging

Colors = namedtuple(
    'Colors', 
    'green red white yellow ltgreen ltred ltwhite ltyellow bold normal'
)
Colors.__new__.__defaults__ = ('',) * 10

if sys.stdin.isatty():

    colors = Colors(
        '\033[32m',
        '\033[31m',
        '\033[37m',
        '\033[33m',
        '\033[92m',
        '\033[91m',
        '\033[97m',
        '\033[93m',
        '\033[1m',
        '\033[0m'
    )

else:
    colors = Colors()

class ConsoleFilter(logging.Filter):

    def filter(self, record):

        ## I don't think I should be doing this but whatever lol
        if record.levelno == 10: 
            record.msg = '{}{}'.format(colors.ltwhite, record.msg)
        elif record.levelno == 20:
            record.msg = '{}{}'.format(colors.ltgreen, record.msg)
        elif record.levelno == 30:
            record.msg = '{}{}'.format(colors.ltyellow, record.msg)
        elif record.levelno == 40:
            record.msg = '{}{}'.format(colors.ltred, record.msg)

        return True

class Log(object):
    """
    Basic logging class that encapsulates python's logging classes and functions.
    """

    def __init__(self, console=True, filename='', name='log', on=True, cfmt='', ffmt=''):
        """
        Initialize a logging object.

        :type both: bool
        :arg both: if true logs to both stdout and a file

        :type file: str
        :arg file: filepath to a log file

        :type on: bool
        :arg on: if true logging is turned on, prints to stdout and/or a file

        :type prefix: str
        :arg prefix: prefix the given string to the beginning of all output. A
                     special string can be given, 'level', and the log level 
                     will be added to output strings
        """

        self.color = colors
        self.filename = filename
        self.cfmt = cfmt
        self.ffmt = ffmt
        self.logger = logging.getLogger(name)

        self.logger.setLevel(logging.DEBUG)

        if console:
            conlog = logging.StreamHandler()

            conlog.setLevel(logging.DEBUG)
            conlog.setFormatter(logging.Formatter(cfmt if cfmt else '%(message)s'))
            conlog.addFilter(ConsoleFilter())

            self.logger.addHandler(conlog)

        if filename:
            filelog = logging.StreamHandler()

            filelog.setLevel(logging.INFO)
            filelog.setFormatter(logging.Formatter(ffmt if ffmt else '%(message)s'))

            self.logger.addHandler(filelog)

    def debug(self, s):
        """
        Log output at the DEBUG level.

        :type s: str
        :arg s: text being logged
        """

        self.logger.debug(s)

    def info(self, s):
        """
        Log output at the INFO level.

        :type s: str
        :arg s: text being logged
        """

        self.logger.info(s)

    def warn(self, s):
        """
        Log output at the WARN level.

        :type s: str
        :arg s: text being logged
        """

        self.logger.warn(s)

    def error(self, s):
        """
        Log output at the ERROR level.This is the only output that is active 
        even when logging is turned off (errors are important dood).

        :type s: str
        :arg s: text being logged
        """

        self.logger.error(s)

    def turn_on(self, on=True):
        """
        Turns logging on or off based on the provided boolean.
        """

        if type(on) != bool:
            self.on = True

        else:
            self.on = on

if __name__ == "__main__":
    log = Log()

    log.info("log.py -- Logs and stuff")
    log.info('')
    log.debug('DEBUG\t| For debugging and developer messages')
    log.info('INFO\t| For general user messages')
    log.warn('WARNING\t| Things went wrong and you should probably know about it')
    log.error('ERROR\t| OH SHIT')


