#!/usr/bin/python

## file:    log.py
## desc:    Simple output logging class.
## auth:    TR
#

import sys
import datetime as dt

class Colors(object):
    """
    Instantiates available terminal colors if we're attached to a TTY.
    """

    def __init__(self):

        if sys.stdin.isatty():
            self.green      = '\033[32m'
            self.red        = '\033[31m'
            self.white      = '\033[37m'
            self.yellow     = '\033[33m'
            self.ltgreen    = '\033[92m'
            self.ltred      = '\033[91m'
            self.ltwhite    = '\033[97m'
            self.ltyellow   = '\033[93m'

            self.bold   = '\033[1m'
            self.normal = '\033[0m'

        else:
            self.green      = ''
            self.red        = ''
            self.white      = ''
            self.yellow     = ''
            self.ltgreen    = ''
            self.ltred      = ''
            self.ltwhite    = ''
            self.ltyellow   = ''

            self.bold   = ''
            self.normal = ''

class Log(object):
    """
    Basic class for logging to to a file and/or stdout.
    """

    DEBUG = 'DEBUG'
    INFO = 'INFO'
    WARN = 'WARNING'
    ERROR = 'ERROR'

    def __init__(self, both=False, file='', on=True, prefix='', time=True):
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

        self.color = Colors()
        self.both = both
        self.file = file
        self.on = on
        self.prefix = prefix
        self.time = time

        if file:
            self.fh = open(file, 'w')

        else:
            self.fh = None
    
    def __del__(self):
        
        if self.fh:
            self.fh.close()

    def __to_color(self, level):
        """
        Converts a log level into a color.
        """

        lookup = {
            self.DEBUG: self.color.ltwhite,
            self.INFO: self.color.ltgreen,
            self.WARN: self.color.ltyellow,
            self.ERROR: self.color.ltred
        }

        return lookup.get(level)

    def __get_timestamp(self):
        """
        """

        now = dt.datetime.now()
        year = str(now.year)
        month = str(now.month)
        day = str(now.day)
        hour = str(now.hour)
        minute = str(now.minute)
        second = str(now.second)

        if len(month) == 1:
            month = '0' + month

        if len(day) == 1:
            day = '0' + day

        if len(hour) == 1:
            hour = '0' + hour

        if len(minute) == 1:
            minute = '0' + minute

        if len(second) == 1:
            second = '0' + second

        return '.'.join([year, month, day, hour, minute, second])

    def __write_file(self, level, s):
        """
        Internal function for writing text to a file.

        :type level: str 
        :arg level: log level

        :type s: str 
        :arg s: text being logged
        """

        if self.on and self.fh:
            os = ''

            if self.time:
                os += '[%s] ' % self.__get_timestamp()

            if self.prefix == 'level' or not self.prefix:
                os += '<%s> ' % level

            else:
                os += '%s ' % self.prefix

            os += '%s' % s

            print >> self.fh, os

    def __write_std(self, level, s):
        """
        Internal function for writing text to stdout.

        :type color: str 
        :arg color: color escape sequence generated from a Colors object

        :type s: str 
        :arg s: text being logged
        """

        color = self.__to_color(level)
        norm_color = self.color.normal

        if self.prefix:
            if self.prefix == 'level':
                pstr = '%s<%s> %s%s' % (color, level, s, norm_color)

            else:
                pstr = '%s%s %s%s' % (color, self.prefix, s, norm_color)

        else:
            pstr = '%s%s%s' % (color, s, self.color.normal)

        ## Errors are always printed!
        if level == self.ERROR:
            print pstr
        
        elif self.on and not self.fh or (self.fh and self.both):
            print pstr

    def debug(self, s):
        """
        Log output at the DEBUG level.

        :type s: str
        :arg s: text being logged
        """

        self.__write_file(self.DEBUG, s)
        self.__write_std(self.DEBUG, s)

    def info(self, s):
        """
        Log output at the INFO level.

        :type s: str
        :arg s: text being logged
        """

        self.__write_file(self.INFO, s)
        self.__write_std(self.INFO, s)

    def warn(self, s):
        """
        Log output at the WARN level.

        :type s: str
        :arg s: text being logged
        """

        self.__write_file(self.WARN, s)
        self.__write_std(self.WARN, s)

    def error(self, s):
        """
        Log output at the ERROR level.This is the only output that is active 
        even when logging is turned off (errors are important dood).

        :type s: str
        :arg s: text being logged
        """

        self.__write_file(self.ERROR, s)
        self.__write_std(self.ERROR, s)

    def turn_on(self, on=True):
        """
        Turns logging on or off based on the provided boolean.
        """

        if type(on) != bool:
            self.on = True

        else:
            self.on = on

    def set_prefix(self, prefix=''):
        """
        Sets an output prefix.
        """

        self.prefix = prefix

if __name__ == "__main__":
    log = Log()

    log.info("log.py -- Let's do some logging!")
    log.info('')
    log.debug('DEBUG\t| For debugging and developer messages')
    log.info('INFO\t| For general user messages')
    log.warn('WARNING\t| Things went wrong and you should probably know about it')
    log.error('ERROR\t| OH SHIT')

    log.info('')
    log.set_prefix('<+>')
    log.info('User defined prefix')
    log.set_prefix('level')
    log.info('Log level prefix')


