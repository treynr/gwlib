#!/usr/bin/python

#### file:  log.py
#### desc:  Simple output logging class.
#### vers:  0.1.0
#### auth:  TR
##

import sys

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

    DEBUG = 'debug'
    INFO = 'info'
    WARN = 'warning'
    ERROR = 'error'

    def __init__(self, both=False, file='', on=True):

        self.color = Colors()
        self.both = both
        self.file = file
        self.on = on

        if file:
            self.fh = open(file, 'w')

        else:
            self.fh = None
    
    def __del__(self):
        
        if self.fh:
            self.fh.close()

    def __write_file(self, level, s):
        """
        Internal function for writing text to a file.

        :type level: str 
        :arg level: log level

        :type s: str 
        :arg s: text being logged
        """

        if self.on and self.fh:
            print >> self.fh, '<%s> %s' % (level, s)

    def __write_std(self, color, s):
        """
        Internal function for writing text to stdout.

        :type color: str 
        :arg color: color escape sequence generated from a Colors object

        :type s: str 
        :arg s: text being logged
        """

        ## Errors are always printed!
        if color == self.color.ltred:
            print '%s%s%s' % (color, s, self.color.normal)
        
        elif self.on and not self.fh or (self.fh and self.both):
            print '%s%s%s' % (color, s, self.color.normal)

    def debug(self, s):
        """
        Log output at the DEBUG level.

        :type s: str
        :arg s: text being logged
        """

        self.__write_file(self.DEBUG, s)
        self.__write_std(self.color.ltwhite, s)

    def info(self, s):
        """
        Log output at the INFO level.

        :type s: str
        :arg s: text being logged
        """

        self.__write_file(self.INFO, s)
        self.__write_std(self.color.ltgreen, s)

    def warn(self, s):
        """
        Log output at the WARN level.

        :type s: str
        :arg s: text being logged
        """

        self.__write_file(self.WARN, s)
        self.__write_std(self.color.ltyellow, s)

    def error(self, s):
        """
        Log output at the ERROR level.This is the only output that is active 
        even when logging is turned off (errors are important dood).

        :type s: str
        :arg s: text being logged
        """

        self.__write_file(self.ERROR, s)
        self.__write_std(self.color.ltred, s)

    def turn_on(self):
        """
        Turns logging on.
        """

        self.on = True

    def turn_off(self):
        """
        Turns logging off.
        """

        self.on = False

if __name__ == "__main__":
    log = Log()

    log.info("log.py -- Let's do some logging!")
    log.info('')
    log.debug('DEBUG\t| For debugging and developer messages')
    log.info('INFO\t| For general user messages')
    log.warn('WARNING\t| Things went wrong and you should probably know about it')
    log.error('ERROR\t| OH SHIT')



