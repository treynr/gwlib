#!/usr/bin/env python

## file: log.py
## desc: Simple output logging class.
## auth: TR
#

from collections import namedtuple
import sys
import logging
import logging.handlers

Colors = namedtuple(
    'Colors',
    'green red white yellow ltgreen ltred ltwhite ltyellow bold normal'
)
Colors.__new__.__defaults__ = ('',) * 10

## Check if the user is running this from a terminal
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
    """
    Logging filter attached to the console handler.
    All this does add color and symbol features to messages based on user-supplied format
    strings.
    """

    def filter(self, record):

        record.default_color = colors.ltwhite

        ## DEBUG
        if record.levelno == 10:
            record.color = colors.ltwhite
            record.symbol = '[*]'

        ## INFO
        elif record.levelno == 20:
            record.color = colors.ltgreen
            record.symbol = '[+]'

        ## WARNING
        elif record.levelno == 30:
            record.color = colors.ltyellow
            record.symbol = '[-]'

        ## ERROR/CRITICAL
        elif record.levelno == 40 or record.levelno == 50:
            record.color = colors.ltred
            record.symbol = '[!]'

        return True

def attach_console_logger(log, format, level=logging.DEBUG):
    """
    Attaches a console logger to the given logging object. Adds a special filter object
    so messages can be printed to the terminal in color.

    arguments
        log:    Python logging object
        format: logging Formatter string
        level:  logging level
    """

    conlog = logging.StreamHandler()

    log.setLevel(level)
    conlog.setLevel(level)
    conlog.setFormatter(logging.Formatter(format))
    conlog.addFilter(ConsoleFilter())

    log.addHandler(conlog)

    return log

def attach_file_logger(log, filepath, format, level=logging.DEBUG):
    """
    Attaches a file handler to the given logging object.

    arguments
        log:      Python logging object
        filepath: log filepath
        format:   logging Formatter string
        level:    logging level
    """

    filelog = logging.FileHandler(filename=filepath)

    log.setLevel(level)
    filelog.setLevel(level)
    filelog.setFormatter(logging.Formatter(format))

    log.addHandler(filelog)

def attach_rotating_file_logger(log, filepath, format, level=logging.DEBUG):
    """
    Attaches a rotating file handler to the given logging object.
    The rotating logger will always be rolled over every time the application runs.

    arguments
        log:      Python logging object
        filepath: log filepath
        format:   logging Formatter string
        level:    logging level
    """

    filelog = logging.handlers.RotatingFileHandler(filename=filepath)

    ## Immediately roll the log over
    filelog.doRollover()
    filelog.setLevel(level)
    filelog.setFormatter(logging.Formatter(format))

    log.addHandler(filelog)
