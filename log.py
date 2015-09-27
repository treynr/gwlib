#!/usr/bin/python

#### file:	log.py
#### desc:	Simple output logging class.
#### vers:	0.1.0
#### auth:	TR
##

import sys

#### Colors
##
#### Instantiates available terminal colors if we're attached to a TTY.
##
class Colors(object):

	def __init__(self):

		if sys.stdin.isatty():
			self.green		= '\033[32m'
			self.red 		= '\033[31m'
			self.white 		= '\033[37m'
			self.yellow 	= '\033[33m'
			self.ltgreen 	= '\033[92m'
			self.ltred 		= '\033[91m'
			self.ltwhite 	= '\033[97m'
			self.ltyellow 	= '\033[93m'

			self.bold 	= '\033[1m'
			self.normal = '\033[0m'

		else:
			self.green		= ''
			self.red 		= ''
			self.white 		= ''
			self.yellow 	= ''
			self.ltgreen 	= ''
			self.ltred 		= ''
			self.ltwhite 	= ''
			self.ltyellow 	= ''

			self.bold 	= ''
			self.normal = ''


#### Log
##
#### Basic class for logging text to a file and/or std out.
##
class Log(object):

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

	#### __writeFile
	##
	#### Internal function for writing text to a file.
	##
	#### arg: string, output level--should be DEBUG|INFO|WARN|ERROR
	#### arg: string, text to ouput
	##
	def __writeFile(self, level, s):

		if self.on and self.fh:
			print >> self.fh, '<%s> %s' % (level, s)

	#### __writeStd
	##
	#### Internal function for writing text to a std out.
	##
	#### arg: string, color escape sequence from a Colors object
	#### arg: string, text to ouput
	##
	def __writeStd(self, color, s):
		
		if not self.fh or (self.fh and self.both):
			print '%s%s%s' % (color, s, self.color.normal)

	#### debug
	##
	#### Log output at the DEBUG level.
	##
	#### arg: string, text to output
	##
	def debug(self, s):

		self.__writeFile(self.DEBUG, s)
		self.__writeStd(self.color.ltwhite, s)

	#### info
	##
	#### Log output at the INFO level.
	##
	#### arg: string, text to output
	##
	def info(self, s):

		self.__writeFile(self.INFO, s)
		self.__writeStd(self.color.ltgreen, s)

	#### warn
	##
	#### Log output at the WARN level.
	##
	#### arg: string, text to output
	##
	def warn(self, s):

		self.__writeFile(self.WARN, s)
		self.__writeStd(self.color.ltyellow, s)

	#### error
	##
	#### Log output at the ERROR level.
	##
	#### arg: string, text to output
	##
	def error(self, s):

		self.__writeFile(self.ERROR, s)
		self.__writeStd(self.color.ltred, s)

	#### turnOn
	##
	#### Turns on logging.
	##
	def turnOn(self):

		self.on = True

	#### turnOff
	##
	#### Turns off logging.
	##
	def turnOff(self):

		self.on = False

