
``log.py`` Module API
=====================

Documentation for the classes and functions in the ``log`` module.

Classes
-------

``class ConsoleFilter(logging.filter)``
'''''''''''''''''''''''''''''''''''''''

A logging filter attached to the console handler.
It simply adds color and symbol features to messaged based on user-supplied format
strings.

If ``%(color)s`` is part of the format string supplied to the logger, the remainder of
the message will be colored based on the logging level.
If ``%(symbol)s`` is part of the format string supplied to the logger, a small string of 
characters will be inserted into the message based on the logging level.


Functions
---------

``log.attach_console_logger(log, format, level=logging.DEBUG)``
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Attaches a console logger to the given logging object. 
Adds a special filter object so messages can be printed to the terminal in color.

Arguments:
^^^^^^^^^^

- log:    Python logging object
- format: logging Formatter string
- level:  logging level, default is DEBUG

----


``log.attach_file_logger(log, filepath, format, level=logging.DEBUG)``
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Attaches a file handler to the given logging object.

Arguments:
^^^^^^^^^^

- log:      Python logging object
- filepath: log filepath
- format:   logging Formatter string
- level:    logging level

----


``log.attach_rotating_file_logger(log, filepath, format, level=logging.DEBUG)``
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Attaches a rotating file handler to the given logging object.
The rotating logger will always be rolled over every time the application runs.

Arguments:
^^^^^^^^^^

- log:      Python logging object
- filepath: log filepath
- format:   logging Formatter string
- level:    logging level

