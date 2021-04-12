import logging
import sys
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler



class loghandler(object):
    """
    Sometimes you want to let a log file grow to a certain size, then open a new file and log to that.
    You may want to keep a certain number of these files, and when that many files have been created,
    rotate the files so that the number of files and the size of the files both remain bounded.
    For this usage pattern, the logging package provides a RotatingFileHandler

    StreamHandler is used to write the logs to console. It sends logging output to streams such as
    sys.stdout, sys.stderr
    """

    def __init__(self, fmt, logfile):
        self.fmt = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
        self.logfile = "logs/application.log"

    def get_consoleHandler(self):
        """It's console handler for logging events to console
        This add the log message handler to the logger

        :return: Returns a new instance of the StreamHandler class. If stream is specified,
        the instance will use it for logging sys.stdout, sys.stderr will be used
        """
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(self.fmt)
        return console_handler


    def get_fileHhandler(self):
        """It's console handler for logging events to log file based on the rotaion policy.
        The size of the rotated files is made small so you can see the results easily
        This add the log message handler to the logger

        :return: Returns a new instance of the RotatingFileHandler class. The specified file is
        opened and used as the stream for logging
        """
        file_handler = RotatingFileHandler(self.logfile, maxBytes=5000, backupCount=0)
        file_handler.setFormatter(self.fmt)
        return file_handler


    def getLogger(logger_name):
        """Set up a specific logger with our desired output level
        with this pattern, we can propagate the error up to parent. but for now its set to False.
        Make it true when when its necessary.

        :param logger_name: Name of the logger
        :return: write logs to console and file
        """
        x = loghandler(fmt=None, logfile=None)
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)  # better to have too much log than not enough
        logger.addHandler(x.get_consoleHandler())
        logger.addHandler(x.get_fileHhandler())
        logger.propagate = False
        return logger
