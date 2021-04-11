import logging
import sys
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

APP_LOG_FILE = "logs/application.log"
FORMAT = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")


def get_consoleHandler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMAT)
    return console_handler


def get_fileHhandler():
    file_handler = RotatingFileHandler(APP_LOG_FILE, maxBytes=5000, backupCount=0)
    file_handler.setFormatter(FORMAT)
    return file_handler


def getLogger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)  # better to have too much log than not enough
    logger.addHandler(get_consoleHandler())
    logger.addHandler(get_fileHhandler())
    # with this pattern, it's rarely necessary to propagate the error up to parent
    logger.propagate = False
    return logger
