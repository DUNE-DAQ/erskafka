#!/usr/bin/env python3

from erskafka.ERSKafkaLogHandler import ERSKafkaLogHandler
import logging

logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

ers = ERSKafkaLogHandler(session='session_tester')

stdout = logging.StreamHandler()


logger.addHandler(ers)
logger.addHandler(stdout)

logger.debug   ("This is a debug message from the logger")
logger.info    ("This is an info message from the logger")
logger.warning ("This is a warning message from the logger")
logger.error   ("This is an error message from the logger")
logger.critical("This is a critical message from the logger")

class a_class:
    def __init__(self):
        a_class_logger = logging.getLogger("a_class_logger")
        a_class_logger.setLevel(logging.DEBUG)
        a_class_logger.addHandler(ers)
        a_class_logger.addHandler(stdout)

        a_class_logger.debug   ("This is a debug message from a_class_logger")
        a_class_logger.info    ("This is an info message from a_class_logger")
        a_class_logger.warning ("This is a warning message from a_class_logger")
        a_class_logger.error   ("This is an error message from a_class_logger")
        a_class_logger.critical("This is a critical message from a_class_logger")

a = a_class()

def a_function():

    a_function_logger = logging.getLogger("a_function_logger")
    a_function_logger.setLevel(logging.DEBUG)
    a_function_logger.addHandler(ers)
    a_function_logger.addHandler(stdout)


    a_function_logger.debug   ("This is a debug message from a_function_logger")
    a_function_logger.info    ("This is an info message from a_function_logger")
    a_function_logger.warning ("This is a warning message from a_function_logger")
    a_function_logger.error   ("This is an error message from a_function_logger")
    a_function_logger.critical("This is a critical message from a_function_logger")


a_function()