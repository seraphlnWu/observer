# coding=utf8

from twisted.python import log

from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL


def debug(msg):
    ''' debug level '''
    log.msg(msg, logLevel=DEBUG)


def info(msg):
    ''' info level '''
    log.msg(msg, logLevel=INFO)


def warning(msg):
    ''' warning level '''
    log.msg(msg, logLevel=WARNING)


def error(msg):
    ''' error level '''
    log.msg(msg, logLevel=ERROR)


def critical(msg):
    ''' critical level '''
    log.msg(msg, logLevel=CRITICAL)


def exception(_failure=None, _wky=None):
    ''' '''
    log.err(_failure, _wky)
