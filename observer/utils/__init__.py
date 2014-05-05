# coding=utf8
#
#


from cookielib import CookieJar
from observer import log
from twisted.internet import reactor, task, ssl
from OpenSSL import SSL


try:
    import json
except ImportError:
    import simplejson as json



def wait(seconds):
    ''' 模拟sleep '''
    def dummy():
        return

    return task.deferLater(reactor, seconds, dummy)


def hex_encode_bytes(s):
    ''' '''
    return ''.join('%02x' % ord(c) for c in s)
