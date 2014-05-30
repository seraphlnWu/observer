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


__es = ''.join(reduce(lambda x, y: x + y, (
    map(chr, range(ord('0'), ord('9') + 1)),
    map(chr, range(ord('a'), ord('z') + 1)),
    map(chr, range(ord('A'), ord('Z') + 1)),
)))
__de = dict((k, i) for i, k in enumerate(__es))


def url_decode(s):
    ''' '''
    s = s.split('?')[0]
    r = 0
    l = len(s)
    b = l % 4
    if b == 0:
        b = 4
    po = 0
    for o in xrange(b, l+1, 4):
        r *= 10000000
        r += reduce(lambda a, b: a * 62 + __de[b], s[po:o], 0)
        po = o

    return r
