# coding=utf8

import re
from twisted.internet import reactor, task

WEIBO_LOGIN_SUCCEED_PATTERN=re.compile('try\\{sinaSSOController.setCrossDomainUrlList\\((?P<js>\\{[^\\}]*\\})\\).*catch.*sinaSSOController.crossDomainAction\\(.*location.replace\\(\'(?P<url>[^\']*)\'\\);\\}\\);\\}')
WEIBO_NEED_LOGIN_PATTERN=re.compile("\\$CONFIG\\['islogin'\\]\\s*=\\s*'0';")
WEIBO_REDIRECT_PATTERN=re.compile('location\\.replace\\("(?P<url>[^"]*)"\\)')


def hex_encode_bytes(s):
    ''' '''
    return ''.join('%02x' % ord(c) for c in s)

def wait(seconds):
    ''' '''
    def dummy():
        return

    return task.deferLater(reactor, seconds, dummy)
