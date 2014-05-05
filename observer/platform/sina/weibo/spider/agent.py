# coding=utf8

import json
import time
import random

from hashlib import sha1
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5

from base64 import b64encode
from urllib import urlencode, quote_plus

from observer import log

from twisted.internet import reactor, task
from twisted.internet.defer import returnValue, Deferred, inlineCallbacks

from twisted.web.http_headers import Headers

from observer.platform.sina.spider.error import NoAgentError

from observer.platform.sina.spider.utils import (
    WEIBO_NEED_LOGIN_PATTERN,
    WEIBO_REDIRECT_PATTERN,
    WEIBO_LOGIN_SUCCEED_PATTERN,
    hex_encode_bytes,
    wait,
)
from observer.platform.sina.spider.error import InfiniteLoginError
from observer.platform.sina.spider.http import request, HTTPBodyReceiver


class LoginResponse(object):
    ''' '''

    def __init__(self, response, body, reason):
        ''' '''
        self.original = response
        self.data = body
        self.reason = reason

    def __getattr__(self, name):
        ''' '''
        return getattr(self.original, name)

    def deliverBody(self, protocol):
        ''' '''
        protocol.dataReceived(self.data)
        protocol.connectionLost(self.reason)


class LoginAgent(object):
    ''' '''

    def __init__(self, agent, retryLimit=1):
        ''' '''
        self._agent = agent
        self.loggedin = False
        self.retryLimit = retryLimit

    def login(self):
        ''' '''
        raise NotImplementedError("Must Implement the login method")

    def testLogin(self, content):
        ''' '''
        raise NotImplementedError("Must Implement the test login method.")

    @inlineCallbacks
    def request(self, method, uri, headers=None, bodyProducer=None):
        ''' '''
        retryCount = 0
        while True:
            if not self.loggedin:
                yield self.login()
                retryCount += 1
            if self.loggedin:
                response = yield self._agent.request(
                    method,
                    uri,
                    headers,
                    bodyProducer,
                )

                finished = Deferred()
                p = HTTPBodyReceiver(finished)

                response.deliverBody(p)
                body = yield finished
                reason = p.reason

                body = yield self.testLogin(body)
            if self.loggedin:
                returnValue(LoginResponse(response, body, reason))
                return
            if bodyProducer is not None:
                returnValue(None)
                return
            if retryCount >= self.retryLimit:
                raise InfiniteLoginError("Maximum retry limit reached")


class SinaLoginAgent(LoginAgent):
    ''' '''

    @staticmethod
    def encodeUsername(username):
        ''' '''
        return b64encode(quote_plus(username))

    @staticmethod
    def rsaEncodePassword(password, servertime, nonce, pubkey):
        ''' '''
        key = RSA.construct((pubkey, 0x10001L))
        cipher = PKCS1_v1_5.new(key)

        ciphertext = cipher.encrypt(
            str(servertime) + '\t' + str(nonce) + '\n' + password
        )

        return hex_encode_bytes(ciphertext)

    @staticmethod
    def wsseEncodePassword(password, servertime, nonce):
        ''' '''
        h = lambda s: sha1(s).hexdiest()
        return h(h(h(password)) + str(servertime) + nonce)

    def __init__(self, agent, username, password, retryLimit=1):
        ''' '''
        self.username = username
        self.password = password
        self.loggedin = False
        self.pool = None

        LoginAgent.__init__(self, agent, retryLimit)

    @inlineCallbacks
    def _prelogin(self):
        ''' '''
        log.debug("Begin _prelogin")
        content = yield request(
            self._agent,
            'http://login.sina.com.cn/sso/prelogin.php?entry=miniblog&su='\
                + SinaLoginAgent.encodeUsername(self.username)\
                + '&rsakt=mod',
        )

        d = json.loads(content)
        log.debug("End _prelogin")
        returnValue((
            d['servertime'],
            d['nonce'].encode('utf8'),
            int(d['pubkey'], 16),
            d['rsakv'],
        ))

    @inlineCallbacks
    def _login(self, d):
        ''' '''
        content = yield request(
            self._agent,
            'http://login.sina.com.cn/sso/login.php?entry=weibosousuo',
            Headers({'Content-Type': ['application/x-www-form-urlencoded']}),
            urlencode(d),
        )

        log.debug('Got login.php: ' + content)
        m = WEIBO_LOGIN_SUCCEED_PATTERN.search(content)
        if m is None:
            m = WEIBO_REDIRECT_PATTERN.search(content)

        if m:
            yield request(self._agent, m.group('url'))
            self.loggedin = True
        else:
            self.loggedin = False

    @inlineCallbacks
    def login(self):
        ''' '''
        if self.pool is None:
            yield wait(random.uniform(10.0, 20.0))
        else:
            now = time.time()
            login_time = self.pool.lastLogin + self.pool.loginInterval
            self.pool.lastLogin = max(now, login_time)
            if now < login_time:
                yield wait(login_time - now)

        servertime, nonce, pubkey, rsakv = yield self._prelogin()
        log.debug("Got _prelogin: %d,%s,%d,%s" % (servertime, nonce, pubkey, rsakv))

        d = {
            'su': self.encodeUsername(self.username),
            'sp': self.rsaEncodePassword(
                self.password,
                servertime,
                nonce,
                pubkey,
            ),
            'url': 'http://weibo.com/login.php?url=http%3A%2F%2Fs.weibo.com%2F',
            'returntype': 'META',
            'encoding': 'utf-8',
            'pwencode': 'rsa2',
            'servertime': servertime,
            'nonce': nonce,
            'gateway': 1,
            'rsakv': rsakv, 
        }

        log.debug("Processing: " + repr(d))
        yield self._login(d)

    @inlineCallbacks
    def testLogin(self, content):
        ''' '''
        m = WEIBO_REDIRECT_PATTERN.search(content)
        if m:
            new_url = m.group('url')
            content = yield request(self._agent, new_url)
        if WEIBO_NEED_LOGIN_PATTERN.search(content):
            log.debug("Need to login: " + content)
            self.loggedin = False
            returnValue(None)
            return

        self.loggedin = True
        returnValue(content) 


class TimeAgentPool(object):
    ''' '''

    def __init__(
        self,
        minTimeInterval=10.0,
        maxTimeInterval=15.0,
        loginInterval=60.0,
    ):
        ''' '''
        self.minTimeInterval = minTimeInterval
        self.maxTimeInterval = maxTimeInterval
        self.loginInterval = loginInterval
        self.lastLogin = 0.0
        self.agents = []
        self.idleAgents = []
        self.defers = []

    def initAgent(self, agent):
        ''' '''
        self.agents.append(agent)
        self.idleAgents.append(agent)
        agent.nextAccess = 0
        agent.pool = self

    def addAgent(self, agent):
        ''' '''
        t = random.uniform(
            self.minTimeInterval,
            self.maxTimeInterval,
        )

        agent.nextAccess = time.time() + t
        if self.defers:
            d = self.defers[0]
            del self.defers[0]
            task.deferLater(reactor, t, d.callback, agent)
        else:
            self.idleAgents.append(agent)

    @inlineCallbacks
    def getAgent(self):
        ''' '''
        if not self.agents:
            raise NoAgentError("This pool has no agent yet.")
        if not self.idleAgents:
            d = Deferred()
            self.defers.append(d)
            agent = yield d
        else:
            agent = self.idleAgents[0]
            del self.idleAgents[0]

        now = time.time()
        if now > agent.nextAccess:
            returnValue(agent)
        else:
            yield wait(agent.nextAccess - now)
            returnValue(agent)

    def removeAgent(self, agent):
        ''' '''
        self.agents.remove(agent) 
