# coding=utf8

import time
import random
from cStringIO import StringIO

from twisted.internet import reactor, task
from twisted.internet.defer import (
    inlineCallbacks,
    returnValue,
    Deferred,
    succeed,
)
from twisted.internet.protocol import Protocol

from twisted.names.client import Resolver
from twisted.web import client
from twisted.web.http_headers import Headers
from twisted.web.client import (
    HTTPPageGetter,
    HTTPClientFactory,
    _makeGetterFactory,
)

from observer.utils.errors import InfiniteLoginError, NoAgentError
from observer.utils import wait
from observer.utils import log


class HTTPRESTGetter(HTTPPageGetter):
    def handleStatus_304(self):
        pass


class HTTPRESTClientFactory(HTTPClientFactory):
    protocol = HTTPRESTGetter


def getPage(url, contextFactory=None, *args, **kwargs):
    return _makeGetterFactory(
        url,
        HTTPRESTClientFactory,
        contextFactory=contextFactory,
        *args, **kwargs).deferred


class HTTPBodyReceiver(Protocol):

    def __init__(self, deferred):
        self.deferred = deferred
        self.body = StringIO()
        self.reason = None

    def dataReceived(self, data):
        self.body.write(data)

    def connectionLost(self, reason):
        body = self.body.getvalue()
        self.reason = reason
        self.deferred.callback(body)


def _cbRequest(response):
    finished = Deferred()
    response.deliverBody(HTTPBodyReceiver(finished))
    return finished


def request(agent, url, headers=None, body=None):
    log.debug('begin request ' + url)
    print agent, agent.request
    if body is None:
        d = agent.request('GET', str(url), headers)
    else:
        d = agent.request('POST', str(url), headers,
                          client.FileBodyProducer(StringIO(body)))
    d.addCallback(_cbRequest)
    return d


class AdditionalHeaderAgent(object):
    """
    An L{Agent} wrapper to add default headers.

    @param headers: A list or tuple of (name, value) objects. The name
        is which of the HTTP header to set the value for. and the value
        is which to set for the named header.

    """
    def __init__(self, agent, headers):
        self._header_list = headers
        self._agent = agent

    def request(self, method, uri, headers=None, bodyProducer=None):
        if headers is None:
            headers = Headers()
        else:
            headers = headers.copy()
        for name, value in self._header_list:
            headers.addRawHeader(name, value)
        return self._agent.request(method, uri, headers, bodyProducer)


class LoginResponse(object):

    def __init__(self, response, body, reason):
        self.original = response
        self.data = body
        self.reason = reason

    def __getattr__(self, name):
        return getattr(self.original, name)

    def deliverBody(self, protocol):
        protocol.dataReceived(self.data)
        protocol.connectionLost(self.reason)


class LoginAgent(object):

    def __init__(self, agent, retryLimit=1):
        self._agent = agent
        self.loggedin = False
        self.retryLimit = retryLimit

    def login(self):
        raise NotImplementedError("Must Implement the login method.")

    def testLogin(self, content):
        raise NotImplementedError("Must Implement the test login method.")

    @inlineCallbacks
    def request(self, method, uri, headers=None, bodyProducer=None):
        retryCount = 0
        while True:
            if not self.loggedin:
                yield self.login()
                retryCount += 1
            response = yield self._agent.request(method, uri,
                                                 headers, bodyProducer)
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


class TimedAgentPool(object):

    #FIXME here the float value should be replaced by variables
    def __init__(
        self,
        minTimeInterval=10.0,
        maxTimeInterval=15.0,
        loginInterval=60.0,
    ):
        self.minTimeInterval = minTimeInterval
        self.maxTimeInterval = maxTimeInterval
        self.loginInterval = loginInterval
        self.lastLogin = 0.0
        self.agents = []
        self.idleAgents = []
        self.defers = []

    def initAgent(self, agent):
        self.agents.append(agent)
        self.idleAgents.append(agent)
        agent.nextAccess = 0
        agent.pool = self

    def addAgent(self, agent):
        t = random.uniform(self.minTimeInterval,
                           self.maxTimeInterval)
        agent.nextAccess = time.time() + t
        if self.defers:
            d = self.defers[0]
            del self.defers[0]
            task.deferLater(reactor, t, d.callback, agent)
        else:
            self.idleAgents.append(agent)

    @inlineCallbacks
    def getAgent(self):
        if not self.agents:
            raise NoAgentError('This pool has no agent yet.')
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
        self.agents.remove(agent)


class DNSCachingResolver(Resolver):
    """
    subclass Resolver to add dns caching mechanism
    """

    clear_interval = 1 * 60 * 60

    def __init__(self, *args, **kwargs):
        Resolver.__init__(self, *args, **kwargs)
        self.clear_cache()

    def clear_cache(self):
        self.cached_url = {}
        reactor.callLater(self.clear_interval, self.clear_cache)

    def update_cache(self, result, name):
        self.cached_url[name] = result
        return result

    def getHostByName(self, name, timeout=None, effort=10):
        """
        @see: twisted.names.client.getHostByName
        """
        # XXX - respect timeout
        if name in self.cached_url:
            return succeed(self.cached_url[name])
        else:
            return self.lookupAllRecords(
                name,
                timeout,
            ).addCallback(
                self._cbRecords,
                name,
                effort,
            ).addCallback(self.update_cache, name)
