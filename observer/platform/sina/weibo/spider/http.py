# coding=utf8
from cStringIO import StringIO

from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol
from twisted.web import client

from observer import log


class HTTPBodyReceiver(Protocol):
    ''' '''

    def __init__(self, deferred):
        ''' '''
        self.deferred = deferred
        self.body = StringIO()
        self.reason = None

    def dataReceived(self, data):
        ''' '''
        self.body.write(data)

    def connectionLost(self, reason):
        ''' '''
        body = self.body.getvalue()
        self.reason = reason
        self.deferred.callback(body)


def _cbRequest(response):
    ''' '''
    finished = Deferred()
    response.deliverBody(HTTPBodyReceiver(finished))
    return finished


def request(agent, url, headers=None, body=None):
    ''' '''
    log.debug('Begin request ' + url)
    if body is None:
        d = agent.request('GET', str(url), headers)
    else:
        d = agent.request(
            'POST',
            str(url),
            headers,
            client.FileBodyProducer(StringIO(body)),
        )

    d.addCallback(_cbRequest)
    return d
