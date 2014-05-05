# coding=utf8

'''
    controller 节点
    controller 节点负责字节点注册，数据任务分发
'''
import socket
import time

from uuid import uuid4
from collections import deque

from twisted.spread import pb
from twisted.internet.defer import (
    inlineCallbacks,
    returnValue,
    Deferred,
    succeed,
)
from twisted.internet import reactor
from twisted.application import service
from twisted.python import failure

import twisted.spread.banana

from observer.node import BANANA_SIZE_LIMIT
from observer.utils import log
from observer.node.errors import UnknownService
from observer.utils.twisted_utils import ReconnectingPBClientFactory

twisted.spread.banana.SIZE_LIMIT = BANANA_SIZE_LIMIT

MAX_DELAYED = 60        # max delay of a pb client


class PBClientFactory(ReconnectingPBClientFactory):
    ''' '''

    def __init__(self, rootCallback):
        ReconnectingPBClientFactory.__init__(self)
        self._rootCallback = rootCallback
        self.maxDelay = MAX_DELAYED

    def gotRootObject(self, root):
        ''' '''
        self._rootCallback(root)


class ControllerServiceBase(service.Service):
    ''' controlelr node. '''

    servicename = ''
    version_major = 1
    version_minor = 0

    def __init__(self, cfg):
        ''' '''
        # not used now
        self.client_register_timeout = cfg.client_register_timeout
        self.port = cfg.controller_port

        self.clients = {}
        self.client_timeout = {}
        self.versions = cfg.versions    # {'servicename': version_tuple}
        self.client_request_timeout = cfg.client_request_timeout
        self.push_id = 0
        self.pull_id = 0
        self.push_requests = {}
        self.push_queue = {}

        self.controller = None
        self.pull_requests = {}
        self.pull_queue = {}
        self.processing_timeout = {}

        self.task_queue = {}
        self.processing_queue = {}

        self.requestid = 1

    def splitName(self, name):
        ''' split the given task name '''
        names = name.split('.')
        return ('observer.' + '.'.join(names[:-1]), names[-1])

    def getService(self, name):
        ''' get the target service by given service name '''
        return self.services.get(name, set())

    def newClientId(self):
        ''' generate a new uuid4().int value '''
        return uuid4().int

    def newRequestId(self):
        ''' generate a new request id '''
        reqid = self.requestid
        self.requestid += 1
        return reqid

    def clientRegisterTimeout(self, clientid):
        ''' '''
        del self.client_timeout[clientid]

    def clientProcTimeout(self, requestid, clientid):
        ''' '''
        try:
            del self.processing_timeout[requestid]
            servicename = self.push_requests[requestid]['servicename']
        except KeyError: # already processed
            return

        self.clients[clientid]['processing'].discard(requestid)
        self.addRequest(servicename, requestid)

    def getController(self):
        ''' '''
        if self.controller is not None:
            return succeed(self.controller)
        else:
            d = Deferred()
            return d

    def startService(self):
        ''' start the controller as a service '''
        for servicename in self.versions:
            self.task_queue[servicename] = []           # record tasks to crawl
            self.processing_queue[servicename] = []     # record which client ops

    def stopService(self):
        ''' stop the controller '''
        for timeout in self.client_timeout.values():
            timeout.cancel()

    def register(
        self,
        servicename,
        version_major,
        version_minor,
        nodename,
        client,
    ):
        ''' register the client to controller '''
        clientid = self.newClientId()

        # 如果给定的servicename不在versions中，表示该节点是无效节点
        if servicename not in self.versions:
            log.info("Added client: %s %s Failed. No such servicename" % (str(clientid), servicename))
            return ('%s is not in a known service' % servicename, 0)
    
        version = self.versions[servicename]
        client_version = (version_major, version_minor)

        if client_version < version[0]:
            return ('version %s is below %s please update the client' % (
                        repr(client_version),
                        repr(version[0])),
                    0)
        if client_version > version[1]:
            return ('version %s is above %s please update the controller' % (
                        repr(client_version),
                        repr(version[1])),
                    0)

        self.clients[clientid] = {
            'client': client,
            'id': clientid,
            'servicename': servicename,
            'name': nodename,
            'processing': {},
            'ip': client.broker.transport.getPeer().host,
            'last_call': time.time(),
        }

        # called when disconnect
        client.notifyOnDisconnect(lambda c: self.unregister(clientid))
        log.info("Added client: %s %s" % (str(clientid), servicename))
        return ('succeed', clientid)

    def unregister(self, clientid):
        ''' unregister the given clientid '''
        if clientid not in self.clients:
            return False

        del self.clients[clientid]
        log.info("Removed client: " + str(clientid))

        return True

    def addRequest(self, servicename, requestid):
        ''' add a new request '''

        pull_queue = self.pull_queue.get(servicename, deque())
        if pull_queue:
            # if pull_queue exists. pop a record from pull_queue
            # remove the current record in client[pulling] and push
            # it into client[processing].
            # get a defer from pull_request and get a request from 
            # push_requests by requestid, finally, active the *request*
            pull_id = pull_queue.popleft()
            pull_request = self.pull_requests[pull_id]
            client = self.clients[pull_request['clientid']]
            client['pulling'].discard(pull_id)
            client['processing'].add(requestid)
            pull_defer = pull_request['defer']
            request = self.push_requests[requestid]
            pull_defer.callback(request)
        else:
            # add a push record into push_queue 
            self.push_queue[servicename].append(requestid)

    @inlineCallbacks
    def remoteRequest(self, name, *args, **kwargs):
        ''' '''
        servicename, method = self.splitName(name)
        if servicename not in self.versions:
            raise UnknownService("Unknown Service: " + servicename)

        requestid = self.newRequestId()
        defer = Deferred()
        request = {
            'id': requestid,
            'servicename': servicename,
            #'clientid': None,
            'method': method,
            'args': args,
            'kwargs': kwargs,
            'attemps': 0,
            #'defer': defer,
        }

        self.push_requests[requestid] = request
        self.addRequest(servicename, requestid)

        try:
            result = yield defer
            returnValue(result)
            return
        except pb.RemoteError, error:
            log.error("Got Error: " + error.remoteType)
            raise

    def gotResult(self, *args, **kwargs):
        ''' '''
        raise NotImplementedError("Should Implemented in subclass")

    @inlineCallbacks
    def clientPush(self, clientid, name, *args, **kwargs):
        ''' push a record into client[pushing] '''

        # if the given method name does not exists in self.versions,
        # raise an UnknownServiceError
        client = self.clients[clientid]
        servicename, method = self.splitName(name)

        if name not in self.versions:
            raise UnknownService("Unknown Service " + servicename)

        # generate a new requestId
        requestid = self.newRequestId()
        defer = Deferred()
        defer.addCallback(self.gotResult, args, kwargs)

        request = {
            'id': requestid,
            'servicename': servicename,
            'clientid': clientid,
            'method': method,
            'arg': args,
            'kwargs': kwargs,
            'attemps': 0,
            'defer': defer,
        }

        client['pushing'].add(requestid)
        self.push_requests[requestid] = request

        try:
            result = {'status': 'success', 'message': ''}
            returnValue(result)
        except pb.RemoteError, error:
            log.error("Got error: " + error.remoteType)
            result = {'status': 'failure', 'message': error.remoteType}
            returnValue(result)
            raise
        finally:
            client['pushing'].discard(requestid)

    @inlineCallbacks
    def clientPull(self, clientid):
        ''' '''
        client = self.clients[clientid]
        push_queue = self.push_queue[name]

        # if there are no push_queue
        if not push_queue:
            # generate a new pull record and add into pull_queue
            defer = Deferred()
            pullid = self.newRequestId()
            pull = {
                'id': pullid,
                'servicename': name,
                'defer': defer,
                'clientid': clientid,
            }

            client['pulling'].add(pullid)
            self.pull_requests[pullid] = pull
            self.pull_queue[name].append(pullid)
            request = yield defer

            del self.pull_requests[pullid]
            requestid = request['id']
            client['processing'].add(requestid)
        else:
            # get a request from push_queue and add into processing queue
            requestid = push_queue.popleft()
            client['processing'].add(requestid)
            request = self.push_requests[requestid]

        self.processing_timeout[requestid] = reactor.callLater(
            self.client_request_timeout,
            self.clientProcTimeout,
            requestid,
            clientid,
        )
        log.info("Sent To: clientid %s, requestid %s." % (
            clientid,
            request['id'],
        ))
        # return requestid, method, args, kwargs to client and
        # client run it.
        returnValue((
            request['id'],
            request['method'],
            request['args'],
            request['kwargs'],
        ))

    def clientReturn(self, clientid, requestid, result):
        ''' '''
        log.info("Returned: clientid: %s, requestid: %s" % (
            clientid,
            requestid,
        ))

        # remove this request from processing deque
        client = self.clients[clientid]
        client['processing'].discard(requestid)

        # try to cancel the processing request.
        # if occured an exception, that means the request
        # was already finishd.
        try:
            self.processing_timeout[requestid].cancel()
            del self.processing_timeout[requestid]
        except KeyError: # 已经处理完成
            pass

        if requestid in self.push_requests:
            push = self.push_requests[requestid]
            if 'error' not in result:
                push['defer'].callback(result['result'])
            else:
                error = result['error']
                push['defer'].errback(failure.Failure(
                    pb.RemoteError(
                        error['type'], 
                        error['value'],
                        error['traceback'],
                )))

            servicename = push['servicename']

            # remove this request from push_queue
            try:
                self.push_queue[servicename].remove(requestid)
            except:
                pass

            if push['clientid'] is not None:
                try:
                    self.clients[push['clientid']]['pushing'].discard(requestid)
                except:
                    pass


class ControllerChildNode(pb.Referenceable):
    ''' '''

    def __init__(self, service):
        self.service = service

    def remote_preregister(self, clientid):
        ''' '''
        return self.service.preregister(clientid)


class ControllerNode(pb.Root):
    ''' start the controller node as service  '''

    def __init__(self, service):
        ''' '''
        self.service = service
        service.node = self

    #def request(self, name, *args, **kwargs):
    #    ''' '''
    #    return self.service.remoteRequest(name, *args, **kwargs)

    #def remote_request(self, clientid, name, *args, **kwargs):
    #    ''' '''
    #    return self.service.clientPush(clientid, name, *args, **kwargs)

    #def remote_pull(self, clientid):
    #    ''' '''
    #    return self.service.clientPull(clientid)

    #def remote_return(self, clientid, requestid, result):
    #    ''' '''
    #    return self.service.clientReturn(clientid, requestid, result)

    #def remote_push(self, requestid, name, *args, **kwargs):
    #    ''' '''
    #    return self.service.clientPush(requestid, name, *args, **kwargs)


    def remote_register(
        self,
        service,
        version_major,
        version_minor,
        nodename,
        client,
    ):
        ''' '''
        return self.service.register(
            service,
            version_major,
            version_minor,
            nodename,
            client,
        )

    def remote_unregister(self, clientid):
        ''' '''
        return self.service.unregister(clientid)

    def remote_nextRequest(self):
        ''' '''
        return self.service.nextRequest()

    def remote_fail(self, name, *args, **kwargs):
        ''' '''
        return self.service.clientFail(name, *args, **kwargs)

    def remote_sendResult(self, requestid, skid, result):
        ''' '''
        return self.service.sendResult(requestid, skid, result)
