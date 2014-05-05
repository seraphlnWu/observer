# coding=utf8

'''
    节点基类

    包含向主节点注册节点，请求任务等操作
'''

import twisted.spread.banana
from twisted.spread import pb
from twisted.internet.defer import (
    inlineCallbacks,
    returnValue,
    Deferred,
    succeed,
)

from twisted.internet import reactor
from twisted.application import service

from observer.utils import wait
from observer import log
from observer.utils import TLSClientContextFactory  #FIXME not used now!
from observer.utils.twisted_utils import ReconnectingPBClientFactory
from observer.node import BANANA_SIZE_LIMIT


twisted.spread.banana.SIZE_LIMIT = BANANA_SIZE_LIMIT


class PBClientFactory(ReconnectingPBClientFactory):
    ''' '''

    def __init__(self, rootCallback):
        ''' '''
        ReconnectingPBClientFactory.__init__(self)
        self._rootCallback = rootCallback
        self.maxDelay = 60

    def gotRootObject(self, root):  # called by connect
        ''' '''
        self._rootCallback(root)


class ClientServiceBase(service.Service):
    '''
        1. 向controller节点注册
    '''

    # controller_name = 'observer.sina.weibo.active_spider'
    controller_name = ''
    servicename = None
    version_major = -1
    version_minor = -1

    def __init__(self, *args, **kwargs):
        ''' '''
        cfg = kwargs['cfg']
        self.cfg = cfg

        self.node = None

        # host name and port
        self.host = cfg.host
        self.port = cfg.port

        # base attribute of client object
        self.clientid = None
        self.controller = None
        self.controllerTimer = None
        self.controller_factory = None
        self.ready = False
        self._requests = []
        self.max_slots = cfg.max_slots


    def cancelControllerTimeout(self):
        ''' '''
        if self.controllerTimer is not None:
            self.controllerTimer.cancel()
            self.controllerTimer = None

    def _clearLocal(self):
        ''' '''
        self.central_node = None
        self.clientid = None
        self.controller = None

    @inlineCallbacks
    def registerController(self, controllernode):
        '''
            register to controller. 

            first, call *register* in controller,
            then:
            if the procedure was running successfully,
                we will return the message in data.
            if there happened a exception,
                we will return a None.
        '''
        message = None
        try:
            data = yield controllernode.callRemote(
                'register',
                self.servicename,
                self.version_major,
                self.version_minor,
                self.host_name,
                self.node,
            )
            message, clientid = data
            self.clientid = clientid
            self.controller = controllernode
            self.ready = True
        except:
            log.error("Error when register to controller.")
            log.exception()
            self._clearController()

        returnValue(message)

    def _clearController(self):
        ''' '''
        self.clientid = None
        self.controller = None

    @inlineCallbacks
    def unregister(self):
        ''' '''
        try:
            yield self.controller.callRemote('unregister', self.clientid)
        except:
            pass
        self._clearController()

    @inlineCallbacks
    def callController(self, methodname, *args, **kwargs):
        ''' '''
        while True:
            controller = self.getController()
            try:
                result = yield controller.result.callRemote(
                    methodname,
                    *args,
                    **kwargs
                )
                returnValue(result)
            except pb.DeadReferenceError:
                self._clearController()
                continue
            except pb.PBConnectionLost:
                self._clearController()
                continue
            except Exception as msg:
                pass

    def controllerRequest(self, methodname, *args, **kwargs):
        ''' '''
        return self.callController(
            'request',
            self.clientid,
            methodname,
            *args,
            **kwargs
        )

    def controllerPull(self):
        ''' '''
        return self.callController('pull', self.clientid)

    def controllerFail(self, methodname, *args, **kwargs):
        return self.callController(methodname, *args, **kwargs)

    def controllerPush(self, methodname, *args, **kwargs):
        ''' '''
        return self.callController(methodname, *args, **kwargs)

    def controllerReturn(self, requestid, result):
        ''' '''
        return self.callController('return', self.clientid, requestid, result)

    @inlineCallbacks
    def gotController(self, root):
        ''' register a client to controller '''

        # first clear local environment
        # then register to controller
        self._clearController()
        message = yield self.registerController(root)

        # if the message is succeed, pop a request record, and active it.
        # else, call disconnect to the controller.
        if message == 'succeed':
            while self._requests:
                d = self._requests.pop(0)
                d.callback(root)
        else:
            log.error("Got error when register to controller " + str(message))
            self.controller_factory.disconnect()

    def getController(self):
        '''
            if there is a controller, return it.
            else: return a new Deferred instance.
        '''
        if self.controller is not None:
            return succeed(self.controller)
        else:
            d = Deferred()
            self._requests.append(d)
            return d

    @inlineCallbacks
    def connectController(self):
        ''' connect to controller node '''
        self.central_factory = PBClientFactory(self.gotController)
        reactor.connectTCP(
            self.host,
            self.port,
            self.central_factory,
        )

        root = yield self.getController()
        returnValue(root)

    @inlineCallbacks
    def startService(self):
        ''' '''
        root = yield self.connectController()
        self.controller = root


    @inlineCallbacks
    def stopService(self):
        ''' '''
        if self.controller is not None:
            yield self.unregister()


class Client(pb.Referenceable):
    ''' '''

    def __init__(self, service):
        ''' '''
        self.service = service
        service.node = self

    def __getattr__(self, name):
        ''' '''
        if name.startswith('remote_'):
            return getattr(self.service, name)
        raise AttributeError(
            "'%s' has no attribute '%s'" % (self.__class__.__name__, name)
        )
