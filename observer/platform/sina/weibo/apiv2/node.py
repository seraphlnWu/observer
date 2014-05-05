# coding=utf8

import time
from twisted.spread import pb
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred

from observer.node.client import ClientServiceBase
from observer.platform.sina.weibo.apiv2.api import API
from observer.platform.sina.weibo.apiv2 import error

from observer.platform.sina.weibo.apiv2.config import MAX_COUNT


AN_HOUR = 3600  # seconds


class NodeService(ClientServiceBase):
    ''' '''

    servicename = 'observer.sina.weibo.apiv2'

    def __init__(self, *args, **kwargs):
        ''' '''
        ClientServiceBase.__init__(self, *args, **kwargs)
        self.max_count = MAX_COUNT,
        self.count = MAX_COUNT  # 每小时最大请求次数
        self.last_call = 0
        self.users = []
        self.user_cursor = 0
        self.api = None
        self.ready = False

    def clear_count(self):
        now = time.time()
        if int(now / AN_HOUR) > int(self.last_call / AN_HOUR):
            self.count = self.max_count
            self.last_clear = now
        i = 0
        for defer in self.deferred_call:
            defer.callback(None)
            if self.count <= 0:
                break
            i += 1
        del self.deferred_call[:i]
        now = time.time()
        reactor.callLater(3601.0 - (now % 3600), self.clear_count)

    @inlineCallbacks
    def startService(self):
        ''' '''
        yield ClientServiceBase.startService(self)
        self.api = API()
        self.deferred_call = []
        self.clear_count()
        #yield self.refresh_users()

    @inlineCallbacks
    def refresh_users(self):
        try:
            self.users = ClientServiceBase.controllerUsers(self)
            self.user_cursor = self.user_cursor % len(self.users)
            if not self.ready:
                self.ready = True
                yield self.updateStatus()
        except pb.RemoteError:
            pass
        reactor.callLater(60.0, self.refresh_users)

    def __getattr__(self, name):
        def newproc(*args, **kwargs):
            if not self.api.is_unmetered(name):
                if self.count <= 0:
                    d = Deferred()
                    d.addCallback(lambda _:
                                      self.get_data(name, *args, **kwargs))
                    self.deferred_call.append(d)
                    return d
            return self.get_data(name, *args, **kwargs)

        if self.api.provides(name):
            return newproc
        raise AttributeError(
            "'%s' has no attribute '%s'" % (self.__class__.__name__, name))

    @inlineCallbacks
    def get_data(self, name, *args, **kwargs):
        if self.api.provides(name):
            has_access_token = 'access_token' in kwargs
            req_args, req_kwargs = args, kwargs.copy()
            if self.api.need_probe(name) and 'access_token' not in kwargs:
                user_info = self.users[self.user_cursor % len(self.users)]
                self.user_cursor += 1
                if self.user_cursor >= len(self.users):
                    self.user_cursor = self.user_cursor % len(self.users)
                req_kwargs = kwargs.copy()
                req_kwargs['access_token'] = user_info['token']
                req_kwargs['del_access_token'] = True
            if not self.api.is_unmetered(name):
                self.count -= 1
            try:
                result = yield getattr(self.api, name)(*req_args, **req_kwargs)
                returnValue((result, self.count))
            except error.SinaServerError, err:
                if has_access_token and err.code in error.TOKEN_ERRORS:
                    raise error.getErrorFromCode(err.code, None, err.method,
                                                 has_access_token)
                else:
                    raise
        else:
            raise NotImplementedError
