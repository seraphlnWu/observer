# coding=utf8

import heapq
import time
import pymongo
from twisted.internet.defer import (
    inlineCallbacks,
    Deferred,
    succeed,
    returnValue,
)

from twisted.spread import pb
from twisted.internet import reactor
from observer.node.controller import ControllerServiceBase
from observer import log
from observer.utils import wait


class ControllerService(ControllerServiceBase):
    ''' '''

    servicename = 'observer.sina.weibo.apiv2'

    def __init__(self, *args, **kwargs):
        ''' '''
        ControllerServiceBase.__init__(self, *args, **kwargs)
        cfg = kwargs['cfg']
        self.dbhost = cfg.userdb_host
        self.dbport = cfg.userdb_port
        self.dbname = cfg.userdb_dbname
        self.dbprobecollection = cfg.userdb_idcollection
        self.dbcollection = cfg.userdb_collection

        self.clients = {}
        self.objects = {}
        self.max_attemps = 3
        self.nextClientId = 1
        self.attemp_interval = 5.0

        #
        self.count = 1000
        self.dbconn = None
        self._users = None
        self._ids = None
        self.users = []

    @staticmethod
    def splitRequestName(name):
        ''' '''
        return name.split('.')

    def getInsertObject(self, name):
        names = self.splitRequestName(name)
        d = self.objects
        for objname in names[:-1]:
            if objname in d:
                d = d[objname]
            else:
                dd = {}
                d[objname] = dd
                d = dd
            if 'children' in d:
                d = d['children']
            else:
                children = {}
                d['children'] = children
                d = children

        objname = names[-1]
        if objname in d:
            d = d[objname]
        else:
            dd = {}
            d[objname] = dd
            d = dd
        if 'clients' in d:
            return d['clients']
        else:
            clients = {'deactivated': {}, 'idle': [], 'defer': []}
            d['clients'] = clients
            return clients

    def findIdle(self, clients, clientid):
        ''' '''
        for i, (c, cid) in enumerate(clients['idle']):
            if cid == clientid:
                return i, c
        return None, None

    def removeIdle(self, clients, clientid):
        ''' '''
        todel, count = self.findIdle(clients, clientid)
        if todel is not None:
            del clients['idle'][todel]
            heapq.heapify(clients['idle'])
            return count
        return None

    def addDeactivated(self, clients, clientid, count):
        ''' '''
        clients['deactivated'][clientid] = count

    def addIdle(self, clients, clientid, count):
        ''' '''
        heapq.heappush(clients['idle'], (-count, clientid))
        if clients['defer']:
            d = clients['defer'].pop(0)
            clientinfo = heapq.heappop(clients['idle'])
            d.callback(clientinfo)

    @inlineCallbacks
    def startService(self):
        yield ControllerServiceBase.startService(self)
        self.refresh_users()

    def refresh_users(self):
        ''' refresh users and their token '''
        self.dbconn = pymongo.Connection(host=self.dbhost, port=self.dbport)
        self._users = self.dbconn[self.dbname][self.dbcollection]
        self._ids = self.dbconn[self.dbname][self.dbprobecollection]

        ids = self._ids.find({}, {'probe_uid': 1})
        idset = set(el.get('probe_uid') for el in ids)
        users = self._users.find({}, {'tok': 1, 'exp': 1})
        self.users = []
        test_timestamp = int(time.time()) + 3600
        for user in users:
            if user['_id'] in idset and user.get('tok', 0):
                exp = user.get('exp', 0)
                if exp > test_timestamp:
                    self.users.append({
                        'token': user['tok'],
                        'exp': user['exp'],
                    })
        self.dbconn.close()
        reactor.callLater(60.0, self.refresh_users)

    def getUsers(self, client_id):
        ''' '''
        return (self.users, self.api_user_limit)

    @inlineCallbacks
    def stopService(self):
        ''' '''
        try:
            self.dbconn.close()
        except Exception as msg:
            print str(msg)

        self.dbconn = None
        self._users = None
        self.users = None
        yield ControllerServiceBase.stopService(self)

    def getRemoteMethod(self, name):
        ''' '''
        names = name.split('.')
        methodname = names[-1]
        clients = self.getObject(names[:-1])
        if clients['idle']:
            clientinfo = heapq.heappop(clients['idle'])
            return succeed((clientinfo, methodname))
        else:
            d = Deferred()
            d.addCallback(lambda r: (r, methodname))
            clients['defer'].append(d)
            return d

    def getObject(self, names):
        ''' '''
        d = self.objects
        try:
            for objname in names[:-1]:
                d = d[objname]['children']
            return d[names[-1]]['clients']
        except KeyError:
            log.error("There is no client with name " + '.'.join(names))
            raise

    @inlineCallbacks
    def remoteRequestV2(self, name, *args, **kwargs):
        ''' '''
        attemps = 0
        while True:
            clientinfo, method = yield self.getRemoteMethod(name)
            client, clients = self.clients[clientinfo[1]]
            try:
                result, count = yield client.callRemote(method, *args, **kwargs) 
                self.addIdle(clients, clientinfo[1], count)
                break
            except pb.RemoteError as error:
                error_type = error.remoteType
                if not error_type.endswith("NoRetry"):
                    log.info("Got retriable exception: " + error_type)
                    log.exception()
                    self.addIdle(clients, clientinfo[1], -clientinfo[0]+1)
                    if attemps > self.max_attemps:
                        log.info("Limit reached")
                        raise
                    attemps += 1
                    yield wait(self.attemp_interval)
                else:
                    log.info("Got not retriable exception: " + error_type)
                    log.exception()
                    self.addIdle(clients, clientinfo[1], -clientinfo[0]+1)
                    raise
            except pb.DeadReferenceError:
                self.unregister(clientinfo[1])
            except pb.PBConnectionLost:
                self.unregister(clientinfo[1])
            except:
                self.addIdle(clients, clientinfo[1], -clientinfo[0])
                raise
        returnValue(result)
