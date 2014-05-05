# coding=utf8

import time
from observer.node.client import ClientServiceBase
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
import pymongo.connection


class NodeService(ClientServiceBase):

    service_name = 'sina.weibo'

    def __init__(self, *args, **kwargs):
        ClientServiceBase.__init__(self, *args, **kwargs)
        self.count = 4000
        self.dbconn = None
        self._users = None
        cfg = kwargs['cfg']
        self.dbhost = cfg.userdb_host
        self.dbport = cfg.userdb_port
        self.dbname = cfg.userdb_dbname
        self.dbprobecollection = cfg.userdb_idcollection
        self.dbcollection = cfg.userdb_collection
        self._ids = None
        self.users = []

    @inlineCallbacks
    def startService(self):
        yield ClientServiceBase.startService(self)
        self.dbconn = pymongo.connection.Connection(
            host = self.dbhost,
            port = self.dbport)
        self._users = self.dbconn[self.dbname][self.dbcollection]
        self._ids = self.dbconn[self.dbname][self.dbprobecollection]
        self.refresh_users()

    def refresh_users(self):
        ids = self._ids.find({}, {'probe_uid': 1})
        idset = set(el.get('probe_uid') for el in ids)
        users = self._users.find({}, {'tok': 1, 'exp': 1})
        self.users = []
        test_timestamp = int(time.time()) + 3600
        for user in users:
            if user['_id'] in idset and user.get('tok', 0):
                exp = user.get('exp', 0)
                if exp > test_timestamp:
                    self.users.append({'token': user['tok'], 'exp': user['exp']})
        reactor.callLater(60.0, self.refresh_users)

    @inlineCallbacks
    def stopService(self):
        self.dbconn.close()
        self.dbconn = None
        self._users = None
        self.users = None
        yield ClientServiceBase.stopService(self)

    def getUsers(self):
        return self.users, self.count
