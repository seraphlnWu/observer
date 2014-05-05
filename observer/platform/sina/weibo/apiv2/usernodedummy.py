# -*- coding: utf-8 -*-
import time
import datetime
import data_fetcher.constants
from data_fetcher.node.client import ClientServiceBase
from data_fetcher.platform.sina.weibo.apiv2.api import API
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, succeed, Deferred
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
        self.dbcollection = cfg.userdb_collection
        self.users = [{'token': '1234566', 'exp': 1033430}]

    @inlineCallbacks
    def startService(self):
        yield ClientServiceBase.startService(self)
        self.dbconn = pymongo.connection.Connection(
            host = self.dbhost,
            port = self.dbport)
        self._users = self.dbconn[self.dbname][self.dbcollection]
        self.refresh_users()

    def refresh_users(self):
        #users = self._users.find({}, {'tok': 1, 'exp': 1})
        #self.users = []
        #for user in users:
        #    if user.get('tok', 0):
        #        self.users.append({'token': user['tok'], 'exp': user['exp']})
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
