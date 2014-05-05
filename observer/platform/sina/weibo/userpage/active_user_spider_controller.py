# coding=utf8
#
# Insight Minr Active Spider Controller Node
# CopyRight BestMiner Inc.
#

'''
    1. 管理关键词
    2. 管理子节点
'''

import os
import time
import json
from datetime import datetime
from twisted.internet import reactor, defer
from heapq import heappop, heappush
from observer import log
from observer.node.controller import ControllerServiceBase
from twisted.internet.defer import inlineCallbacks, returnValue
from collections import deque
from utils import send_messages, check_exists
from config import SEARCH_TIMEOUT


class KeyQueue(object):

    def __init__(self):
        ''' '''
        self.queue = []             # 关键词队列
        self.defers = deque()       # defers队列
        self.nextcall = None        # next call

    def close(self):
        ''' '''
        log.info("Closing KeyQueue...")
        self.canceltimedcall()

    def get_key(self):
        ''' '''
        if self.defers or not self.queue:
            # 如果有其他在等待的请求或者关键字队列为空的时候
            # 直接把请求放入等待队列
            d = defer.Deferred()
            self.defers.append(d)
            return d
        
        now = time.time()
        nexttime = self.queue[0][0]
        if nexttime <= now:
            t, req_info = heappop(self.queue)
            while req_info[0] in abandoned:
                t, req_info = heappop(self.queue)
                
            return defer.succeed((req_info, t))

        d = defer.Deferred()
        self.defers.append(d)
        assert self.nextcall is None, "The nextcall must be none when there is no defers"
        self.nextcall = reactor.callLater((nexttime-now)+1.0, self.delayedcall)
        return ((0, 0, 0, 0, 0), 0)

    def add_key(self, req_info, t):
        ''' '''
        log.info('Gotting keyword %s in add_key' % req_info[0])
        if not self.defers:
            heappush(self.queue, (t, req_info))
            return

        now = time.time()
        if now >= t:
            log.info('Key %s is time execced' % req_info[0])
            defer = self.defers.popleft()
            defer.callback((req_info, t))
            if not self.defers:
                self.canceltimedcall()
            return

        heappush(self.queue, (t, req_info))
        if self.queue:
            nexttime = self.queue[0][0]
            if nexttime >= t:
                if self.nextcall:
                    self.nextcall.reset(t-now)
            else:
                self.nextcall = reactor.callLater((t-now)+1.0, self.delayedcall)

    def canceltimedcall(self):
        ''' '''
        if self.nextcall is not None:
            self.nextcall.cancel()
            self.nextcall = None

    def delayedcall(self):
        ''' '''
        now = time.time()
        assert len(self.queue) > 0, "The queue should not be empty"
        assert len(self.defers) > 0, "The defer queue should not be empty"
        assert now >= self.queue[0][0], "The current time must after the head of queue"
        t, kid = heappop(self.queue)
        d = self.defers.popleft()
        d.callback((kid, t))
        if self.defers and self.queue:
            nexttime = self.queue[0][0]
            self.nextcall = reactor.callLater(abs(nexttime-now), self.delayedcall)
        else:
            self.nextcall = None


class ControllerService(ControllerServiceBase):
    ''' '''

    servicename = 'observer.sina.weibo.user_active_spider'

    def __init__(self, *args, **kwargs):
        ''' '''
        ControllerServiceBase.__init__(self, *args, **kwargs)
        cfg = kwargs['cfg']

        self.dbhost = cfg.userdb_host           # user db host
        self.dbport = cfg.userdb_port           # user db port
        self.dbname = cfg.userdb_dbname         # user db name
        self.collection = cfg.collection_name   # user db collection

        self.db_uids_host = cfg.db_uids_host    # uid db host
        self.db_uids_port = cfg.db_uids_port    # uid db port
        self.db_uids_db = cfg.db_uids_dbname    # uid db name
        self.db_uids_collection = cfg.db_uids_collection # uid db coll

        self.min_priority = cfg.min_priority    # min keyword priority
        self.max_priority = cfg.max_priority    # max keyword priority

        self.clients = {}
        self.uids = []                          # for uids to crawl


    def startService(self):
        ''' '''
        ControllerServiceBase.startService(self)
        self.refresh_uids()

    def refresh_uids(self):
        ''' '''
        conn = pymongo.Connection(self.db_uids_host, self.db_uids_port)
        db = self.conn[self.db_uids_db]
        map(
            lambda x: heapq.heappush(self.uids, x.get('_id')),
            db[self.db_uids_collection].find()
        )
        db.close()

        # reactor.callLater(60.0, self.refresh_uids)

    def getUser(self):
        ''' '''
        r = (None, None, None)
        try:
            r = heapq.heappop(self.users)
        except IndexError:
            pass
        return r

    def stopService(self):
        '''
            stop this service and write data info files
            for the next call
        '''
        pass

    def gotResult(self, data, uid, clientid):
        ''' '''

        log.info("Uid: %s result: %s at %s" % (
            repr(uid),
            str(data),
            str(clientid),
        ))
        if data:
            #kafka_client = generate_kafka_client()
            #kafka_client.send_messages(json.dumps(feeds))
            data.update({'_id': uid})
            udb.sina_users.save(data)

    def clientPull(self, clientid):
        ''' '''
        def _get_uid():
            if not self.uids:
                return None
            else:
                return heapq.heappop(self.uids)

        requestid = self.newRequestId()
        uid  = _get_uid()
        if not uid:
            return(None, None, None, {})
        else:
            result = (
                requestid,
                'search',
                [],
                {
                    'uid': uid,
                    'clientid': clientid,
                    't': time.time(),
                },
            )

            self.processing_queue[self.servicename].append(result)
            return(result)

    def remove_rid(self, requestid):
        #FIXME 
        self._check_mongodb()
        for r in self.processing_queue[self.servicename]:
            rid = r[0]
            if rid == requestid:
                self.processing_queue[self.servicename].remove(r)
            else:
                pass
        self.db[self.db_uids_collection].remove({'_id': r[3].get('uid')})

    def clientPush(self, requestid, name, *args, **kwargs):
        ''' '''
        servicename, method = self.splitName(name)

        if name not in self.versions:
            raise UnknownService("Unknown Service " + servicename)

        self.gotResult(
            kwargs.get('data', None),
            kwargs.get('uid', 0),
            kwargs.get('clientid'),
        )
        self.remove_rid(requestid)

    def clientFail(self, requestid, name, *args, **kwargs):
        ''' '''

        servicename, method = self.splitName(name)
        if name not in self.versions:
            raise UnknownService("Unknown Service " + servicename)

        log.info("%s Client Failed. Keyword: %s. reason: %s" % (
            kwargs.get('clientid'),
            repr(kwargs.get('keyword')),
            kwargs.get('reason', ''),
        ))
        self.remove_rid(requestid)
