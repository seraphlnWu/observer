# coding=utf8
#
# Insight Minr Active Spider Controller Node
# CopyRight BestMiner Inc.
#

'''
    manage the tasks
    manage the nodes
'''

import os
import time
import json
from heapq import heappop, heappush
from collections import deque

from datetime import datetime
from twisted.internet import reactor, defer
from twisted.internet.defer import inlineCallbacks, returnValue

from observer.utils import log
from observer.utils.tools import send_messages, check_exists, dump_to_redis
from observer.node.controller import ControllerServiceBase


class KeyQueue(object):
    ''' '''

    def __init__(self):
        ''' '''
        self.queue = []             # task queue
        self.defers = deque()       # defers queue
        self.nextcall = None        # next call

    def close(self):
        ''' '''
        log.info("Closing KeyQueue at %s" % repr(datetime.now()))
        self.canceltimedcall()

    def get_key(self):
        ''' '''
        if self.defers or not self.queue:
            # if there is no other request or the queue is empty
            # put the request to defers
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
        self.nextcall = reactor.callLater(nexttime-now, self.delayedcall)
        #FIXME the req_info object
        return ((0, 0, 0, 0, 0), 0)

    def add_key(self, req_info, t):
        ''' '''
        log.info("Gotting keyword %s in add_key" % req_info[0])
        if not self.defers:
            heappush(self.queue, (t, req_info))
            return

        now = time.time()
        if now >= t:
            log.info("Key %s is time execced" % req_info[0])
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
                self.nextcall = reactor.callLater(t-now, self.delayedcall)

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

    def __init__(self, *args, **kwargs):
        ''' '''
        ControllerServiceBase.__init__(self, *args, **kwargs)
        cfg = kwargs['cfg']

        self.feed_id_file = cfg.feed_id_file
        self.keyword_freq_file = cfg.keyword_freq_file

        # common configurations
        self.min_priority = cfg.min_priority    # minum priority
        self.max_priority = cfg.max_priority    # maxum priority

        self.key_queue = KeyQueue()             # keywords list
        self.keywords_freq = {}                 # keywords freq dict
        self.search_defers = {}                 # search defers
        self.m_keywords = {}                    # record last task
        self.update_dict = {}
        self.last_update = time.time()

    def startService(self):
        '''
        connect to controller
        prepare task list
        check node status
        '''
        ControllerServiceBase.startService(self)

        # restore tasks
        self.restore_tasks()

        # load tasks
        self.load_tasks()

    def restore_tasks(self):
        ''' restore tasks from backup files '''
        if os.path.exists(self.keyword_freq_file):
            ''' '''
            with open(self.keyword_freq_file, 'rb') as ifs:
                self.keywords_freq = json.loads(ifs.read())

            now = time.time()

            #FIXME

    def load_tasks(self):
        ''' load tasks from mongodb '''
        now = time.time()
        conn = pymongo.Connection(self.cfg.mongo_host, self.cfg.mongo_port)
        db = conn[self.cfg.mongo_dbname]

        for l in db[self.cfg.task_collection].find():
            kid = keyword.get('_id')
            priority = keyword.get('priority', 1)
            if keyword.get('is_del', False):
                abandoned[kid] = keyword
                continue

            if all([
                self.min_priority is not None,
                priority < self.min_priority,
            ]):
                # lower priority
                continue

            if all([
                self.max_priority is not None,
                priority > self.max_priority,
            ]):
                # higher priority
                continue

            self.add_keyword_freq(kid, now)
            self.keywords[kid] = keyword

        reactor.callLater(LOAD_INTERVAL, self.load_tasks)