# coding=utf8
#
# Insight Minr Active Spider Controller Node
# CopyRight BestMiner Inc.
#

'''
    1. 管理关键词
    2. 管理子节点
'''

import time
import json
from datetime import datetime, timedelta
from collections import deque
from heapq import heappop, heappush

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor, defer

from observer import log
from observer.node.controller import ControllerServiceBase
from utils import check_exists, send_messages
from config import SEARCH_TIMEOUT
from observer.citycodes import cities


KEYWORDS_FILE = '/home/operation/observer/backtracking.txt'
KEYWORDS_FILE = 'backtracking.txt'
abandoned = {}

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
        return ((0, 0, 0, {}, 0), 0)

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

    servicename = 'observer.sina.weibo.backtracking_spider'

    def __init__(self, *args, **kwargs):
        ''' '''
        ControllerServiceBase.__init__(self, *args, **kwargs)
        cfg = kwargs['cfg']

        self.dbhost = cfg.mongo_host            # user db host
        self.dbport = cfg.mongo_port            # user db port
        self.dbname = cfg.mongo_dbname          # user db name
        self.collection = cfg.collection_name   # user db collection
        self.db_keywords_collection = cfg.db_keywords_collection # key db coll
        # 在重启controller结点的时候使用
        self.feed_id_file = cfg.feed_id_file    # feed文件
        self.keyword_freq_file = cfg.keyword_freq_file # 关键词词频文件

        # common configures
        self.min_priority = cfg.min_priority    # 最低优先级
        self.max_priority = cfg.max_priority    # 最高优先级
        self.key_queue = KeyQueue()             # 关键词任务列表
        self.keywords = {}                      # 关键词字典
        self.keywords_freq = {}                 # 关键词词频字典
        self.key_statuses = {}                  # 关键词本次抓回的微博记录
        self.search_defers = {}                 # 存储各种search任务

        self.m_keywords = {}
    def startService(self):
        ''' '''
        ControllerServiceBase.startService(self)
        self.refresh_keywords()

    def recalcKeywordFreq(self, kid, count, max_interval):
        ''' 重新计算关键词下次抓取的时间间隔 '''
        now = time.time()
        freq_info = self.keywords_freq.get(kid)
        interval = max(1.0, now-freq_info[2])

        # 如果本次抓取有weibo
        if count > 0:
            new_freq = count / interval
            new_freq = 0.75*freq_info[1] + 0.25*new_freq
        else:
            new_freq = freq_info[1]*0.5

        freq_info[1] = new_freq
        freq_info[0] = now + min(
            300.0,
            15.0/(freq_info[1] if freq_info[1] > 0 else 0.00000001))

        if max_interval > 0:
            freq_info[0] = min(freq_info[0], now+max_interval)

        freq_info[2] = now
        log.debug("Calc kid: %s freq: %s" % (str(kid), str(freq_info)))
        self.key_queue.add_key((kid, 1, 1, None, None), freq_info[0])
        log.debug("After Calc kid: %s freq: %s" % (str(kid), str(freq_info)))

    @staticmethod
    def getRegion(province, city):
        ''' '''
        return 'custom:%d:%d' % (province, city)

    def refresh_keywords(self):
        ''' '''
        now = time.time()
        with open(KEYWORDS_FILE) as fp:
            for l in fp.readlines():
                start_date, end_date = None, None
                keyword = json.loads(l)
                key_id = keyword['_id']
                priority = keyword.get('priority', 1)
                
                if keyword.get('start_date'):
                    start_date = datetime.strptime(keyword.get('start_date'), "%Y-%m-%d %H:%M:%S")
                    keyword['start_date'] = keyword.get('start_date')
                if keyword.get('end_date'):
                    end_date = datetime.strptime(keyword.get('end_date'), "%Y-%m-%d %H:%M:%S")
                    keyword['end_date'] = keyword.get('end_date')

                if keyword.get('is_del', False):
                    abandoned[key_id] = keyword
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
                if start_date:
                    for i in range((end_date-start_date).days*24):
                        s = start_date + timedelta(hours=i)
                        for c, k in cities.iteritems():
                            region = self.getRegion(c, 1000)
                            self.key_queue.add_key((key_id, 1, 1, {'start_date': s, 'end_date': s+timedelta(hours=1)}, region), now+1.0)
                else:
                    self.addKeywordFreq(key_id, now, start_date, end_date)
                self.keywords[key_id] = keyword

        #reactor.callLater(73.0, self.refresh_keywords)

    def addKeywordFreq(self, kid, now, start_date=None, end_date=None):
        ''' add keyword to task_queue '''
        if kid not in self.keywords_freq:
            self.keywords_freq[kid] = [now-1., 15/60., now]
            self.key_queue.add_key((kid, 1, 1, {'start_date': start_date, 'end_date': end_date}, None), now+1.0)

    def stopService(self):
        '''
            stop this service and write data info files
            for the next call
        '''
        with open(self.keyword_freq_file, 'wb') as fp:
            fp.write(json.dumps(self.keywords_freq))
 
        try:
            with open(self.feed_id_file, 'wb') as fp:
                d = {}
                for k, v in self.key_statuses.iteritems():
                    l = []
                    d[k] = l
                    for s in v:
                        l.append(list(s))

                fp.write(json.dumps(d))
                fp.close()
        except:
            pass

    def gotResult(self, data, kid, keyword, page, totalpage, t, max_interval, timescope, region):
        ''' '''
        tp, feeds = data

        if kid in self.key_statuses:
            statuses = self.key_statuses[kid]
        else:
            statuses = [set(), set(), set()]
            self.key_statuses[kid] = statuses

        log.info("Keyword: %s page: %d/%d result: %d" % (repr(keyword), page,
                                                         totalpage, len(feeds)))

        fds = []
        new_statuses = statuses[0]
        collided_feeds = 0
        next_request = False

        for feed in feeds:
            feed['kid'] = str(kid)
            if check_exists(feed):
                fds.append(feed)
            else:
                collided_feeds += 1

        # 发送消息给kafka队列
        #FIXME change to mongodb
        send_messages(kid, keyword, fds)

        # 判断是否需要继续抓取
        if collided_feeds * 3 > len(feeds):
            log.info("collided_feeds: %d with total: %d" % (collided_feeds, len(feeds)))
        else:
            page += 1
            totalpage = max(tp, totalpage)
            log.info("Page: %d with totalpage: %d" % (page, totalpage))
            if page < totalpage:
                next_request = True

        if next_request:
            statuses[1] |= new_statuses
            if len(statuses[1]) > 100:
                self.key_statuses[kid] = [set(), set(), statuses[1]]
            else:
                self.key_statuses[kid][0] = set()
            self.key_queue.add_key((kid, page, totalpage, timescope, region), t)
        else:
            pass

    def sendResult(self, reqid, skid, result):
        ''' '''
        # 如果reqid不在search defers中，直接返回
        if reqid not in self.search_defers:
            return

        defer_info = self.search_defers[reqid]
        defer_info['timeout'].cancel()
        d = defer_info['defer']

        if result is None:
            d.errback(Exception("Search error"))
        else:
            d.callback(result)
        del self.search_defers[reqid]

    @inlineCallbacks
    def nextRequest(self):
        ''' '''
        while 1:
            now = time.time()
            (kid, page, totalpage, timescope, region), t = yield self.key_queue.get_key()
            if kid in self.keywords:
                log.debug("Got keyword %s, %s at %s" % (str(kid), str(t), str(now)))
                self.m_keywords[kid] = now
                reqid = self.newRequestId()
                keyword_info = self.keywords[kid]
                skid = str(keyword_info['_id'])
                keyword = keyword_info.get('key')
                d = defer.Deferred()
                d.addCallback(self.gotResult, skid, keyword,
                              page, totalpage, t,
                              keyword_info.get('max_interval', -1), timescope, region,
                            ).addErrback(self.gotError, skid, page, totalpage, t, timescope, region)
                timeout = reactor.callLater(SEARCH_TIMEOUT,
                                            self.cancelSearch,
                                            reqid, kid)
                self.search_defers[reqid] = {'timeout': timeout, 'defer': d}
                log.info("Before sending %s to client" % skid)
                returnValue((reqid, keyword, page, timescope, region, skid))
                log.info("After sending %s to client" % skid)

    def cancelSearch(self, reqid, kid):
        ''' '''
        if reqid in self.search_defers:
            defer_info = self.search_defers[reqid]
            defer_info['defer'].errback(Exception("Timeout when waiting"))
            del self.search_defers[reqid]

    def gotError(self, fail, kid, page, totalpage, t, timescope, region):
        ''' '''
        log.exception(fail)
        self.key_queue.add_key((kid, page, totalpage, timescope, region), t)

    def clientFail(self, name, *args, **kwargs):
        ''' '''
        clientid = kwargs.get('clientid')
        log.info("Error happened with %s" % name)
        self.recalcKeywordFreq(kwargs.get('kid'), 0, kwargs.get('max_interval', 20))

        log.info("%s Client Failed. name: %s reason: %s" % (
            clientid,
            name,
            kwargs.get('reason', ''),
        ))
