# coding=utf8
#
# Insight Minr Active Spider Controller Node
# CopyRight BestMiner Inc.
#

'''
    1. 管理任务队列
    2. 管理子节点状况
'''

from twisted.internet import reactor, defer
import time
from observer import log
from observer.node.controller import ControllerServiceBase
from twisted.internet.defer import inlineCallbacks, returnValue
from config import SEARCH_TIMEOUT
from utils import check_duplicate, save_extract_ids, save_statuses
from base_redis import RedisOp

TASK_QUEUE = 'task_queue'

ttype_mapper = {
    'extract': 'extract_queue',
    'user': 'task_queue',
}


class ControllerService(ControllerServiceBase):
    ''' '''

    servicename = 'observer.sina.users.status_active_spider'

    def __init__(self, *args, **kwargs):
        ''' '''
        ControllerServiceBase.__init__(self, *args, **kwargs)
        cfg = kwargs['cfg']

        self.redis = RedisOp()


    def startService(self):
        ''' 启动任务 '''
        ControllerServiceBase.startService(self)

        # 启动前得准备
        self.get_prepared()

    def get_prepared(self):
        ''' '''

        # 打开redis连接
        self.redis.open_connection()

    def stopService(self):
        '''
            stop this service and write data info files
            for the next call
        '''
        pass

    def gotResult(self, data, uid, ttype):
        '''
            获取数据。任务分2种。
            1. 抓取用户的关注列表，这部分需要写入到task_queue中
            2. 抓取用户的个人信息以及最新得100条微博，这部分需要写入到文件
        '''
        if ttype == 'extract':
            tids = check_duplicate(self.redis, data)
            save_extract_ids(self.redis, tids)
        else:
            save_statuses(data)

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
    def nextRequest(self, ttype):
        ''' '''
        while 1:
            now = time.time()
            uid = self.redis.query_list_data(ttype_mapper[ttype])

            reqid = self.newRequestId()
            d = defer.Deferred()
            d.addCallback(self.gotResult, uid, ttype).addErrback(self.gotError, uid)
            timeout = reactor.callLater(SEARCH_TIMEOUT, self.cancelSearch, reqid)
            self.search_defers[reqid] = {'timeout': timeout, 'defer': d}
            returnValue((reqid, uid))

    def cancelSearch(self, reqid):
        ''' '''
        if reqid in self.search_defers:
            defer_info = self.search_defers[reqid]
            defer_info['defer'].errback(Exception("Timeout when waiting"))
            del self.search_defers[reqid]

    def gotError(self, fail, uid):
        ''' 当出现异常时，将任务重新写入到Redis队列 '''
        log.exception(fail)
        self.redis.push_list_data(TASK_QUEUE, uid, direct='right')

    def clientFail(self, *args, **kwargs):
        ''' '''
        clientid = kwargs.get('clientid')
        log.info("%s Client Failed, reason: %s" % (clientid, kwargs.get('reason', '')))
