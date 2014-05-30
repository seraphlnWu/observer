# coding=utf8
#
# Insight Minr Active Spider Client Node
# CopyRight BestMiner Inc.
#

'''
    从controller节点获取任务数据
'''

import os
import json
import time
import socket
import urllib

from twisted.internet.defer import inlineCallbacks, returnValue

from observer.utils.http import request, TimedAgentPool, InfiniteLoginError
from observer.utils import wait
from observer.platform.sina.weibo.sweibo.utils import getAgent
from observer.node.client import ClientServiceBase
from observer import log



class NodeService(ClientServiceBase):
    ''' '''

    servicename = 'observer.sina.users.user_spider'

    def __init__(self, *args, **kwargs):
        ''' '''

        ClientServiceBase.__init__(self, *args, **kwargs)
        cfg = kwargs['cfg']
        self.name = socket.gethostname() + cfg.prefix    # node name

        self.proxy = cfg.http_proxy     # not used
        self.userAgent = cfg.http_agent
        self.max_agent = cfg.max_agent
        self.agentPool = TimedAgentPool(self.interval_min,
                                        self.interval_max,
                                        self.login_interval)
        self.token = self.get_token()
        self.last_clear = 0
        self.ready = True

    def get_token(self):
        ''' 获取一个可用的token'''
        url = 'http://insight.bestminr.com/get_token'
        return json.loads(urllib.urlopen(url).read()).get('access_token')

    def addAgent(self, seq):
        ''' 添加一个新的agent到agentPool '''
        agent = getAgent(self.proxy, self.userAgent)
        agent.remove = False
        agent.seq = seq
        self.agentPool.initAgent(agent)
        self.searchLoop(agent)

    @inlineCallbacks
    def startService(self):
        ''' start the fetch service '''
        os.environ['TZ'] = 'PRC'
        time.tzset()
        yield ClientServiceBase.startService(self)
        self.fillAgents()

    @inlineCallbacks
    def fillAgents(self):
        ''' '''
        while 1:
            seq = 0
            while len(self.agentPool.agents) < self.max_agent:
                seq += 1
                self.addAgent(seq)
            yield wait(10.)

    @inlineCallbacks
    def searchLoop(self, agent):
        ''' '''
        needbreak = False
        while 1:
            if agent.remove:
                self.agentPool.removeAgent(agent)
                break
            reqid, uid = yield self.callController('nextRequest', 'extract')
            log.info('Got uid %s from server' % uid)

            try:
                result = yield self.search(agent, uid)
            except InfiniteLoginError:
                log.exception()
                yield self.callController("fail", uid=uid)
                result = None
                needbreak = True
            except:
                log.exception()
                result = None
            self.callController('sendResult', reqid, uid, result)
            if needbreak:
                break

    @inlineCallbacks
    def getContent(self, agent, uid, token):
        ''' '''
        url = 'https://api.weibo.com/2/friendships/friends/ids.json?uid=%s&access_token=%s' % (uid, token)
        result = yield request(agent, url)
        returnValue(result)

    @inlineCallbacks
    def search(self, agent, uid, token):
        ''' '''
        data = yield self.getContent(agent, uid, token)

        if data is None:
            log.debug("Got Something Wrong with uid: %s" % uid)
            returnValue((None))

        returnValue((data))
