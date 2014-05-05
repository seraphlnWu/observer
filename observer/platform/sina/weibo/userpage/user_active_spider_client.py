# coding=utf8

import re
import os
import json
import time
import random
import socket
import lxml.etree
from datetime import timedelta
from cookielib import CookieJar

from twisted.internet.defer import inlineCallbacks, returnValue

from observer.utils.http import request, TimedAgentPool, InfiniteLoginError
from observer.utils import wait
from observer.platform.sina.weibo.sweibo.utils import getAgent
from observer.node.client import ClientServiceBase
from observer import log

ONE_HOUR = timedelta(seconds=3600)


WEIBO_FEED_PATTERN = re.compile(
    '<script>[^\n]*FM.view[ \t]*\\((?P<js>[^\n]*)\\)[^\n]*</script>')
WEIBO_COUNT_PATTERN = re.compile('\\((?P<count>\\d+)\\)')


class NodeService(ClientServiceBase):
    ''' sweibo fetcher node '''

    servicename = 'observer.sina.weibo.user_active_spider'

    def __init__(self, *args, **kwargs):
        ''' '''

        ClientServiceBase.__init__(self, *args, **kwargs)
        cfg = kwargs['cfg']
        self.name = socket.gethostname() + cfg.prefix    # node name

        self.proxy = cfg.http_proxy     # not used
        self.userAgent = cfg.http_agent
        self.interval_min = cfg.http_interval_min
        self.interval_max = cfg.http_interval_max
        self.avg_interval = (self.interval_min + self.interval_max) / 2.0
        self.login_interval = cfg.login_interval
        self.max_agent = cfg.max_agent

        self.agentPool = TimedAgentPool(
            self.interval_min,
            self.interval_max,
            self.login_interval,
        )

        self.last_clear = 0
        self.count = int(3600.0 * 2 / self.avg_interval)
        self.ready = False

    def addAgent(self, username, password, seq):
        '''
            Add a new agent to the pool.
            In fact, an agent is a username & password of sina weibo.
        '''
        cookies = CookieJar()
        agent = getAgent(
            username,
            password,
            self.proxy,
            self.userAgent,
            cookies,
        )
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
    def getUser(self):
        ''' get a user by a remotecall '''
        username, password, seq = yield self.callController('getUser')
        returnValue((username, password, seq))

    @inlineCallbacks
    def checkAgents(self):
        #FIXME Should think about how to realize this function
        while True:
            for agent in self.agentPool.agents:
                if not agent.remove:
                    validated = yield self.callController(
                        'refresh',
                        agent.username,
                        agent.seq,
                    )
                    if not validated:
                        agent.remove = True
            yield wait(300.0)

    @inlineCallbacks
    def fillAgents(self):
        ''' '''
        while True:
            while len(self.agentPool.agents) < self.max_agent:
                username, password, seq = yield self.getUser()
                if username is not None:
                    self.addAgent(username, password, seq)
                yield wait(self.login_interval)
            yield wait(60.0)

    @inlineCallbacks
    def searchLoop(self, agent=None):
        #FIXME REFACTOR THIS METHOD
        needbreak = False
        while True:
            kwg = {}
            if agent.remove:
                self.agentPool.removeAgent(agent)
                break

            requestid, _, _, kwg = yield self.controllerPull()
            if not requestid:
                wait(random.uniform(self.interval_min, self.interval_max))
                break
            try:
                result = yield self.search(kwg.get('uid'), agent)
            except InfiniteLoginError as msg:
                log.exception()
                kwg.update({'reason': str(msg)})
                yield self.controllerFail(
                    'fail',
                    *(requestid, self.servicename, self.clientid),
                    **kwg
                )
                result = None
                needbreak = True
            except:
                log.exception()
                result = None

            kwg.update({'data': result or []})
            self.controllerPush(
                'push',
                *(requestid, self.servicename, self.clientid),
                **kwg
            )
            wait(random.uniform(self.interval_min, self.interval_max))
            if needbreak:
                break

    @inlineCallbacks
    def getContent(self, agent, uid):
        ''' '''
        url = 'http://weibo.com/p/100505%s/info?from=page_100505&mod=TAB#place' % (uid, )
        result = yield request(agent, url)
        returnValue(result)

    @staticmethod
    def getFeedsHtml(content):
        """ 
        新浪用户结果使用js填充页面。
        填充内容在FM.view的调用参数里面，其中domid
        为 
            [
                Pl_Official_LeftInfo__16,
                Pl_Official_LeftInfo__17,
            ]
        的调用中，
        字典里的html对应的键值对保存了要现实的html内容。
        """
        JS_DOMID = ['Pl_Official_LeftInfo__16', 'Pl_Official_LeftInfo__17']
        for m in WEIBO_FEED_PATTERN.finditer(content):
            d = json.loads(m.group('js'))
            if d['domid'] in JS_DOMID:
                return d['html']
        return None

    @staticmethod
    def getFeeds(html_content):
        '''
            div[@class='infoblock'] 下存放的是个人基本信息

            暂定为:
            {
                'name': {
                    'key': //div[@class='infoblock'][1]/div[1]/div[1]//text(),
                    'value': //div[@class='infoblock'][1]/div[1]/div[2]//text()     # 0
                },
                'location': {
                    'key': //div[@class='infoblock'][1]/div[2]/div[1]//text()")
                    'value': //div[@class='infoblock'][1]/div[2]/div[2]//text()")   # 0
                },
                'gender': {
                    'key': //div[@class='infoblock'][1]/div[3]/div[1]//text()"),
                    'value': //div[@class='infoblock'][1]/div[3]/div[2]//text()")
                },
                'birthday': {
                    'key': //div[@class='infoblock'][1]/div[4]/div[1]//text()"),
                    'value': //div[@class='infoblock'][1]/div[4]/div[2]//text()")      # 19xx年x月x日
                },
                'blog': {
                    'key': //div[@class='infoblock'][1]/div[5]/div[1]//text()")
                    'value': //div[@class='infoblock'][1]/div[5]/div[2]//text()")          # 1
                },
                'domain': {
                    'key': //div[@class='infoblock'][1]/div[6]/div[1]//text()"),
                    'value': //div[@class='infoblock'][1]/div[6]/div[2]//text()")        # 1
                },
                'description': {
                    'key': //div[@class='infoblock'][7]/div[3]/div[1]//text()"),
                    'value': //div[@class='infoblock'][7]/div[3]/div[2]//text()")
                },
            }

        '''
        print html_content
        html = lxml.etree.HTML(html_content)
        el = html.xpath("//div[@class='infoblock']") 
        if not el:
            return [], 0

        template = {
            u'昵称': 'name',
            u'真实姓名': 'rname',
            u'所在地': 'location',
            u'性别': 'gender',
            u'生日': 'birthday',    # 19xx年x月x日
            u'星座': 'constellation',
            u"性取向": 'strend',
            u"感情状况": 'emotional',
            u"血型": 'blood',
            u'博客': 'blog',
            u'个性域名': 'domain',
            u'简介': 'description',
            u"邮箱": "email",
            u"QQ": 'qq',
            u"MSN": "msn",
            u"公司": 'career',
            u"大学": 'colleage',
            u"高中": 'high_school',
            u"中专技校": "tecnical_school",
            u"初中": 'middle_school',
            u"小学": 'primary_school',
            u"标签": 'tags',
            u"认证原因": 'verified_reason',
        }


        # in fact, I can do more here, but now, I only need a counter
        count = html.xpath("//form[@class='info_title'][1]/fieldset/legend//text()")

        key_xpath_template = "//div[@class='infoblock'][%s]/div[%s]/div[1]//text()"
        val_xpath_template = "//div[@class='infoblock'][%s]/div[%s]/div[2]//text()"

        d = dict.fromkeys(template.values(), None)

        for i in range(1, len(count)+1):
            #FIXME DO PARSEING
            for j in range(1, 15):
                k = html.xpath(key_xpath_template % (i, j))
                if len(k) > 0:
                    key = k[0]
                    v = html.xpath(val_xpath_template % (i, j))
                    if key in template:
                        vals = filter(lambda x: x, map(lambda x: x.strip(), v))
                        if vals:
                            if len(vals) > 1:
                                d[template[key]] = vals
                            else:
                                d[template[key]] = vals[0]
                else:
                    pass

        return d


    @inlineCallbacks
    def search(self, uid, agent):
        ''' '''
        _, feeds = yield self._search(uid, agent)
        returnValue(feeds)

    @inlineCallbacks
    def _search(self, uid, agent):
        ''' '''

        log.debug("Fetched_uid: %s." + str(uid))
        page_content = yield self.getContent(agent, uid)
        try:
            html_content = self.getFeedsHtml(page_content)
        except ValueError:
            returnValue((False, None))
            return
        if html_content is None:
            log.debug('got feed content: ' + page_content)

        data = self.getFeeds(html_content)
        returnValue((True, data))
