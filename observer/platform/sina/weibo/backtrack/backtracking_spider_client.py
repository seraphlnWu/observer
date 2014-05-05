# coding=utf8
#
# Insight Minr Active Spider Client Node
# CopyRight BestMiner Inc.
#

'''
    从本地load用户信息
    从controller节点加载任务信息
'''

import re
import os
import json
import time
import random
import socket
from datetime import datetime
import lxml.etree
from heapq import heappop
from urllib import quote_plus
from datetime import timedelta
from cookielib import CookieJar

from twisted.internet.defer import inlineCallbacks, returnValue

from observer.citycodes import cities
from observer.utils.http import request, TimedAgentPool, InfiniteLoginError
from observer.utils import wait
from utils import url_decode, getAgent
from observer.node.client import ClientServiceBase
from observer import log
from config import users

ONE_HOUR = timedelta(seconds=3600)


WEIBO_FEED_PATTERN = re.compile(
    '<script>[^\n]*STK.pageletM.view[ \t]*\\((?P<js>[^\n]*)\\)[^\n]*</script>')
WEIBO_COUNT_PATTERN = re.compile('\\((?P<count>\\d+)\\)')

        
class NodeService(ClientServiceBase):
    ''' sweibo fetcher node '''

    servicename = 'observer.sina.weibo.backtracking_spider'

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
        self.count = int((3600.0*2)/self.avg_interval)
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

    @staticmethod
    def getRegion(province, city):
        ''' '''
        return 'custom:%d:%d' % (province, city)

    @staticmethod
    def timeScopeStr(t):
        ''' '''
        return t.strftime('%Y-%m-%d-') + str(t.hour)

    @staticmethod
    def getTimeScope(begintime, endtime):
        ''' '''
        return 'custom:%s:%s' % (
            NodeService.timeScopeStr(begintime),
            NodeService.timeScopeStr(endtime),
        )

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
            while len(self.agentPool.agents) < self.max_agent:
                username, password, seq = heappop(users)
                if username is not None:
                    self.addAgent(username, password, seq)
                yield wait(self.login_interval)
            yield wait(60.0)

    @inlineCallbacks
    def searchLoop(self, agent=None):
        #FIXME REFACTOR THIS METHOD
        needbreak = False
        while True:
            if agent.remove:
                self.agentPool.removeAgent(agent)
                break

            reqid, keyword, page, timescope, region, skid = yield self.callController('nextRequest')

            begintime = timescope.get('start_date')
            endtime = timescope.get('end_date')
            timescope = self.getTimeScope(begintime, endtime-ONE_HOUR)

            try:
                result = yield self.search(
                    agent=agent,
                    keyword=keyword,
                    page=page,
                    timescope=timescope,
                    region=region,
                )
            except InfiniteLoginError as msg:
                log.exception()
                yield self.callController("fail", agent.username, kid=skid)
                result = None
                needbreak = True
            except:
                log.exception()
                result = None

            log.info("Sending Data to server with kid %s" % skid)
            self.callController('sendResult', reqid, skid, result)
            log.info("Sent Data to server with kid %s" % skid)
            yield wait(random.uniform(self.interval_min, self.interval_max))
            if needbreak:
                break

    @inlineCallbacks
    def getContent(self, agent, keyword, page, timescope, region):
        ''' '''
        url = 'http://s.weibo.com/weibo/%s&Refer=index&page=%d' % (
            quote_plus(quote_plus(keyword)),
            page,
        )
        if region is not None:
            url += '&region=' + region
        if timescope is not None:
            url += '&timescope=' + timescope
        url += '&nodup=1'
        result = yield request(agent, url)
        returnValue(result)


    @staticmethod
    def getFeedsHtml(content):
        """
        新浪微博搜索结果页使用JS填充搜索内容。
        填充内容放在STK.pageletM.view的调用参数里面，其中pid为pl_weibo_feedlist的
        调用中，字典里的html对应的键值保存了要显示的html内容。
        """
        for m in WEIBO_FEED_PATTERN.finditer(content):
            d = json.loads(m.group('js'))
            if d['pid'] in ['pl_weibo_direct', 'pl_weibo_feedlist']:
                return d['html']
        return None

    @staticmethod
    def getText(el):
        """
        提取微博正文中的文本。
        所有的&nbsp;替换成空格
        表情图片替换成[表情]。
        """
        has_link = False
        text = ''
        if el.tag == 'img' and el.get('type', None) == 'face':
            text += el.get('alt', '')
        elif el.tag == 'a' and el.get('mt','') == 'url':
            has_link = True
        t = el.text
        if t is not None:
            text += t.replace(u'&nbsp;', u' ')
        for e in el.iterchildren():
            t,hl = NodeService.getText(e)
            text += t
            has_link = has_link or hl
        t = el.tail
        if t is not None:
            text += t.replace(u'&nbsp;', u' ')
        return text, has_link

    @staticmethod
    def parseTextContent(el):
        """
        微博文字内容中包含的信息
        name: 用户名
        isV: 是否认证用户
        haslink: 是否有链接
        uid: 用户id
        text: 内容
        """
        d = {}
        d['name'] = unicode(el.xpath('./a/@nick-name')[0])
        d['isV'] = bool(el.xpath("./a/img[@class='approve' or @class='approve_co']"))
        text_el= el.xpath("./em")[0]
        d['text'], d['haslink'] = NodeService.getText(text_el)
        return d

    @staticmethod
    def parsePic(el):
        """
        提取图片信息
        """
        imgl = el.xpath("./ul[@class='piclist']/li/img[@action-type='feed_list_media_img']/@src")
        if len(imgl) == 0:
            return None
        return unicode(imgl[0])

    @staticmethod
    def parseForwardedPic(el):
        """
        提取图片信息
        """
        imgl = el.xpath("./dd/ul[@class='piclist']/li/img[@action-type='feed_list_media_img']/@src")
        if len(imgl) == 0:
            return None
        return unicode(imgl[0])

    @staticmethod
    def getCount(text):
        m = WEIBO_COUNT_PATTERN.search(text)
        if m:
            return int(m.group('count'))
        return 0

    @staticmethod
    def parseMiscInfo(el):
        """
        提取微博的评论数、转发数以及发表时间和来源，从微博地址获取mid
        """
        e = el.xpath("./*[@class='info W_linkb W_textb']")[0]
        d = {
            'ccount': 0,
            'rcount': 0,
            'source': '',
        }
        al = e.xpath("./span/a")
        for a in al:
            if a.get('action-type', '') == 'feed_list_forward':
                d['rcount'] = NodeService.getCount(a.text)
            if a.get('action-type', '') == 'feed_list_comment':
                d['ccount'] = NodeService.getCount(a.text)
        d['cdate'] = int(
            e.xpath("./a[@node-type='feed_list_item_date']/@date")[0])
        hrefl = unicode(
            e.xpath("./a[@node-type='feed_list_item_date']/@href")[0]
        ).split('/')
        mid = url_decode(hrefl[-1])
        d['uid'] = int(hrefl[-2])
        d['source'] = unicode(e.xpath("./a[last()]/text()")[0])
        return d, mid

    @staticmethod
    def parseContent(el):
        """
        提取内容信息
        """
        d = NodeService.parseTextContent(
            el.xpath("./p[@node-type='feed_list_content']")[0])
        d['pic'] = NodeService.parsePic(el)
        md, mid = NodeService.parseMiscInfo(el)
        d['hot'] = bool(el.xpath("./div[@class='hot_feed']"))
        d.update(md)
        return d

    @staticmethod
    def parseForwardContent(el):
        """
        提取转发内容信息
        """
        try:
            log.debug("parsing forwarded content...")
            d = NodeService.parseTextContent(
                el.xpath("./dt[@node-type='feed_list_forwardContent']")[0])
            d['pic'] = NodeService.parseForwardedPic(el)
            md, mid = NodeService.parseMiscInfo(el)
            d.update(md)
            d['mid'] = mid
            return d
        except:     # 过滤被删除微博
            log.exception()
            return None

    @staticmethod
    def parseFace(el):
        """
        获取头像信息
        """
        imgl = el.xpath("./a/img/@src")
        if len(imgl) == 0:
            return None
        return unicode(imgl[0])

    @staticmethod
    def parseFeed(el):
        """
        获取微博信息
        _id: 微博id
        ccount: 微博的评论数
        cdate: 发布时间
        fcount: 粉丝数 (无)
        flash: 曝光数，暂等于粉丝数(无)
        gender：性别（无）
        haslink: 微博内容是否有链接
        hot: 是否热门微博
        isV: 发表者是否加V
        loc: 发表者所在地（无）
        name: 发表者昵称
        pan: 正负面(无)
        pan_ot:修正后的正负面(无)
        pic: 内容中的图片
        rcount: 转发数
        retweet: { 原微博信息
            rcount:
            isV:
            uid:
            ccount:
            source:
            date:
            text:
            pic:
            id:
            s_name:
        }
        segment:分词(无)
        source: 来源
        text: 内容
        uid: 用户id
        uimg: 用户头像
        """
        d = {
            'fcount': None,
            'flash': None,
            'gender': None,
            'loc': None,
            'pan': None,
            'pan_ot': None,
            'segment': None,
        }
        try:
            mid = el.get('mid', None)
            if mid is not None:
                mid = int(mid)
            else:
                return None
            d['_id'] = mid
            d['id'] = mid
            d['uimg'] = NodeService.parseFace(
                el.xpath("./dt[@class='face']")[0])
            ce = el.xpath("./dd[@class='content']")[0]
            d.update(NodeService.parseContent(ce))
            rl = ce.xpath("./dl/dt[@node-type='feed_list_forwardContent']")
            d['retweet'] = {
                'rcount': None,
                'isV': None,
                'uid': None,
                'ccount': None,
                'text': None,
                'source': None,
                'date': None,
                'pic': None,
                'id': None,
                's_name': None,
            }
            if rl:
                e = rl[0].getparent()
                fd = NodeService.parseForwardContent(e)
                if fd is not None:
                    rd = d['retweet']
                    rd['rcount'] = fd['rcount']
                    rd['isV'] = fd['isV']
                    rd['uid'] = fd['uid']
                    rd['ccount'] = fd['ccount']
                    rd['text'] = fd['text']
                    rd['source'] = fd['source']
                    rd['id'] = fd['mid']
                    rd['pic'] = fd['pic']
                    rd['s_name'] = fd['name']
                    rd['date'] = fd['cdate']
            return d
        except:
            log.error('error when parsing: ' + lxml.etree.tostring(el))
            log.exception()
            return None

    @staticmethod
    def parseUser(el):
        '''
            _id: user_id
            ./div[@class='person_pic']/a/img/@uid
            1

            avatar: 
            ./div[@class='person_pic']/a/img/@src
            1

            nickname: 
            ./div[@class='person_detail']/p/[@class='person_name']/a/text()
            1

            gender:
            ./div[@class='person_detail']/p/[@class='person_addr']/span[1]/@title
            1

            location: 
            ./div[@class='person_detail']/p/[@class='person_addr']/span[2]/text()
        '''

    @staticmethod
    def getUsers(html_content):
        '''
            <dl action-type='feeds_list_item'>下存放的是微博内容
        '''
        html = lxml.etree.HTML(html_content)
        el = html.xpath("//div[@class='pl_noresult']")
        if el:
            return [], 0
        el = html.xpath("//div[@class='pl_personlist']/div")
        users = filter(
            lambda e:e is not None,
            map(NodeService.parseUser, el)            
        )
        tpl = html.xpath("//ul[@class='search_page_M']/li[last()-1]/a/text()")
        tps = 0
        if tpl:
            try:
                tps = int(tpl[0])
            except:
                log.info("No page found: " + repr(tpl))
        return users, tps
        

    @staticmethod
    def getFeeds(html_content):
        """
        <dl action-type="feed_list_item">下存放的是微博内容
        """
        html = lxml.etree.HTML(html_content)
        el = html.xpath("//div[@class='pl_noresult']")
        if el:
            return [], 0
        el = html.xpath("//dl[@action-type='feed_list_item']")
        feeds = filter(
            lambda e: e is not None,
            map(NodeService.parseFeed, el)
        )
        #查找最后一页的页数，如果已经在最后一页，那么取到的是上一页的页码
        #如果只有一页，那么取不到此元素
        #log.debug('html: %s'%lxml.etree.tostring(html))
        tpl = html.xpath("//ul[@class='search_page_M']/li[last()-1]/a/text()")
        tps = 0
        if tpl:
            try:
                tps = int(tpl[0])
            except:
                tps = 0
                log.info('no page found: '+repr(tpl))
                pass
        return feeds, tps

    @inlineCallbacks
    def _searchHour(self, agent, keyword, statuses, t):
        ''' '''
        timescope = self.getTimeScope(t, t)
        astatuses = set()
        haveResult, feeds = yield self._search(
            agent,
            keyword,
            statuses,
            None,
            timescope,
        )

        if haveResult:
            returnValue(feeds)
            return
        #feeds = []
        for province in cities:
            feeds += yield self._searchProvince(
                agent,
                keyword,
                statuses,
                astatuses,
                timescope,
                province,
            )
        returnValue(feeds)

    @inlineCallbacks
    def _searchProvince(
        self,
        agent,
        keyword,
        statuses,
        astatuses,
        timescope,
        province,
    ):
        region = self.getRegion(province, 1000)
        if len(cities[province]) > 1:
            haveResult, feeds = yield self._search(
                agent,
                keyword,
                statuses,
                astatuses,
                timescope,
                region,
            )
        else:
            haveResult, feeds = yield self._search(
                agent,
                keyword,
                statuses,
                astatuses,
                timescope,
                region,
                True,
            )

        if haveResult:
            returnValue(feeds)
            return

        feeds = []
        for city, _ in cities[province][1:]:
            region = self.getRegion(province, city)
            try:
                _, fs = yield self._search(
                    agent,
                    keyword,
                    statuses,
                    astatuses,
                    timescope,
                    region,
                    True,
                )
                if fs:
                    feeds += fs
            except:
                break

        returnValue(feeds)

    @inlineCallbacks
    def search(self, agent, keyword, page, timescope=None, region=None, skid=None):
        ''' '''
        skeyword = keyword.encode('utf8')
        page_content = yield self.getContent(agent, skeyword, page, timescope, region)
        html_content = self.getFeedsHtml(page_content)

        if html_content is None:
            log.debug("Got feed content: " + str(page_content))
            returnValue((None, None))

        feeds, tp = self.getFeeds(html_content)
        map(lambda x: x.update({'kid': skid}), feeds)
        log.info("GotFeeds: %d, page: %d/%d" % (len(feeds), page, tp))
        returnValue((tp, feeds))

    #@inlineCallbacks
    #def search(self, agent, keyword, statuses, begintime=None, endtime=None):
    #    ''' '''
    #    if not begintime:
    #        _, feeds = yield self._search(agent, keyword, statuses)
    #        returnValue(feeds)
    #        return

    #    begintime = datetime.strptime(begintime, '%Y-%m-%d %H:%M:%S')
    #    endtime = datetime.strptime(endtime, '%Y-%m-%d %H:%M:%S')

    #    timescope = self.getTimeScope(begintime, endtime-ONE_HOUR)
    #    haveResult, feeds = yield self._search(agent, keyword, statuses, None, timescope)
    #    if haveResult:
    #        returnValue(feeds)

    #    #feeds = []
    #    #curtime = begintime
    #    #while curtime < endtime:
    #    #    feeds += yield self._searchHour(agent, keyword, statuses, curtime)
    #    #    curtime += ONE_HOUR
    #    #returnValue(feeds)

    #@inlineCallbacks
    #def _search(
    #    self,
    #    agent,
    #    keyword,
    #    statuses,
    #    astatuses=None,
    #    timescope=None,
    #    region=None,
    #    force=False,
    #):
    #    fetched_feeds = set(statuses)
    #    log.debug("Fetched_feeds: " + str(len(fetched_feeds)))
    #    skeyword = keyword
    #    cp, tp = 1, 1
    #    feeds = []
    #    new_fetched_feeds = set()

    #    while cp <= tp:
    #        collided_feeds = 0
    #        page_content = yield self.getContent(agent, skeyword, cp, timescope, region)
    #        cp += 1
    #        html_content = self.getFeedsHtml(page_content)
    #        if html_content is None:
    #            log.debug('got feed content: ' + page_content)
    #            break
    #        #TODO: 处理异常情况
    #        fs, ntp = self.getFeeds(html_content)
    #        if ntp > tp:
    #            tp = ntp
    #        log.info('gotfeeds: %d, pages: %d/%d' % (len(fs), cp - 1, tp))
    #        if timescope is not None and tp == 50 and not force:
    #            #回溯到达上限，直接返回
    #            returnValue((False, None))
    #            return
    #        if cp >= 10:
    #            break
    #        if tp == 50 and astatuses is not None:
    #            fetched_feeds |= astatuses
    #        for feed in fs:
    #            mid = feed['_id']
    #            if mid in fetched_feeds:
    #                collided_feeds += 1
    #            else:
    #                if mid not in new_fetched_feeds:
    #                    feeds.append(feed)
    #            if tp == 50 and astatuses is not None:
    #                astatuses.add(mid)
    #            new_fetched_feeds.add(mid)
    #        if collided_feeds > 0 and len(fs) / collided_feeds < 3:
    #            break
    #        yield wait(random.uniform(self.interval_min, self.interval_max))
    #    returnValue((True, feeds))
