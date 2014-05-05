# coding=utf8
#
# Insight Minr Active Spider Client Node
# CopyRight BestMiner Inc.
#

'''
    从本地load用户信息
    从controller节点加载任务信息
'''

import random
import re
import os
import json
import time
import socket
import lxml.etree
from urllib import quote_plus
from datetime import timedelta
from cookielib import CookieJar
from heapq import heappop

from twisted.internet.defer import inlineCallbacks, returnValue

from observer.utils.http import request, TimedAgentPool, InfiniteLoginError
from observer.utils import wait
from observer.platform.sina.weibo.sweibo.utils import url_decode, getAgent
from observer.node.client import ClientServiceBase
from observer import log
from config import users


ONE_HOUR = timedelta(seconds=3600)

WEIBO_FEED_PATTERN = re.compile(
    '<script>[^\n]*STK.pageletM.view[ \t]*\\((?P<js>[^\n]*)\\)[^\n]*</script>')
WEIBO_COUNT_PATTERN = re.compile('\\((?P<count>\\d+)\\)')


class NodeService(ClientServiceBase):
    ''' '''

    servicename = 'observer.sina.weibo.status_active_spider'

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
        self.agentPool = TimedAgentPool(self.interval_min,
                                        self.interval_max,
                                        self.login_interval)
        self.last_clear = 0
        self.count = int(3600.0*2 / self.avg_interval)
        self.ready = True

    def addAgent(self, username, password, seq):
        ''' 添加一个新的agent到agentPool '''
        cookies = CookieJar()
        agent = getAgent(username, password,
                         self.proxy, self.userAgent, cookies)
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
        username, password, seq = yield self.callController('getUser', self.clientid)
        returnValue((username, password, seq))

    @inlineCallbacks
    def fillAgents(self):
        ''' '''
        while 1:
            #FIXME 添加用户信息到本地文件
            while len(self.agentPool.agents) < self.max_agent:
                username, password, seq = heappop(users)
                #username, password, seq = yield self.getUser()
                if username is not None:
                    self.addAgent(username, password, seq)
                yield wait(self.login_interval)
            yield wait(60.)

    @inlineCallbacks
    def searchLoop(self, agent):
        ''' '''
        needbreak = False
        while 1:
            if agent.remove:
                self.agentPool.removeAgent(agent)
                break
            reqid, keyword, page, timescope, region, skid = yield self.callController('nextRequest')
            log.info("Got keyword %s from server" % skid)

            try:
                result = yield self.search(agent, keyword,
                                           page, timescope, region)
            except InfiniteLoginError:
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
        url = 'http://s.weibo.com/wb/%s&Refer=index&page=%d' % (
            quote_plus(quote_plus(keyword)),
            page)
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
        if not content:
            pass
        else:
            for m in WEIBO_FEED_PATTERN.finditer(content):
                d = json.loads(m.group('js'))
                if d['pid'] in ['pl_weibo_direct', 'pl_weibo_feedlist', 'pl_wb_feedlist']:
                    return d['html']
        return None

    @staticmethod
    def getText(el):
        """
        提取微博正文中的文本。
        所有的&nbsp;替换成空格
        表情图片替换成[表情]。
        """
        url = None
        has_link = False
        text = ''
        if el.tag == 'img' and el.get('type', None) == 'face':
            text += el.get('alt', '')
        elif el.tag == 'a' and el.get('mt','') == 'url':
            has_link = True
            url = el.get('title')
        t = el.text
        if t is not None:
            text += t.replace(u'&nbsp;', u' ')
        for e in el.iterchildren():
            t,hl,u = NodeService.getText(e)
            text += t
            has_link = has_link or hl
            url = url or u
        t = el.tail
        if t is not None:
            text += t.replace(u'&nbsp;', u' ')

        return text, has_link, url

    @staticmethod
    def parseTextContent(el):
        """
        微博文字内容中包含的信息
        name: 用户名
        v: 是否认证用户
        hl: 是否有链接
        uid: 用户id
        text: 内容
        """
        d = {}
        try:
            d['name'] = unicode(el.xpath('./a/@nick-name')[0])
        except:
            d['name'] = ''
        try:
            d['v'] = bool(el.xpath("./a/img[@class='approve' or @class='approve_co']"))
        except:
            d['v'] = False
        try:
            text_el= el.xpath("./em")[0]
            d['txt'], d['hl'], d['ourl'] = NodeService.getText(text_el)
        except:
            d['txt'] = ''
            d['hl'] = False
            d['ourl'] = None
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
            'cmt': 0,
            'rpt': 0,
            'src': '',
        }
        al = e.xpath("./span/a")
        for a in al:
            if a.get('action-type', '') == 'feed_list_forward':
                d['rpt'] = NodeService.getCount(a.text)
            if a.get('action-type', '') == 'feed_list_comment':
                d['cmt'] = NodeService.getCount(a.text)
        try:
            d['ct'] = int(
                e.xpath("./a[@node-type='feed_list_item_date']/@date")[0])
        except:
            d['ct'] = 0
        hrefl = unicode(
            e.xpath("./a[@node-type='feed_list_item_date']/@href")[0]
        ).split('/')
        mid = url_decode(hrefl[-1])
        try:
            d['uid'] = int(hrefl[-2])
        except ValueError:
            d['uid'] = 0
        d['src'] = unicode(e.xpath("./a[last()]/text()")[0])
        return d, mid

    @staticmethod
    def parseContent(el):
        """
        提取内容信息
        """
        d = NodeService.parseTextContent(
            el.xpath("./p[@node-type='feed_list_content']")[0])
        d['p'] = NodeService.parsePic(el)
        md, mid = NodeService.parseMiscInfo(el)
        d['h'] = bool(el.xpath("./div[@class='hot_feed']"))
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
            d['p'] = NodeService.parseForwardedPic(el)
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
        cmt: 微博的评论数
        rpt: 转发数
        ct: 发布时间
        fcnt: 粉丝数 (无)
        imp: 曝光数，暂等于粉丝数(无)
        g：性别（无）
        hl: 微博内容是否有链接
        h: 是否热门微博
        v: 发表者是否加V
        l: 发表者所在地（无）
        n: 发表者昵称
        st: 正负面(无)
        stot:修正后的正负面(无)
        p: 内容中的图片
        retweet: { 原微博信息
            rpt:
            v:
            uid:
            cmt:
            src:
            ct:
            txt:
            p:
            id:
            n:
        }
        seg:分词(无)
        src: 来源
        txt: 内容
        uid: 用户id
        upi: 用户头像
        """
        d = {
            'fcnt': None,
            'imp': None,
            'g': None,
            'l': None,
            'st': None,
            'stot': None,
            'seg': None,
        }
        try:
            mid = el.get('mid', None)
            if mid is not None:
                mid = int(mid)
            else:
                return None
            d['_id'] = mid
            d['id'] = mid
            d['upi'] = NodeService.parseFace(
                el.xpath("./dt[@class='face']")[0])
            ce = el.xpath("./dd[@class='content']")[0]
            d.update(NodeService.parseContent(ce))
            rl = ce.xpath("./dl/dt[@node-type='feed_list_forwardContent']")
            if rl:
                e = rl[0].getparent()
                fd = NodeService.parseForwardContent(e)
                if fd is not None:
                    rd = d.setdefault('retweet', {})

                    rd.update({
                        'txt': fd['txt'],
                        'rpt': fd['rpt'],
                        'uid': fd['uid'],
                        'n': fd['name'],
                        'id': fd['mid'],
                        'v': fd['v'],
                        'cmt': fd['cmt'],
                        'src': fd['src'],
                        'p': fd['p'],
                        'ct': fd['ct'],
                    })

            return d
        except:
            log.error('error when parsing: ' + lxml.etree.tostring(el))
            log.exception()
            return None

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
    def search(self, agent, keyword, page, timescope=None, region=None):
        ''' '''
        skeyword = keyword.encode('utf8')
        page_content = yield self.getContent(agent, skeyword, page, timescope, region)
        html_content = self.getFeedsHtml(page_content)

        if html_content is None:
            log.debug("Got feed content: " + str(page_content))
            returnValue((None, None))

        feeds, tp = self.getFeeds(html_content)
        log.info("GotFeeds: %d, page: %d/%d" % (len(feeds), page, tp))
        returnValue((tp, feeds))
