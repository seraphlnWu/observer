# coding=utf8

import re
import os
import json
import time
import random
import socket
import lxml.etree
from datetime import timedelta
from datetime import datetime
from cookielib import CookieJar

from twisted.internet.defer import inlineCallbacks, returnValue

from observer.utils.http import request, TimedAgentPool, InfiniteLoginError
from observer.utils import wait
from observer.platform.sina.weibo.sweibo.utils import getAgent, url_decode
from observer.node.client import ClientServiceBase
from observer import log

ONE_HOUR = timedelta(seconds=3600)

WEIBO_FEED_PATTERN = re.compile(
    '<script>[^\n]*FM.view[ \t]*\\((?P<js>[^\n]*)\\)[^\n]*</script>')
WEIBO_COUNT_PATTERN = re.compile('\\((?P<count>\\d+)\\)')


class NodeService(ClientServiceBase):
    ''' sweibo fetcher node '''

    servicename = 'observer.sina.weibo.user_statuses_active_spider'

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
    def getContent(self, agent, uid, page=1):
        ''' '''
        url = 'http://weibo.com/p/100505%s/weibo?is_search=0&visible=0&is_tag=0&profile_ftype=1&page=%s#feedtop' % (int(uid), page)
        result = yield request(agent, url)
        returnValue(result)

    @inlineCallbacks
    def getAjaxContent(self, agent, uid, page, max_id, end_id, page_bar):
        '''

        '''
        url = 'http://weibo.com/p/aj/mblog/mbloglist?domain=100505&pre_page=%s&page=%s&max_id=%s&end_id=%s&count=15&pagebar=%s&max_msign=&filtered_min_id=&pl_name=Pl_Official_LeftProfileFeed__14&id=100505%s&script_uri=/p/100505%s/weibo&feed_type=0&is_search=0&visible=0&is_tag=0&profile_ftype=1&__rnd=%s' % (page, page, max_id, end_id, page_bar, int(uid), int(uid), int(time.time()*1000))

        result = yield request(agent, url)
        returnValue(result)


    @staticmethod
    def getAjaxFeedsHtml(content):
        '''
        Ajax请求回来的微博会放在一个json object的data中
        '''
        return json.loads(content).get('data', '')


    @staticmethod
    def getFeedsHtml(content):
        """ 
        新浪用户结果使用js填充页面。
        填充内容在FM.view的调用参数里面，其中domid
        为 Pl_Core_OwnerFeed__3 的调用中，
        字典里的html对应的键值对保存了要现实的html内容。
        """
        JS_DOM_DOMAIN = ['Pl_Core_OwnerFeed__3', 'Pl_Official_LeftProfileFeed__14']
        for m in WEIBO_FEED_PATTERN.finditer(content):
            d = json.loads(m.group('js'))
            if d['domid'] in JS_DOM_DOMAIN:
                return d['html']
        return None

    @staticmethod
    def getText(el):
        '''
        提取微博正文中的文本
        '''
        has_link = False

        text = ''
        if el.tag == 'img' and el.get('type', None) == 'face':
            text += el.get('alt', '')
        elif el.tag == 'a' and el.get('mt', '') == 'url':
            has_link = True

        t = el.text
        if t is not None:
            text += t.replace(u'&nbsp;', u' ')
        for e in el.iterchildren():
            t, hl = NodeService.getText(e)
            text += t
            has_link = has_link or hl
        t = el.tail

        if t is not None:
            text += t.replace(u'&nbsp;', u' ')

        return text, has_link


    @staticmethod
    def parseTextContent(el):
        '''
            微博文字内容中包含的信息
            uname: 用户名
            isV: 是否加V
            haslink: 是否有链接
            uid: 用户id
            text: 内容
        '''
        d = {}
        d['text'], d['haslink'] = NodeService.getText(el.xpath("./div[@class='WB_text']")[0])
        d['text'] = d['text'].strip()
        d['pin'] = bool(el.xpath("./div[@class='WB_text']/a/span[@class='W_ico20 ico_feedpin']"))
        try:
            d['uname'] = unicode(el.xpath("./div[@class='WB_text']/@nick-name")[0])
        except:
            try:
                d['uname'] = unicode(el.xpath("./div[@class='WB_media_expand SW_fun2 S_line1 S_bg1']/div[@node-type='feed_list_forwardContent']/div[@class='WB_info']/a[@node-type='feed_list_originNick']/@nick-name")[0])
            except:
                try:
                    d['uname'] = unicode(el.xpath("./div[@class='WB_info']/a[@class='WB_name S_func3']/@nick-name")[0])
                except:
                    d['uname'] = ''

        return d

    @staticmethod
    def parsePic(el):
        '''
        提取图片信息
        '''
        e = el.xpath("./div[@class='WB_text']")[0]
        imgl = e.xpath("./ul[@node-type='feed_list_media_prev']/div[@action-type='feed_list_media_img']/img[@node-type='feed_list_media_bgimg']/@src")

        if len(imgl) == 0:
            return None

        return unicode(imgl[0])


    @staticmethod
    def getCount(text):
        ''' '''
        m = WEIBO_COUNT_PATTERN.search(text)
        if m:
            return int(m.group('count'))
        return 0


    @staticmethod
    def parseMiscInfo(el):
        '''
        提取微博的评论数，转发数以及发表时间和来源，从微博地址获取mid
        '''
        e = el.xpath(".//div[@class='WB_from']")[0]
        d = {
            'scmt': 0,
            'srpt': 0,
            'like': 0,
            'src': '',
            'favourite': 0,
        }

        d['ct'] = datetime.fromtimestamp(int(e.xpath(".//a[@node-type='feed_list_item_date']/@date")[0])/1000.)

        al = el.xpath("./div[@class='WB_func clearfix']/div[@class='WB_handle']/a")
        for a in al:
            if a.get('action-type', '') == 'fl_forward':
                d['srpt'] = NodeService.getCount(a.text)
            if a.get('action-type', '') == 'fl_comment':
                d['scmt'] = NodeService.getCount(a.text)
            if a.get('action-type', '') == 'fl_favorite':
                d['like'] = NodeService.getCount(a.text)
            '''
            if a.get('action-type', '') == 'fl_like':
                d['favourite'] = NodeService.getCount(a.text)
            '''

            d['interaction'] = d.get('srpt', 0) + d.get('scmt', 0)

        hrefl = e.xpath(".//a[@node-type='feed_list_item_date']/@href")[0]
        hrefl = hrefl.split("/")
        try:
            mid = url_decode(hrefl[-1])
            d['uid'] = int(hrefl[-2])
            d['src'] = unicode(e.xpath(".//a[last()]/text()")[0])
        except:
            pass

        return d, mid

    @staticmethod
    def parseMapData(el):
        '''
            parse map data info
        '''
        poi = {}
        e = el.xpath("./div[@class='map_data']")
        if e:
            e = e[0]
            l = e.xpath("./a[@action-type='feed_list_geo_info']/@action-data")[0]

            poi = dict(map(lambda x: x.split("="), l.split('&')))
            poi['geo'] = poi['geo'].split(',')

        return poi 

    @staticmethod
    def parseForwardContent(el):
        '''
            提取转发内容信息
        '''
        try:
            log.debug("Parsing forwared content...")
            e = el.xpath(".//div[@node-type='feed_list_forwardContent']")[0]
            md, mid = NodeService.parseMiscInfo(el)
            d = NodeService.parseTextContent(e)
            d['pic'] = NodeService.parsePic(e)
            poi = NodeService.parseMapData(el)
            d.update(md)
            if poi:
                d.update({'poi': poi})
            if poi.get('geo'):
                d.update({'geo': {'coordinates': poi.get('geo')}})
            d['id'] = int(mid)
            return d
        except:
            log.exception()
            return None

    @staticmethod
    def parseContent(el):
        '''
            提取内容信息
        '''
        md, mid = NodeService.parseMiscInfo(el)
        d = NodeService.parseTextContent(el)
        d['pic'] = NodeService.parsePic(el)
        poi = NodeService.parseMapData(el)
        d.update(md)
        if poi:
            d.update({'poi': poi})
        if poi.get('geo'):
            d.update({'geo': {'coordinates': poi.get('geo')}})

        return d

    @staticmethod
    def parseFeed(el):
        '''
        获取微博信息

        _id: 微博id
        scmt: 评论数
        srpt:  转发数
        ct:  创建时间
        imp:    impress
        haslink: 是否有链接
        pic:     内容中的图片
        text:    微博内容
        uid:     用户id
        lbs:     lbs 信息
        poi:     poi 信息
        src:  来源
        like:
        favourite:
        interaction: 互动 (comment + repost)
        retweet: {  # 被转发微博信息
            srpt:
            isV:
            uid:
            scmt:
            rsrc:
            date:
            text:
            pic:
            id:
            s_name:
        }

        upiurl:   用户头像
        seg:     分词
        top:     topic分词
        '''
        d = {
            'fcount': None,
            'imp': None,
            'seg': None,
            'topic': None,
        }

        try:
            mid = el.get('mid', None)
            if mid is not None:
                mid = int(mid)
            else:
                return None

            d['_id'] = mid
            d['id'] = mid

            ce = el.xpath(".//div[@class='WB_detail']")[0]
            d.update(NodeService.parseContent(ce))

            rl = ce.xpath(".//div[@node-type='feed_list_forwardContent']")
            if rl:
                d['retweet'] = {
                    'srpt': None,
                    'scmt': None,
                    'isV': None,
                    'uid': None,
                    'text': None,
                    'date': None,
                    'pic': None,
                    'id': None,
                    'sname': None,
                }
                e = rl[0].getparent()
                fd = NodeService.parseForwardContent(e)

                if fd is not None:
                    rd = d['retweet']
                    rd.update({
                        'rsid': fd['id'],
                        'srpt': fd['srpt'],
                        'scmt': fd['scmt'],
                        #'isV': fd['isV'],
                        'rsuid': fd['uid'],
                        'text': fd['text'],
                        'pic': fd['pic'],
                        'rsuname': fd['uname'],
                        'rsct': fd['ct'],
                        'rsrc': fd['src'],
                    })

            return d
        except:
            log.error("Error when parsing: " + lxml.etree.tostring(el))
            log.exception()
            return None

    @staticmethod
    def getFeeds(html_content):
        '''
            <div action-type="feed_list_item"> 下存放的是微博列表
        '''
        try:
            html = lxml.etree.HTML(html_content)
            el = html.xpath("//div[@action-type='feed_list_item']")
            feeds = filter(
                lambda e: e is not None,
                map(NodeService.parseFeed, el)
            )
        except:
            feeds = []

        return feeds

    @inlineCallbacks
    def search(self, uid, agent):
        ''' '''
        data = []
        for i in range(1, 4):
            _, feeds = yield self._search(uid, agent, i)
            if feeds:
                data.extend(feeds)
        returnValue(data)

    @inlineCallbacks
    def _search(self, uid, agent, page):
        ''' '''

        log.debug("Fetched_uid: %s." + str(uid))
        pin = None
        page_content = yield self.getContent(agent, uid, page)
        try:
            html_content = self.getFeedsHtml(page_content)
        except ValueError:
            returnValue((False, None))
            return
        if html_content is None:
            log.debug('got feed content: ' + page_content)

        data = self.getFeeds(html_content)
        if data:
            if data[0].get('pin'):
                pin = data.pop(0)

        ajax_data = []
        for j in range(2):
            try:
                if data:
                    ajax_content = yield self.getAjaxContent(agent, uid, page, data[-1]['id'], data[0]['id'], j)
                    ajax_data = self.getAjaxFeedsHtml(ajax_content)
                    ajax_data = self.getFeeds(ajax_data)
            except Exception as msg:
                print str(msg)

            data.extend(list(ajax_data))

        if pin is not None and isinstance(pin, dict):
            data.append(pin)

        returnValue((True, data))
