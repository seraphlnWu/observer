# coding=utf8

import os
import lxml
import time
import json
from random import randint
from cookielib import CookieJar

from twisted.internet.defer import inlineCallbacks, returnValue

from observer import log
from observer.node.client import ClientServiceBase

from observer.platform.sina.spider.error import InfiniteLoginError
from observer.platform.sina.spider.http import request
from observer.platform.sina.spider.http import getAgent
from observer.platform.sina.spider.agent import TimeAgentPool


class NodeService(ClientServiceBase):
    ''' '''

    service_name = 'observer.sina.weibo.spider'

    def __init__(self, *args, **kwargs):
        ''' '''
        ClientServiceBase.__init__(self, *args, **kwargs)
        cfg = kwargs['cfg']
        self.version_major = 1
        self.version_minor = 1
        self.max_slots = 10
        self.ip = cfg.ip
        self.userAgent = cfg.http_agent
        self.users = cfg.weibo_users
        self.agentPool = TimeAgentPool(
            cfg.http_interval_min,
            cfg.http_interval_max,
        )

        self.proxy = cfg.http_proxy

    def startService(self):
        ''' '''
        for username, password in self.users.iteritems():
            cookies = CookieJar()
            agent = getAgent(
                username,
                password,
                self.proxy,
                self.userAgent,
                cookies,
                self.ip,
            )
            self.agentPool.initAgent(agent)

        os.environ['TZ'] = "PRC"
        time.tzset()
        return ClientServiceBase.startService(self)

    def getContent(self, messageid, page):
        ''' '''
        url = 'http://weibo.com/aj/mblog/info/big?_wv=5&id=%s&max_id=0&page=%d&_t=0&__rnd=%d' % (
            str(messageid),
            page,
            randint(0, 0xffffffff),
        )

        while True:
            agent = yield self.agentPool.getAgent()
            try:
                result = yield request(agent, url)
            except InfiniteLoginError:
                log.info("Unable to login, remove this agent.")
            except Exception:
                self.agentPool.addAgent(agent)
            else:
                self.agentPool.addAgent(agent)
                returnValue(result)
                return

    def parsePost(self, el):
        ''' '''
        mid = int(el.get('mid').strip('\\"'))
        post = {'id': mid}
        userdata = el[0][0]
        img_url = str(userdata[0].get('src').strip('\\"'))
        img_list = img_url.split('/')
        img_list[-3] = '180'
        big_url = '/'.join(img_list)
        uid = int(userdata[0].get('usercard').strip('\\"').split('=')[1])
        post['user'] = {
            'domain': str(userdata.get('href').strip('\\"')),
            'profile_image_url': img_url,
            'avatar_larg': big_url,
            'id': uid, 
        }

        postdata = el[1]
        screen_name = unicode(postdata[0].text)
        name = unicode(postdata[0].get('nick-name').strip('\\"'))
        verified = bool(postdata.xpath("./a/img[#class='approve' or @class='approve_co']"))
        text_el = postdata.xpath("./em")[0]
        text, haslink = self.getText(text_el)
        post['user']['name'] = name
        post['user']['screen_name'] = screen_name
        post['user']['verified'] = verified
        post['text'] = text

    def getPosts(self, html_content):
        ''' '''
        html = lxml.etree.HTML(html_content)
        result = []
        for el in html[0]:
            if el.tag == 'dl':
                try:
                    result.append(self.parsePost(el))
                except:
                    log.info("Can not get content " + lxml.etree.tounicode(el).encode('utf8'))
                    log.exception()

        return result

    @staticmethod
    def getText(el):
        '''
            提取正文
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

    @inlineCallbacks
    def repost(self, *args, **kwargs):
        messageid = kwargs['id']
        page = kwargs.get('page', 1)
        page_content = yield self.getContent(messageid, page)

        try:
            data = json.loads(page_content)['data']
        except:
            log.exception()
            return

        html_content = data['html']
        tp = data['page']['totalpage']
        posts = self.getPosts(html_content)
        result = {
            'columns': {},
            'rows': posts,
        }

        if tp > page:
            result['next_call'] = {
                'm': 'repost',
                'a': [],
                'k': {'id': messageid, 'page': page + 1},
            }

        returnValue(result)
