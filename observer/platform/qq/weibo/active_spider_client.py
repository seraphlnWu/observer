# coding=utf8
#
# Insight Minr Active Spider Client Node
# CopyRight BestMiner Inc.
#

'''
    从本地load用户信息
    从controller节点加载任务信息
'''

from datetime import datetime
import random
import re
import os
import time
import socket
import lxml.etree
from heapq import heappop

from twisted.internet.defer import inlineCallbacks, returnValue

from utils import request
from utils import TimedAgentPool, InfiniteLoginError, getAgent
from observer.utils import wait
from observer.node.client import ClientServiceBase
from observer import log
from config import users
import pyvirtualdisplay

display = pyvirtualdisplay.Display(visible=False, size=(800,600))
display.start()

CREATE_GEREX = re.compile('\d+')
GEO_REGEX = re.compile(r'lbs.php\?lat=(.*)\&lng=(.*)&addr=(.*)')


class NodeService(ClientServiceBase):
    ''' '''

    servicename = 'observer.qq.weibo.status_active_spider'

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
        agent = getAgent(username, password)

        agent.remove = False
        agent.seq = seq
        log.debug('Init Agent')
        self.agentPool.initAgent(agent)
        log.debug('Searching Loop')
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
            while len(self.agentPool.agents) < self.max_agent:
                username, password, seq = heappop(users)
                if username is not None:
                    log.debug('Starting login with %s' % username)
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
            log.debug('Starting got a task')
            reqid, keyword, page, timescope, region, skid = yield self.callController('nextRequest')
            log.info("Got keyword %s from server" % skid)

            if reqid is None:
                pass
            else:
                try:
                    result = yield self.search(agent, keyword,
                                               page, timescope, region, skid)
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
        url = 'http://search.t.qq.com/index.php?k=%s&is_dup=1&pos=174&p=%d' % (keyword, page)
        log.debug('Starting request url: %s' % url)

        result = yield request(agent, url)
        returnValue(result)


    @staticmethod
    def parseContent(kid, content):
        ''' '''
        tps = False
        content = lxml.etree.HTML(content)
        feeds = content.xpath('//ul[@id="newHotList"]/li') + content.xpath('//ul[@id="talkList"]/li')
        statuses = filter(lambda x: x, map(lambda x: NodeService.parseFeed(kid, x), feeds))
        pages = content.xpath('//div[@class="main"]/div[@class="pageNav blueFoot"]/a[@class="pageBtn"]/text()')
        for p in pages:
            if p == u"下一页 >>":
                tps = True
            else:
                tps = False

        return statuses, tps

    @staticmethod
    def parseFeed(kid, fd):
        """ """
        status_dict = {'general': {'source': 'qqweibo', 'type': 'weibo'},
                       'original': {},
                       'language': {},
                       'demographic': {},
                       'augmentation': {}}
        sid = fd.get('id')
        if sid:
            status_dict.update({'kid': kid, '_id': sid, 'id': sid})

            # sid
            status_dict['general']['id'] = sid

            # user {name: 1, link: 1, avatar: 1}
            status_dict['general']['author'] = NodeService.parseUserPic(fd.xpath('./div[@class="userPic"]')[0])

            # user {verified: 1, verified_type: 1, gender: 1, username: 1}
            extra_user_info = NodeService.parseUserMsgBox(fd.xpath('./div[@class="msgBox"]')[0])
            status_dict['demographic']['gender'] = extra_user_info.pop('gender')
            status_dict['general']['author'].update(extra_user_info)

            # status {pic: 1, is_hot: 1, hotwords: 1, content: 1,
            #         hash_link: 1, source: 1, url: 1, create_at: 1,
            #         reads: 1, total_r_c: 1, repost: 1, comment: 1}
            user_msg = NodeService.parseMsgContent(fd)
            status_dict['general'].update(user_msg)
            status_dict['txt'] = status_dict.get('general', {}).get('txt', '')
            status_dict['ttype'] = 'qqweibo'

            retweet = fd.xpath('./div[@class="msgBox"]/div[@class="replyBox"]')

            if retweet:
                retweet_status = NodeService.parseForwardRetweet(retweet[0])
                status_dict['general']['retweet'] = retweet_status

        else:
            return

        return status_dict

    @staticmethod
    def parseForwardRetweet(el):
        ''' '''
        try:
            cnt = el.xpath('./div[@class="msgCnt"]')[0]
            user_pic = NodeService.parseUserPic(cnt)

            text, hash_link = NodeService.getText(cnt.xpath('./div')[0])
            user_box = NodeService.parseUserMsgBox(el.xpath('./div[@class="msgBox"]')[0])
        except:
            pass


    @staticmethod
    def parseUserPic(el):
        ''' '''
        ua = el.xpath('./a')[0]
        user = {
            'link': ua.get('href'),
            'name': ua.get('title'),
            'avatar': ua.xpath('./img')[0].get('src'),
        }

        return user

    @staticmethod
    def parseMsgContent(el):
        ''' '''
        status = {}
        status['content'], status['has_link'] = NodeService.getText(el.xpath('./div[@class="msgBox"]/div[@class="msgCnt"]')[0])
        extra_info = NodeService.parseMediaWrap(el)

        status.update(extra_info)
        return status


    @staticmethod
    def parseMediaWrap(el):
        ''' '''
        #FIXME videos!!!
        ohref, pic, video = None, None, None
        is_hot = False
        media = el.xpath('./div[@class="msgBox"]')[0]
        try:
            hotwords = NodeService.parseHotWords(media.xpath('./div[@class="hotWords"]')[0])
        except IndexError:
            hotwords = []

        try:
            ohref = media.xpath('./div[@class="clear multiMedia"]/div[@class="mediaWrap"]/div[@class="picBox"]/a')[0].get('href')
            pic = media.xpath('./div[@class="clear multiMedia"]/div[@class="mediaWrap"]/div[@class="picBox"]/a/img')[0].get('crs')
        except:
            pass

        extra_info = NodeService.parseExtraInfo(media.xpath('./div[@class="pubInfo c_tx5"]')[0])
        extra_info.update({'pic': pic, 'ohref': ohref})
        try:
            feeds_type = NodeService.parseFeedsType(media.xpath('./div[@class="feeds_type"]')[0])
            is_hot = True
            extra_info.update({'is_hot': True})
        except:
            pass

        extra_info['hotwords'] = hotwords
        return extra_info


    @staticmethod
    def parseFeedsType(el):
        ''' '''

    @staticmethod
    def parseUserMsgBox(el):
        ''' '''
        ud = {'verified': False, 'verified_type': '', 'vip': False, 'vip_type': ''}

        m = el.xpath('./div[@class="userName"]')[0].xpath('./strong/a')
        user = m[0]

        ud['gender'] = NodeService.parseGender(user.get('gender'))
        try:
            username = user.get('title').replace('(', '').replace(')', '')
            ud['username'] = username[:username.index('@')]
            ud['uid'] = username[username.index('@'):]
        except:
            log.debug('Meet a bug in parseUserMsgBox with username: ' + usrename)

        if len(m) > 1:
            for l in m[1:]:
                if l.get('class') in ['vip', 'tIcon ico_cvip', 'tIcon ico_master_star']:
                    ud['verified'] = True
                    ud['verified_type'] = l.get('title')

                if l.get('class') == 'ic _ic_vip ':
                    ud['vip'] = True
                    ud['vip_type'] = l.get('title')

        return ud

    @staticmethod
    def parseGender(gd):
        ''' '''
        g_dict = {u'他': 'm', u'她': 'f'}
        if gd in g_dict:
            return g_dict.get(gd)
        else:
            return 'N/A'

    @staticmethod
    def parseHotWords(el):
        ''' '''
        return el.xpath('./a/text()')

    @staticmethod
    def parseExtraInfo(el):
        ''' '''
        status = {}
        base_info = el.xpath('./span[@class="left c_tx5"]')[0]

        b = base_info.xpath('./a')[1]
        status['source'] = base_info.xpath('./a/i')[0].get('title')
        status['url'] = b.get('href')
        status['create_at'] = str(datetime(*map(int, CREATE_GEREX.findall(b.get('title')))))
        try:
            status['reads'] = CREATE_GEREX.findall(base_info.xpath('./span[@class="cNote"]')[0].get('title'))[0]
        except:
            status['reads'] = 0
        try:
            status['total_r_c'] = base_info.xpath('./a[@class="zfNum"]/b/text()')[0]
            try:
                status['total_r_c']  = int(status['total_r_c'])
            except:
                print '1'
        except:
            status['total_r_c'] = 0

        broad_info = el.xpath('./div[@class="funBox"]')[0]
        status['repost'] = broad_info.xpath('./a[@class="comt"]')[0].get('num')
        status['comment'] = broad_info.xpath('./a[@class="relay"]')[0].get('num')

        return status

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

    @inlineCallbacks
    def search(self, agent, keyword, page, timescope=None, region=None, skid=None):
        ''' '''
        skeyword = keyword.encode('utf8')
        log.info('skeyword: %s, page: %d' % (skeyword, page))
        page_content = yield self.getContent(agent, skeyword, page, timescope, region)
        feeds, tp = self.parseContent(skid, page_content)
        if tp:
            tps = page + 1
        else:
            tps = page
        log.info("GotFeeds: %d, page: %d and tps: %s" % (len(feeds), page, tps))
        returnValue((tps, feeds))
