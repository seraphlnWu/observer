# coding=utf8

import os
from config import LOG_PATH
from datetime import datetime
import re
import time
import json
import random
from hashlib import sha1
from base64 import b64encode
from urllib import quote_plus, urlencode

from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.web import client
from twisted.web.http_headers import Headers
from observer.utils import wait, log, hex_encode_bytes
from observer.utils.http import (
    request,
    AdditionalHeaderAgent,
    LoginAgent,
)

from configurations import node as configuration
from kafka import KafkaClient
from kafka.producer import SimpleProducer
from base_redis import RedisOp


__es = ''.join(reduce(lambda x, y: x + y, (
    map(chr, range(ord('0'), ord('9') + 1)),
    map(chr, range(ord('a'), ord('z') + 1)),
    map(chr, range(ord('A'), ord('Z') + 1)),
)))
__de = dict((k, i) for i, k in enumerate(__es))

ONE_DAY = 24 * 3600


def url_decode(s):
    s = s.split('?')[0]
    r = 0
    l = len(s)
    b = l % 4
    if b == 0:
        b = 4
    po = 0
    for o in xrange(b, l + 1, 4):
        r *= 10000000
        r += reduce(lambda a, b: a * 62 + __de[b], s[po:o], 0)
        po = o
    return r


def url_encode(n):
    r = ''
    while n:
        t = n % 10000000
        n = n / 10000000
        for i in range(4):
            r = __es[t % 62] + r
            t = t / 62
    return r.lstrip('0')

WEIBO_LOGIN_SUCCEED_PATTERN = re.compile('try\\{sinaSSOController.setCrossDomainUrlList\\((?P<js>\\{[^\\}]*\\})\\).*catch.*sinaSSOController.crossDomainAction\\(.*location.replace\\(\'(?P<url>[^\']*)\'\\);\\}\\);\\}')
WEIBO_NEED_LOGIN_PATTERN = re.compile("\\$CONFIG\\['islogin'\\]\\s*=\\s*'0';")
#('action-type="login"')
WEIBO_REDIRECT_PATTERN = re.compile('location\\.replace\\("(?P<url>[^"]*)"\\)')


def getAgent(username, password, proxy, useragent, cookies):
    log.debug('proxy: ' + repr(proxy))
    if proxy is None:
        log.debug('direct connection')
        agent = client.Agent(reactor)
    else:
        log.debug('proxy connection')
        proxy_host, proxy_port = proxy
        endpoint = TCP4ClientEndpoint(reactor, proxy_host, proxy_port)
        agent = client.ProxyAgent(endpoint)
    agent = client.ContentDecoderAgent(agent, [('gzip', client.GzipDecoder)])
    agent = client.CookieAgent(agent, cookies)
    agent = client.RedirectAgent(agent)
    agent = AdditionalHeaderAgent(agent, [('User-Agent', useragent)])
    agent = SinaLoginAgent(agent, username, password)
    return agent


class SinaLoginAgent(LoginAgent):

    @staticmethod
    def encodeUsername(username):
        return b64encode(quote_plus(username))

    @staticmethod
    def rsaEncodePassword(password, servertime, nonce, pubkey):
        key = RSA.construct((pubkey, 0x10001L))
        cipher = PKCS1_v1_5.new(key)
        r = '%s\t%s\n%s' % (str(servertime), str(nonce), password)
        ciphertext = cipher.encrypt(r.encode('utf8'))
        return hex_encode_bytes(ciphertext)

    @staticmethod
    def wsseEncodePassword(password, servertime, nonce):
        h = lambda s: sha1(s).hexdigest()
        return h(h(h(password)) + str(servertime) + nonce)

    def __init__(self, agent, username, password, retryLimit=1):
        self.username = username
        self.password = password
        self.loggedin = False
        self.pool = None
        LoginAgent.__init__(self, agent, retryLimit)

    @inlineCallbacks
    def _prelogin(self):
        log.debug('begin _prelogin')
        content = yield request(
            self._agent,
            'http://login.sina.com.cn/sso/prelogin.php?entry=miniblog&su=%s&rsakt=mod' % SinaLoginAgent.encodeUsername(self.username),
        )
        d = json.loads(content)
        log.debug('end _prelogin')
        returnValue((d['servertime'],
                     d['nonce'].encode('utf8'),
                     int(d['pubkey'], 16),
                     d['rsakv']))

    @inlineCallbacks
    def _login(self, d):
        content = yield request(
            self._agent,
            'http://login.sina.com.cn/sso/login.php?entry=weisousuo',
            Headers({'Content-Type': ['application/x-www-form-urlencoded']}),
            urlencode(d))
        log.debug('got login.php: ' + content)
        m = WEIBO_LOGIN_SUCCEED_PATTERN.search(content)
        if m is None:
            m = WEIBO_REDIRECT_PATTERN.search(content)
        if m:   
            yield request(self._agent, m.group('url'))
            self.loggedin = True
        else:   
            self.loggedin = False

    @inlineCallbacks
    def login(self):
        if self.pool is None:
            yield wait(random.uniform(10.0, 20.0))
        else:
            now = time.time()
            login_time = self.pool.lastLogin + self.pool.loginInterval
            self.pool.lastLogin = max(now, login_time)
            if now < login_time:
                yield wait(login_time - now)
        servertime, nonce, pubkey, rsakv = yield self._prelogin()
        log.debug('got _prelogin: %d,%s,%d,%s' % (
            servertime,
            nonce,
            pubkey,
            rsakv,
        ))
        d = {
            'su': self.encodeUsername(self.username),
            'sp': self.rsaEncodePassword(
                self.password,
                servertime,
                nonce,
                pubkey,
            ),
            'url': 'http://weibo.com/login.php?url=http%3A%2F%2Fwww.weibo.com%2F',
            'returntype': 'META',
            'encoding': 'utf-8',
            'pwencode': 'rsa2',
            'servertime': servertime,
            'nonce': nonce,
            'gateway': 1,
            'rsakv': rsakv,
        }
        log.debug('processing: ' + repr(d))
        yield self._login(d)

    @inlineCallbacks
    def testLogin(self, content):
        m = WEIBO_REDIRECT_PATTERN.search(content)
        if m:
            new_url = m.group('url')
            content = yield request(self._agent, new_url)
        if WEIBO_NEED_LOGIN_PATTERN.search(content):
            log.debug('need to login: '+content)
            self.loggedin = False
            returnValue(None)
            return
        self.loggedin = True
        returnValue(content)


def check_exists(f):
    '''
    过滤微博是否已经抓过了 

    @feeds -> 微博列表
    '''
    r = RedisOp()
    tid = '%s_%s' % (f.get('keyid'), f.get('id'))
    if r.check_set_exists('store_set', tid):
        pass
    else:
        r.push_set_data('store_set', tid)
        return f 



def generate_kafka_client(topic):
    '''
    生成一个kafka的连接 
    :param topic -> 指定kafka的队列名

    :return -> (Producer, kafka链接)
    '''
    # FIXME batch 应该可以提高性能
    kafka = KafkaClient(configuration.kafka_host, configuration.kafka_port)
    producer = SimpleProducer(kafka, topic)
    return (producer, kafka)


def send_messages(kid, keyword, feeds):
    ''' '''
    feeds = filter(lambda x: x, feeds)
    now = datetime.now()
    # 生成kafka链接
    #kafka_client, kafka = generate_kafka_client(kid)
    print "Got new %d statuses" % len(feeds)
    for feed in feeds:
        # 首先写入文件日志
        try:
            pname = os.path.join(LOG_PATH, feed.get('kid'))
            if not os.path.isdir(pname):
                os.mkdir(pname)

            fname = os.path.join(pname,
                                 "%s_%s%s" % (keyword, now.year, now.month))
            with open(fname, 'a') as fp:
                fp.write(json.dumps(feed)+'\n')
        except:
            log.exception()
        finally:
            # 发送kafka消息
<<<<<<< HEAD
            pass
            #kafka_client.send_messages(json.dumps(feed))
    #kafka.close()
=======
            kafka_client.send_messages(json.dumps(feed))
    kafka.close()


def dump_to_redis(update_dict):
    ''' '''
    r = RedisOp()
    now = datetime.now()
    for kid, val in update_dict.iteritems():
        kid = '%s_%s%s%s%s%s' % (kid, now.year, now.month, now.day, now.hour, now.minute)
        r.push_set_data(kid, val)
        r.set_expire(kid, ONE_DAY)
>>>>>>> 0faac31cc242fa41102ab68ba11b8aebfbe6a53c
