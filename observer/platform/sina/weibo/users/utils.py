# coding=utf8

from twisted.internet import reactor
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.web import client
from twisted.web.http_headers import Headers
from observer.utils.http import AdditionalHeaderAgent

REDIS_TASK_QUEUE = 'task_queue'
REDIS_EXTRACT_QUEUE = 'extract_queue'
REDIS_DUPLICATE_SET = 'extract_duplicate_set'
REDIS_TASK_DUPLICATE_SET = 'task_duplicate_set'


def getAgent(proxy, useragent):
    if proxy is None:
        agent = client.Agent(reactor)
    else:
        proxy_host, proxy_port = proxy
        endpoint = TCP4ClientEndpoint(reactor, proxy_host, proxy_port)
        agent = client.ProxyAgent(endpoint)
    agent = client.ContentDecoderAgent(agent, [('gzip', client.GzipDecoder)])
    agent = AdditionalHeaderAgent(agent, [('User-Agent', useragent)])
    return agent


def check_duplicate(r, target_ids):
    ''' '''
    result = []
    for uid in target_ids:
        if r.sismember(REDIS_DUPLICATE_SET, uid):
            pass
        else:
            result.append(uid)
            r.sadd(REDIS_DUPLICATE_SET, uid)

    return result

def save_extract_ids(r, fids):
    ''' '''
    for uid in fids:
        r.rpush(REDIS_EXTRACT_QUEUE, uid)
        if r.sismember(REDIS_TASK_DUPLICATE_SET, uid):
            pass
        else:
            r.rpush(REDIS_TASK_QUEUE, uid)
