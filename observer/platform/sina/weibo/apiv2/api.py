# coding=utf8

'''
Python client SDK for sina weibo API using OAuth 2 And Twisted.
'''

import time
import urllib
import base64
from observer.utils.http import getPage
from observer.utils import json_loads
from observer.platform.sina.weibo.apiv2.apiinfo import API_INFOS
from observer.platform.sina.weibo.apiv2 import error
from observer import log

from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web import error as web


_CONTENT_TYPES = {
    '.png': 'image/png',
    '.gif': 'image/gif',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.jpe': 'image/jpeg',
}

_HTTP_GET = 0
_HTTP_POST = 1
_HTTP_UPLOAD = 2


def b64decode(s):
    s=s+'='*(4-len(s)%4)
    return base64.urlsafe_b64decode(s)

def _guess_content_type(ext):
    return _CONTENT_TYPES.get(ext, 'application/octet-stream')


def _encode_multipart(**kw):
    '''
    Build a multipart/form-data body with generated random boundary.
    '''
    boundary = '----------%s' % hex(int(time.time() * 1000))
    data = []

    for k, v in kw.iteritems():
        data.append('--%s' % boundary)
        if hasattr(v, 'read'):
            # file-like object:
            ext = ''
            filename = getattr(v, 'name', '')
            n = filename.rfind('.')
            if n != (-1):
                ext = filename[n:].lower()
            content = v.read()
            data.append('Content-Disposition: form-data; name="%s"; filename="hidden"' % k)
            data.append('Content-Length: %d' % len(content))
            data.append('Content-Type: %s\r\n' % _guess_content_type(ext))
            data.append(content)
        else:
            data.append('Content-Disposition: form-data; name="%s"\r\n' % k)
            data.append(v.encode('utf-8') if isinstance(v, unicode) else v)
    data.append('--%s--\r\n' % boundary)
    return '\r\n'.join(data), boundary


def _http_get(url, authorization=None, **kw):
    print 'GET', url
    return _http_call(url, _HTTP_GET, authorization, kw)


def _http_post(url, authorization=None, **kw):
    print 'POST', url
    return _http_call(url, _HTTP_POST, authorization, kw)


def _http_upload(url, authorization=None, **kw):
    print 'MULTIPART POST', url
    return _http_call(url, _HTTP_UPLOAD, authorization, kw)


@inlineCallbacks
def _http_call(url, method, authorization, args):
    '''
    send an http request and expect to return a json object if no error.
    '''
    params = None
    boundary = None
    if method == _HTTP_UPLOAD:
        params, boundary = _encode_multipart(args)
    else:
        params = urllib.urlencode(args)

    http_url = '%s?%s' % (url, params) if method == _HTTP_GET else url
    http_body = None if method == _HTTP_GET else params
    headers = {}

    if authorization:
        headers['Authorization']='OAuth2 %s' % authorization
    if http_body is not None:
        headers['Content-Type']='application/x-www-form-urlencoded'
    if boundary:
        headers['Content-Type']='multipart/form; boundary=%s' % boundary

    try:
        if http_body is None:
            body = yield getPage(http_url,method='GET',headers=headers)
        else:
            body = yield getPage(
                http_url,
                method='POST',
                postdata=body,
                headers=headers,
            )
        r = json_loads(body)
        if 'error_code' in r:
            raise error.getErrorFromCode(
                r['error_code'],
                r.get('error'),
                r.get('request'),
            )
        returnValue(r)
    except web.Error, err:
        log.error('got error response from sina: ' + err.status)
        log.error(err.response)
        exc = error.getErrorFromResponse(err.response, False)
        if exc is not None:
            raise exc
        raise


class HttpObject(object):

    def __init__(self, client, method):
        self.client = client
        self.method = method

    def __getattr__(self, attr):
        def wrap(**kw):
            access_token=kw['access_token']
            if self.client.is_expires(access_token):
                return defer.fail(error.getErrorFromCode(
                    21327,
                    'Expired token',
                    attr),
                )
            del kw['access_token']
            return _http_call('%s%s.json' % (
                self.client.api_url,
                attr.replace('__', '/')),
                self.method,
                access_token,
                kw,
            )
        return wrap


class API(object):
    '''
    API client using synchronized invocation.
    '''
    def __init__(self):
        response_type='code'
        domain='api.weibo.com'
        version='2'
        self.response_type = response_type
        self.auth_url = 'https://%s/oauth2/' % domain
        self.api_url = 'https://%s/%s/' % (domain, version)
        self.access_tokens = {}
        self.users={}
        self.expires = {}
        self.get = HttpObject(self, _HTTP_GET)
        self.post = HttpObject(self, _HTTP_POST)
        self.upload = HttpObject(self, _HTTP_UPLOAD)

    def is_expires(self,access_token):
        return False #access_token not in self.users or time.time() > self.expires[access_token]

    def provides(self, name):
        return name in API_INFOS

    def need_probe(self, name):
        return API_INFOS[name].get('need_probe', True)

    def is_pageable(self, name):
        return 'page' in API_INFOS[name]

    def is_unmetered(self, name):
        return API_INFOS[name].get('unmetered', False)

    def next_page(self, name, args, kwargs, result, del_access_token):
        api_info = API_INFOS[name]
        page_info = api_info.get('page', None)
        if page_info is None:
            return None, None
        key = page_info['key']
        init = page_info['init']
        incr = page_info['incr']
        value = kwargs.get(key, init)
        old_value = value
        if isinstance(incr, str):
            value = result.get(incr)
        else:
            value += incr
        if value is None:
            return None, None
        if value <= old_value:
            return None, None
        kwargs[key] = value
        if del_access_token:
            try:
                del kwargs['access_token']
            except KeyError:
                pass
        return args, kwargs

    @staticmethod
    def proc_result(sina_result, apiinfo):
        payload_key = apiinfo.get('payload')
        if payload_key is None:
            rows = sina_result
        else:
            if not sina_result:
                raise error.getErrorFromCode(10001)
            rows = sina_result[payload_key]
        payload_type = apiinfo.get('payload_type', 'list')
        if payload_type == 'dict':
            rows = [rows]
        elif payload_type != 'list':
            log.warning('Unknown payload_type ' + payload_type)
            rows = [rows]
        return  {'columns': {},
                 'rows': rows}

    @inlineCallbacks
    def _request(self, name, *args, **kwargs):
        kwargs = kwargs.copy()
        method = getattr(self.get, name)
        del_access_token = kwargs.get('del_access_token', False)
        if del_access_token:
            del kwargs['del_access_token']
        for key, value in kwargs.iteritems():
            if isinstance(value, unicode):
                kwargs[key] = value.encode('utf-8')
        try:
            sina_result = yield method(*args, **kwargs)
            log.debug('got result: ' + repr(sina_result))
            next_args, next_kwargs = self.next_page(name, args, kwargs.copy(),
                                                    sina_result, del_access_token)
            result = self.proc_result(sina_result, API_INFOS[name])
            if next_args is not None or next_kwargs is not None:
                result['next_call'] = {'m': name,
                                       'a': next_args,
                                       'k': next_kwargs}
        except error.SinaServerRecordLimitReached:
            result = {'columns': {},
                      'rows': []}
        returnValue(result)

    def __getattr__(self, name):
        if name not in API_INFOS:
            raise AttributeError(
                "'%s' has no attribute '%s'" % (self.__class__.__name__,
                                                name))
        def __new_func(*args, **kwargs):
            return self._request(name, *args, **kwargs)

        return __new_func
