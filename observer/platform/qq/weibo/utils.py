# coding=utf8
#

from twisted.internet import reactor, task
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred
import random
import re
import time
from observer.utils import wait
from observer import log
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.remote.errorhandler import TimeoutException


LOGIN_URL = 'http://ui.ptlogin2.qq.com/cgi-bin/login?appid=46000101&&hide_title_bar=1&hide_close_icon=1&self_regurl=http%3A%2F%2Freg.t.qq.com/index.php&s_url=http%3A%2F%2Ft.qq.com'
CAPTCHA_RE = re.compile(r'captcha\.qq\.com')


def get_browser():
    browser = webdriver.Firefox()
    return browser


class InfiniteLoginError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message

    def __str__(self):
        return self[0]


class NoAgentError(Exception):

    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message

    def __str__(self):
        return self[0]


def getAgent(username, password):
    ''' '''

    browser = QQLoginAgent(get_browser(), username, password)
    return browser


class QQLoginAgent(object):
    ''' '''

    def __init__(self, browser, username, password):
        ''' '''
        self.browser = browser
        self.username = username
        self.password = password
        self.logined = False
        self.nextAccess = None
        self.pool = None
        self.seq = None
        self.remove = False

    @inlineCallbacks
    def login(self):
        ''' login with selenium webdriver '''
        log.debug('starting login')
        if self.pool is None:
            yield wait(random.uniform(8.0, 10.0))
        else:
            now = time.time()
            login_time = self.pool.lastLogin + self.pool.loginInterval
            self.pool.lastLogin = max(now, login_time)
            if now < login_time:
                yield wait(login_time - now)

        self.browser.get(LOGIN_URL)
        # 使用selenium登陆腾讯微博
        elem = self.browser.find_element_by_id('u')
        elem.send_keys(self.username)
        elem = self.browser.find_element_by_id('p')
        elem.send_keys(self.password)

        elem = self.browser.find_element_by_id('imgVerify')
        need_verify = elem.get_attribute('src')

        if need_verify is not None and CAPTCHA_RE.search(need_verify):
            # 如果需要验证码，换一个帐号
            # FIXME 可以考虑人工输入验证码的部分
            log.debug(repr(self.username) + 'need verify')
            returnValue(False)
            return
        else:
            elem = self.browser.find_element_by_id('login_button')
            elem.send_keys(Keys.RETURN)

        yield wait(5)
        try:
            e = self.browser.find_element_by_id('err_m')
            if e.text:
                log.debug('Got error %s with user: %s' % (repr(e.text),
                                                          self.username))
                returnValue(None)
                return
        except NoSuchElementException:
            pass

        log.debug('Finished login with user: %s' % self.username)
        self.logined = True
        returnValue(True)

def request(agent, url):
    ''' '''
    result = ''
    if not agent.logined:
        d = agent.login()

    time.sleep(5)
    try:
        agent.browser.get(url)
        result = agent.browser.page_source
    except TimeoutException:
        log.error('Agent %s got a timeout with url: %s' % (repr(agent), url))

    return result

class TimedAgentPool(object):

    def __init__(self, minTimeInterval=10.0, maxTimeInterval=15.0,
                 loginInterval=60.0):
        self.minTimeInterval = minTimeInterval
        self.maxTimeInterval = maxTimeInterval
        self.loginInterval = loginInterval
        self.lastLogin = 0.0
        self.agents = []
        self.idleAgents = []
        self.defers = []

    def initAgent(self,agent):
        self.agents.append(agent)
        self.idleAgents.append(agent)
        agent.nextAccess = 0
        agent.pool = self

    def addAgent(self, agent):
        t = random.uniform(self.minTimeInterval,
                           self.maxTimeInterval)
        agent.nextAccess = time.time() + t
        if self.defers:
            d = self.defers[0]
            del self.defers[0]
            task.deferLater(reactor, t, d.callback, agent)
        else:
            self.idleAgents.append(agent)

    @inlineCallbacks
    def getAgent(self):
        if not self.agents:
            raise NoAgentError('This pool has no agent yet.')
        if not self.idleAgents:
            d = Deferred()
            self.defers.append(d)
            agent = yield d
        else:
            agent = self.idleAgents[0]
            del self.idleAgents[0]
        now = time.time()
        if now > agent.nextAccess:
            returnValue(agent)
            return
        else:
            yield wait(agent.nextAccess - now)
            returnValue(agent)
            return

    def removeAgent(self, agent):
        self.agents.remove(agent)


def send_messages(kid, keyword, fds):
    ''' '''
    pass

def check_exists(feed):
    ''' '''
    pass

