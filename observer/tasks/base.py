# coding=utf8

import time


class Task(object):
    ''' '''

    def __init__(self, ttype='sinaweibo', ttime=time.time(), tcontext={}):
        ''' '''
        self.ttype = ttype
        self.ttime = ttime
        self.tcontext = tcontext