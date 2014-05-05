# coding=utf8


class ControllerException(Exception):
    ''' '''

    def __init__(self, reason):
        ''' '''
        Exception.__init__(self, reason)


class UnknownService(Exception):
    ''' '''

    def __init__(self, reason):
        ''' '''
        Exception.__init__(self, reason)
