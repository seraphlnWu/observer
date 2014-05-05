# coding=utf8


class InfiniteLoginError(Exception):
    ''' '''

    def __init__(self, message):
        ''' '''
        Exception.__init__(self, message)
        self.message = message

    def __str__(self):
        ''' '''
        return self[0]


class NoAgentError(Exception):
    ''' '''

    def __init__(self, message):
        ''' '''
        Exception.__init__(self, message)
        self.message = message

    def __str__(self):
        ''' '''
        return self[0]