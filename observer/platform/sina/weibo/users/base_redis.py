# coding=utf8
from configurations.mainnode import REDIS_HOST, REDIS_PORT

from redis import StrictRedis
from redis import ConnectionPool

r = StrictRedis(host=REDIS_HOST, port=REDIS_PORT)

class RedisOp(object):
    ''' the basic redis operate class '''
    def __init__(self, host=REDIS_HOST, port=REDIS_PORT):
        self.host = host
        self.port = port
        self.redis_db = r

    def open_connection(self):
        ''' open the redis connection '''
        pool = ConnectionPool(host=self.host, port=self.port)
        return StrictRedis(connection_pool=pool)

    def query_set_data(self, key_name):
        ''' query data with the given table name '''
        return self.redis_db.spop(key_name)

    def push_set_data(self, key_name, o_value):
        ''' push data to a set '''
        return self.redis_db.sadd(key_name, o_value)

    def check_set_exists(self, key_name, o_value):
        ''' check the given o_value if in the given set '''
        return self.redis_db.sismember(key_name, o_value)

    def get_set_length(self, key_name):
        ''' get the given set length '''
        return self.redis_db.scard(key_name)

    def query_list_data(self, key_name, wait_block=True, timeout=0.01):
        ''' query data from the given list '''
        result = []
        if wait_block:
            result = self.redis_db.blpop(key_name, timeout=timeout)
        else:
            result = self.redis_db.lpop(key_name)

        return result

    def push_list_data(self, key_name, o_value, direct='left'):
        ''' push a data object into the given list '''
        if direct == 'left':
            self.redis_db.lpush(key_name, o_value)
        elif direct == 'right':
            self.redis_db.rpush(key_name, o_value)
        else:
            pass

    def check_list_exists(self, key_name, o_value):
        ''' check the given value if exists in the given list '''
        pass

    def get_list_length(self, key_name):
        ''' get the given list length '''
        return self.redis_db.llen(key_name)

    def set_expire(self, name, value):
        ''' set expire time '''
        return self.redis_db.expire(name, value)
