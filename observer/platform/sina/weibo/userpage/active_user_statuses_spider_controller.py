# coding=utf8
#

import urllib
import urllib2
import time
import heapq
import pymongo
from observer import log

from twisted.internet import reactor
from observer.node.controller import ControllerServiceBase
from observer.node.errors import UnknownService

MONGO_HOST = 'store'
MONGO_PORT = 27017
MONGO_DBNAME = 'sandbox_mongo_1'

API_URL = 'http://people.bestminr.com/api/inner/update_statistic/'

def get_db():
    ''' '''
    conn = pymongo.Connection(MONGO_HOST, MONGO_PORT)
    db = conn[MONGO_DBNAME]
    return conn, db


class ControllerService(ControllerServiceBase):
    ''' '''

    servicename = 'observer.sina.weibo.user_statuses_active_spider'

    def __init__(self, *args, **kwargs):
        ''' '''
        ControllerServiceBase.__init__(self, *args, **kwargs)
        cfg = kwargs['cfg']

        self.dbhost = cfg.userdb_host           # user db host
        self.dbport = cfg.userdb_port           # user db port
        self.dbname = cfg.userdb_dbname         # user db name
        self.collection = cfg.collection_name   # user db collection

        self.db_uids_host = cfg.db_uids_host    # uid db host
        self.db_uids_port = cfg.db_uids_port    # uid db port
        self.db_uids_db = cfg.db_uids_dbname    # uid db name
        self.db_uids_collection = cfg.db_uids_collection # uid db coll

        self.min_priority = cfg.min_priority    # min keyword priority
        self.max_priority = cfg.max_priority    # max keyword priority

        self.clients = {}
        self.users = []
        self.uids = []                          # for uids to crawl

        self._make_uids_connection()

    def _check_mongodb(self):
        ''' '''
        now = time.time()
        if now - self.last_open_mongo > 300:
            self._make_uids_connection()

    def _make_uids_connection(self):
        ''' '''
        self.conn = pymongo.Connection(self.db_uids_host, self.db_uids_port)
        self.db = self.conn[self.db_uids_db]
        self.last_open_mongo = time.time()

    def startService(self):
        ''' '''
        self._check_mongodb()
        ControllerServiceBase.startService(self)
        self.refresh_users()
        self.refresh_uids()

    def refresh_uids(self):
        ''' '''
        self._check_mongodb()
        map(
            lambda x: heapq.heappush(self.uids, x.get('_id')),
            self.db[self.db_uids_collection].find()
        )

        reactor.callLater(10.0, self.refresh_uids)

    def getUser(self):
        ''' '''
        r = (None, None, None)
        try:
            r = heapq.heappop(self.users)
        except IndexError:
            pass
        return r

    def refresh_users(self):
        ''' '''
        # reload users from mongodb.
        # and maybe the page can config the user state
        users = []
        dbconn = pymongo.Connection(self.dbhost, self.dbport)
        db = dbconn[self.dbname]
        for i, user in enumerate(db[self.collection].find()):
            if not user.get('is_del', False):
                heapq.heappush(
                    users,
                    (user.get('name'), user.get('passwd'), i+1),
                )

        self.users = users
        dbconn.close()

    def stopService(self):
        '''
            stop this service and write data info files
            for the next call
        '''
        pass

    def gotResult(self, data, uid, clientid):
        ''' '''

        log.info("Uid: %s result: %s at %s" % (
            repr(uid),
            str(data),
            str(clientid),
        ))
        if data:
            #kafka_client = generate_kafka_client()
            #kafka_client.send_messages(json.dumps(feeds))
            #data.update({'_id': uid})
            conn, db = get_db()
            for d in data:
                print d.get('id')
                db.status.insert(d)
            conn.close()

            #FIXME call the remove server
            try:
                urllib2.urlopen(API_URL, data=urllib.urlencode({'uid': int(uid)})).read()
            except Exception as msg:
                print str(msg)

    def clientPull(self, clientid):
        ''' '''

        def _get_uid():
            if not self.uids:
                return None
            else:
                return heapq.heappop(self.uids)

        requestid = self.newRequestId()
        uid  = _get_uid()
        if not uid:
            return(None, None, None, {})
        else:
            result = (
                requestid,
                'search',
                [],
                {
                    'uid': uid,
                    'clientid': clientid,
                    't': time.time(),
                },
            )

            self.processing_queue[self.servicename].append(result)
            return(result)

    def remove_rid(self, requestid):
        #FIXME 
        self._check_mongodb()
        for r in self.processing_queue[self.servicename]:
            rid = r[0]
            if rid == requestid:
                self.processing_queue[self.servicename].remove(r)
            else:
                pass
        self.db[self.db_uids_collection].remove({'_id': r[3].get('uid')})

    def clientPush(self, requestid, name, *args, **kwargs):
        ''' '''
        servicename, method = self.splitName(name)

        if name not in self.versions:
            raise UnknownService("Unknown Service " + servicename)

        self.gotResult(
            kwargs.get('data', None),
            kwargs.get('uid', 0),
            kwargs.get('clientid'),
        )
        self.remove_rid(requestid)

    def clientFail(self, requestid, name, *args, **kwargs):
        ''' '''

        servicename, method = self.splitName(name)
        if name not in self.versions:
            raise UnknownService("Unknown Service " + servicename)

        log.info("%s Client Failed. Keyword: %s. reason: %s" % (
            kwargs.get('clientid'),
            repr(kwargs.get('keyword')),
            kwargs.get('reason', ''),
        ))
        self.remove_rid(requestid)
