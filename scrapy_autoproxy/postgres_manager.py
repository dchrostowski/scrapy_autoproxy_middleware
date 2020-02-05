
from scrapy_autoproxy.config import config
from scrapy_autoproxy.proxy_objects import Proxy,Detail,Queue
from scrapy_autoproxy.exceptions import QueueMismatchException
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import sql
import time

DB_CONNECT_INTERVAL = config.settings['db_connect_attempt_interval']
MAX_DB_ATTEMPTS = config.settings['max_db_connect_attempts']
SEED_QUEUE_ID = config.settings['seed_queue_id']
AGGREGATE_QUEUE_ID = config.settings['aggregate_queue_id']
SEED_QUEUE_DOMAIN = "RESERVED_SEED_QUEUE"
AGGREGATE_QUEUE_DOMAIN = "RESERVED_AGGREGATE_QUEUE"
PROXY_INTERVAL = config.settings['proxy_interval']
LAST_USED_CUTOFF = datetime.utcnow() - timedelta(seconds=PROXY_INTERVAL)


class PostgresManager(object):
    def __init__(self):
        connect_params = config.db_config
        connect_params.update({'cursor_factory':DictCursor})
        self.connect_params = connect_params
        self.connect_attempts = 0

    def new_connection(self):
        try:
            conn = psycopg2.connect(**self.connect_params)
            conn.set_session(autocommit=True)
        except Exception as e:
            if self.connect_attempts > MAX_DB_ATTEMPTS:
                raise(e)

            self.connect_attempts +=1
            time.sleep(DB_CONNECT_INTERVAL)
            return self.new_connection()

        return conn

    def cursor(self):
        return self.new_connection().cursor()

    def do_query(self, query, params=None):
        cursor = self.cursor()
        cursor.execute(query,params)
        try:
            data = cursor.fetchall()
            return data
        except Exception:
            pass
        cursor.close()

    def init_seed_queues(self):
        seed_queue = Queue(domain=SEED_QUEUE_DOMAIN,queue_id=SEED_QUEUE_ID)
        agg_queue = Queue(domain=AGGREGATE_QUEUE_DOMAIN, queue_id=AGGREGATE_QUEUE_ID)


        query = "SELECT queue_id from queues WHERE domain = %(domain)s"
        db_seed = self.do_query(query, {'domain':SEED_QUEUE_DOMAIN})
        db_agg = self.do_query(query, {'domain': AGGREGATE_QUEUE_DOMAIN})
        
        
        if len(db_seed) == 0:
            self.insert_queue(seed_queue)
        elif db_seed[0]['queue_id'] != SEED_QUEUE_ID:
            raise ReservedQueueMismatch("Unexpected seed queue id in database. Expecting %s, but got %s." % (SEED_QUEUE_ID, db_seed[0]['queue_id']))
        
        if len(db_agg) == 0:
            self.insert_queue(agg_queue)
        elif(db_agg[0]['queue_id'] != AGGREGATE_QUEUE_ID):
            raise ReservedQueueMismatch("Unexpected aggregate queue id in database. Expecting %s, but got %s." % (AGGREGATE_QUEUE_ID, agg_seed[0]['queue_id']))

        cursor = self.cursor()
        query = """
        BEGIN;
        LOCK TABLE queues IN EXCLUSIVE MODE;
        SELECT setval('queues_queue_id_seq', COALESCE((SELECT MAX(queue_id)+1 FROM queues),1), false);
        COMMIT;
        """
        cursor.execute(query)
        cursor.close()


    def update_detail(self,obj,cursor=None):
        table_name = sql.Identifier('details')
        obj_dict = obj.to_dict()
        where_sql = sql.SQL("{0}={1}").format(sql.Identifier('detail_id'),sql.Placeholder('detail_id'))        

        if 'detail_id' not in obj_dict:
            if 'queue_id' not in obj_dict or 'proxy_id' not in obj_dict:
                raise Exception("cannot update detail without a detail id, queue id, or proxy id")
            where_sql = sql.SQL("{0}={1} AND {2}={3}").format(sql.Identifier('queue_id'),sql.Placeholder('queue_id'),sql.Identifier('proxy_id'),sql.Placeholder('proxy_id'))        
            
            

        set_sql = sql.SQL(', ').join([sql.SQL("{0}={1}").format(sql.Identifier(k),sql.Placeholder(k)) for k in obj_dict.keys()])
        update = sql.SQL('UPDATE {0} SET {1} WHERE {2}').format(table_name,set_sql,where_sql)
        if cursor is not None:
            cursor.execute(update,obj.to_dict())
        else:
            self.do_query(update,obj.to_dict())

    def insert_object(self,obj,table,returning, cursor=None):
        table_name = sql.Identifier(table)
        column_sql = sql.SQL(', ').join(map(sql.Identifier, obj.to_dict().keys()))
        placeholder_sql = sql.SQL(', ').join(map(sql.Placeholder,obj.to_dict()))
        returning = sql.Identifier(returning)
        
        insert = sql.SQL('INSERT INTO {0} ({1}) VALUES ({2}) RETURNING {3}').format(table_name,column_sql,placeholder_sql, returning)


        if cursor is not None:
            cursor.execute(insert,obj.to_dict())
        else:
            self.do_query(insert,obj.to_dict())

    def insert_detail(self,detail, cursor=None):
        self.insert_object(detail,'details', 'detail_id',cursor)

    def insert_queue(self,queue, cursor=None):
        self.insert_object(queue, 'queues','queue_id',cursor)
    
    def insert_proxy(self,proxy,cursor=None):
        self.insert_object(proxy,'proxies','proxy_id',cursor)

    def init_seed_details(self):
        seed_count = self.do_query("SELECT COUNT(*) as c FROM details WHERE queue_id=%(queue_id)s", {'queue_id':SEED_QUEUE_ID})[0]['c']

        cursor = self.cursor()
        if seed_count == 0:
            proxy_ids = [p['proxy_id'] for p in self.do_query("SELECT proxy_id FROM proxies")]
            for proxy_id in proxy_ids:
                insert_detail = "INSERT INTO details (proxy_id,queue_id) VALUES (%(proxy_id)s, %(queue_id)s);"
                params = {'proxy_id': proxy_id, 'queue_id': SEED_QUEUE_ID}
                cursor.execute(insert_detail,params)

        
        query = """
        BEGIN;
        LOCK TABLE details IN EXCLUSIVE MODE;
        SELECT setval('details_detail_id_seq', COALESCE((SELECT MAX(detail_id)+1 FROM details),1), false);
        COMMIT;
        """
        
        cursor.execute(query)
        cursor.close()

    def get_unused_proxy_ids(self,queue,count,excluded_pids):
        query = None
        excluded_pids.append(-1)
        excluded_pids.append(-2)

        params = {
            'seed_queue_id': SEED_QUEUE_ID,
            'limit': count,
            'excluded_pids': tuple(excluded_pids)
        }
        
        if queue.id() is None:
            query = """
                SELECT proxy_id FROM details 
                WHERE queue_id = %(seed_queue_id)s
                AND proxy_id NOT IN %(excluded_pids)s
                ORDER BY RANDOM()
                LIMIT %(limit)s
                """
            
        else:
            params['queue_id'] = queue.id()
            query = """
                SELECT proxy_id FROM details 
                WHERE queue_id = %(seed_queue_id)s 
                AND proxy_id NOT IN ( SELECT proxy_id FROM details WHERE queue_id = %(queue_id)s )
                AND proxy_id NOT IN %(excluded_pids)s
                ORDER BY RANDOM()
                LIMIT %(limit)s
                """

        pids = [pid[0] for pid in self.do_query(query,params)]
        return pids

    def get_queues(self):
        self.init_seed_queues()
        return [Queue(**r) for r in self.do_query("SELECT * FROM queues;")]
        

    def get_proxies(self):
        return [Proxy(**p) for p in self.do_query("SELECT * FROM proxies")]

    def get_non_seed_details(self,queue_id):
        if queue_id is None:
            return []
        query= """
            SELECT * FROM details 
            WHERE queue_id = %(queue_id)s
            AND active=%(active)s
            AND last_used < %(last_used_cutoff)s
            ORDER BY last_used ASC
            LIMIT %(limit)s;
            """
        
        active_params = { 
            'queue_id': queue_id,
            'active': True,
            'last_used_cutoff': LAST_USED_CUTOFF,
            'limit': ACTIVE_LIMIT
        }

        inactive_params = {
            'queue_id': queue_id,
            'active': False,
            'last_used_cutoff': LAST_USED_CUTOFF,
            'limit': INACTIVE_LIMIT
        }

        active = [Detail(**d) for d in self.do_query(query, active_params)]
        
        inactive = [Detail(**d) for d in self.do_query(query, inactive_params)]


        return active + inactive

        
        