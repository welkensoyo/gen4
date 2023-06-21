import logging
import traceback
from contextlib import contextmanager

import gevent
import psycopg2
import random
import sys
from gevent.queue import Queue
from gevent.socket import wait_read, wait_write
from psycopg2 import extensions, OperationalError
from psycopg2.pool import ThreadedConnectionPool

from API.config import psqldsn
from API.njson import jc

dsn = psqldsn

if sys.version_info[0] >= 3:
    integer_types = (int,)
else:
    import __builtin__
    integer_types = (int, __builtin__.long)

log = logging.getLogger(__name__)
poolsize = 3
_pgpool = None


def fetchone(PSQL, *args): return Pcursor().fetchone(PSQL, *args)
def fetchreturn(PSQL, *args): return Pcursor().fetchreturn(PSQL, *args)
def fetchall(PSQL, *args): return Pcursor().fetchall(PSQL, *args)
def execute(PSQL, *args): return Pcursor().execute(PSQL, *args)
def executemany(PSQL, args): return Pcursor().executemany(PSQL, args)
def post(tablename, constraint, **kw): return Pcursor().post(tablename, constraint, **kw)
def get(tablename, *keys, **where): return Pcursor().get(tablename, *keys, **where)
def logs(clientid, meta, origin): return Pcursor().logs(clientid, meta, origin)
def set_dsn(*args):
    l = len(args)
    if l>6:
        _DSN.new(*args)
    elif l==2:
        _DSN(args[0],args[1])
    else:
        _DSN(args[0], poolsize)

class PostgresError(Exception):
    """ This is a base class for all POSTGRES related exceptions """
    pass

#singleton connection pool, gets reset if a connection is bad or drops
def _pgcon():
    global _pgpool
    if not _pgpool:
        try:
            if dsn:
                _pgpool = PostgresConnectionPool(dsn, maxsize=poolsize)
            else:
                raise PostgresError('No DSN was assigned, use set_dsn(<host>, <port>, <database>, <username>, <password>, <timeout>, <application_name>, <number of connections>) ')
        except psycopg2.OperationalError as exc:
            log.warning(exc)
            _pgpool = None
    return _pgpool

#sets DSN and re-establishes connection
class _DSN:
    def __init__(self, psqldsn, pool):
        global dsn
        global poolsize
        global _pgpool
        dsn = psqldsn
        poolsize = pool
        _pgpool = None
        _pgcon()

    @classmethod
    def new(cls, host, port, database, username, password, timeout, application_name, pool=10):
        dsn = f"host='{host}' port='{port}' dbname='{database}' user='{username}' password='{password}' connect_timeout={timeout} application_name='{application_name}'"
        return cls(dsn, pool)

class Pcursor:
    def __init__(self, **kwargs):
        # Auto Reconnect
        global _pgpool
        if not _pgpool:
            log.info('Attempting Connection To Postgres...')
            while not _pgpool:
                try:
                    _pgcon()
                    log.info('Connection To Postgres Established')
                except PostgresError:
                    raise
                except:
                    gevent.sleep(0.5)

    def fetchone(self, PSQL, *args):
        with _pgpool.cursor() as cursor:
            try:
                cursor.execute(PSQL, args)
            except TypeError:
                cursor.execute(PSQL, args[0])
            except Exception as exc:
                print(sys._getframe().f_back.f_back.f_code)
                print(sys._getframe().f_back.f_back.f_code.co_name)
                print(str(exc))
                print(traceback.print_exc())
            log.debug(cursor.query)
            return cursor.fetchone()

    def fetchreturn(self, PSQL, *args):
        with _pgpool.cursor() as cursor:
            try:
                cursor.execute(PSQL, args)
            except TypeError as exc:
                cursor.execute(PSQL, args[0])
            except Exception as exc:
                print(str(exc))
                print(traceback.print_exc())
            x = cursor.fetchone()
            if x:
                return x[0]
            return x

    def fetchall(self, PSQL, *args):
        with _pgpool.cursor() as cursor:
            try:
                cursor.execute(PSQL, args)
            except TypeError:
                cursor.execute(PSQL, args[0])
            except Exception as exc:
                print(str(exc))
                print(traceback.print_exc())
            log.debug(cursor.query)
            return cursor.fetchall()

    def execute(self, PSQL, *args):
        with _pgpool.cursor() as cursor:
            try:
                cursor.execute(PSQL, args)
            except TypeError:
                cursor.execute(PSQL, args[0])
            except Exception as exc:
                # log.warning(sys._getframe().f_back.f_code)
                # log.warning(sys._getframe().f_back.f_code.co_name)
                # log.warning(str(exc))
                print(str(exc))
                print(traceback.print_exc())
            finally:
                log.debug(cursor.query)
                return cursor.query

    def executemany(self, PSQL, args):
        with _pgpool.cursor() as cursor:
            try:
                cursor.executemany(PSQL, args)
                print(cursor.query)
            except Exception as exc:
                # log.warning(sys._getframe().f_back.f_code)
                # log.warning(sys._getframe().f_back.f_code.co_name)
                # log.warning(str(exc))
                print(str(exc))
                print(traceback.print_exc())
            finally:
                log.debug(cursor.query)
                return cursor.query


    def fetchmany(self, PSQL, *args):
        """ Yields a generator for dealing with large datasets """
        with _pgpool.cursor() as cursor:
            try:
                cursor.execute(PSQL, args)
            except TypeError:
                cursor.execute(PSQL, args[0])
            while 1:
                items = cursor.fetchmany()
                if not items:
                    break
                for item in items:
                    yield item

    def copyfrom(self, tablename, obj):
        """ Bulk load a large dataset (list) quickly  """
        copyconn = ThreadedConnectionPool(1, 1, dsn=dsn)
        conn = copyconn.getconn()
        conn.autocommit = True
        cursor = conn.cursor()
        try:
            if obj:
                f = AsFile(obj)
                cursor.copy_from(f, tablename, sep='|')
                log.debug(cursor.query)
                return cursor.query
        except Exception as e:
            log.critical(e)
            return None

    def logs(self, clientid, meta, origin):
        PSQL = ''' INSERT INTO cache.logs (clientid, meta, origin) VALUES (%s, %s, %s) '''
        return self.execute(PSQL, clientid, meta, origin)

    def post(self, tablename, key, **kw):
        meta = kw['meta'] = jc(kw.get('meta'))
        values = list(kw.values())
        values.append(meta)
        SQL = f'''INSERT INTO {tablename} ({','.join(kw.keys())}) VALUES ({str('%s,' * len(kw.keys()))[:-1]}) ON CONFLICT ({key}) DO UPDATE SET meta = %s'''
        return self.execute(SQL, *values)

    def merge(self, tablename, key, **kw):
        meta = kw['meta'] = jc(kw.get('meta'))
        values = list(kw.values())
        values.append(meta)
        SQL = f'''INSERT INTO {tablename} ({','.join(kw.keys())}) VALUES ({str('%s,' * len(kw.keys()))[:-1]}) ON CONFLICT ({key}) DO UPDATE SET meta = {tablename}.meta || %s'''
        return self.execute(SQL, *values)

    def get(self, tablename, *args, **kw):
        keys = ','.join(args)
        where = ''
        values = []
        if kw:
            for k, v in kw.items():
                values.append(v)
                where += f'{k}=%s AND '
            where = where[0:-4]
        else:
            where = '0=0'
        SQL = f''' SELECT {keys} FROM {tablename} WHERE {where}'''
        return self.fetchall(SQL, *values)

class AbstractDatabaseConnectionPool:

    def __init__(self, maxsize=poolsize):
        if not isinstance(maxsize, integer_types):
            raise TypeError('Expected integer, got %r' % (maxsize, ))
        self.maxsize = maxsize
        self.pool = Queue()
        self.size = 0

    def create_connection(self):
        raise NotImplementedError()

    def get(self):
        pool = self.pool
        if self.size >= self.maxsize or pool.qsize():
            return pool.get()

        self.size += 1
        try:
            new_item = self.create_connection()
        except:
            self.size -= 1
            raise
        return new_item

    def put(self, item):
        self.pool.put(item)

    def closeall(self):
        while not self.pool.empty():
            conn = self.pool.get_nowait()
            try:
                conn.close()
            except Exception:
                pass

    @contextmanager
    def connection(self, isolation_level=None):
        conn = self.get()
        try:
            if isolation_level is not None:
                if conn.isolation_level == isolation_level:
                    isolation_level = None
                else:
                    conn.set_isolation_level(isolation_level)
            yield conn
        except:
            if conn.closed:
                conn = None
                self.closeall()
            raise
        else:
            if conn.closed:
                raise OperationalError(f"Cannot commit because connection was closed: {conn}")
        finally:
            if conn is not None and not conn.closed:
                if isolation_level is not None:
                    conn.set_isolation_level(isolation_level)
                self.put(conn)

    @contextmanager
    def cursor(self, *args, **kwargs):
        isolation_level = kwargs.pop('isolation_level', None)
        with self.connection(isolation_level) as conn:
            try:
                yield conn.cursor(*args, **kwargs)
            except:
                global _pgpool
                _pgpool = None
                del(self)


class PostgresConnectionPool(AbstractDatabaseConnectionPool):
    def __init__(self, postgres_dsn, **kwargs):
        try:
            self.pconnect = ThreadedConnectionPool(1, poolsize, dsn=postgres_dsn)
        except:
            global _pgpool
            _pgpool = None
            raise PostgresError('Database Connection Failed')
        maxsize = kwargs.pop('maxsize', None)
        self.kwargs = kwargs
        AbstractDatabaseConnectionPool.__init__(self, maxsize)

    def create_connection(self):
        self.conn = self.pconnect.getconn()
        self.conn.autocommit = True
        return self.conn


def gevent_wait_callback(conn, timeout=None):
    """A wait callback useful to allow gevent to work with Psycopg."""
    while 1:
        state = conn.poll()
        if state == extensions.POLL_OK:
            break
        elif state == extensions.POLL_READ:
            wait_read(conn.fileno(), timeout=timeout)
        elif state == extensions.POLL_WRITE:
            wait_write(conn.fileno(), timeout=timeout)
        else:
            raise PostgresError("Bad result from poll: %r" % state)

extensions.set_wait_callback(gevent_wait_callback)

def spoolq(setofqueries, dsn=dsn):
    """ allows async bulk loading """
    def sexecute(PSQL, args):
        conn = pool.create_connection()
        with conn.cursor() as pcursor:
            pcursor.execute(PSQL, args)
    extensions.set_wait_callback(gevent_wait_callback)
    pool = PostgresConnectionPool(dsn, maxsize=3)
    threads = [gevent.spawn(sexecute, PSQL, args) for PSQL, args in setofqueries]
    values = [thread.value for thread in threads]
    return values

def tokenstring(value):
    xnum = str(random.randint(1000, 9999))
    xnum = '$N' + xnum + '$'
    newval = xnum + str(value) + xnum
    return newval

class AsFile:
    """ used to bulk load a list and treat it as a file for import via psycopg2 """
    def __init__(self, obj):
        if not isinstance(obj, (list,tuple)):
            raise PostgresError('Must be a list or tuple when copyfrom() is used')
        import io
        elements = "{}|" * len(obj[0])
        self._it = (str(elements[:-1]).format(*x) for x in obj)
        self._f = io.StringIO()

    def read(self, length=sys.maxsize):
        try:
            while self._f.tell() < length:
                self._f.write(next(self._it) + "\n")
        except StopIteration as e:
            pass
        except Exception as e:
            log.warning("uncaught exception: {}".format(e))
        finally:
            self._f.seek(0)
            data = self._f.read(length)
            remainder = self._f.read()
            self._f.seek(0)
            self._f.truncate(0)
            self._f.write(remainder)
            return data

    def readline(self):
        return next(self._it)

def changekey(tablename, hashcode, key, newkey):
    PSQL = ''' UPDATE {0} SET meta = meta - %s || jsonb_build_object(%s, meta->%s) WHERE hashcode = %s'''.format(tablename)
    return execute(PSQL, key, newkey, key, hashcode)

def dict2clause(whereclause=None, metaclause=None):
    wheremeta = ''
    wclause = "{0} %({0})s AND "
    mclause = '''meta @> '{{"{0}" : "%({0})s"}}' AND '''
    for k in whereclause:
        wheremeta = wheremeta + wclause.format(k)
    for k in metaclause:
        wheremeta = wheremeta + mclause.format(k)
    wheremeta = wheremeta[:-4]
    return wheremeta

def setmeta(tablename, hashcode, meta):
    try:
        PSQL = ''' INSERT INTO {0} (hashcode, meta) VALUES (%s, %s)
                   ON CONFLICT (hashcode)
                   DO UPDATE SET meta = {0}.meta || %s, lastupdated = now() ;'''.format(tablename)
        execute(PSQL, hashcode, jc(meta), jc(meta))
        return True
    except Exception as exc:
        return False

def getmeta(tablename, whereclause, metaclause, lencheck=True, limit=None, columns=()):
    #whereclause should be a dict key = column, and value.  WHERE KEY = VALUE
    #possible meta clause should be a dict
    if not isinstance(whereclause, dict) or not isinstance(metaclause, dict):
        raise TypeError('The whereclause and metaclause needs to be a dict type')
    if columns:
        PSQL = 'SELECT meta, {} FROM '.format(', '.join(columns))
    else:
        PSQL = 'SELECT meta FROM '
    if limit:
        PSQL = PSQL+'{0} WHERE {1} LIMIT {2}'.format(tablename, dict2clause(whereclause, metaclause), limit)
    else:
        PSQL = PSQL+'{0} WHERE {1} '.format(tablename, dict2clause(whereclause, metaclause))
    metaclause.update(whereclause)
    x =  fetchall(PSQL, metaclause)
    if x and lencheck and not columns:
        return x if len(x) > 1 else x[0][0]
    elif x:
        return x
    else:
        return {}

def updatemeta(tablename, id, meta, zipcode=None, updatezip=None):
    meta = jc(meta)
    try:
        if updatezip and zipcode:
            PSQL = ''' UPDATE {} SET meta = meta || %s, zipcode=%s WHERE id = %s AND zipcode = %s'''.format(tablename)
            return execute(PSQL, meta, updatezip, id, zipcode)
        elif updatezip:
            PSQL = ''' UPDATE {} SET meta = meta || %s, zipcode=%s WHERE id = %s'''.format(tablename)
            return execute(PSQL, meta, updatezip, id)
        elif zipcode:
            PSQL = ''' UPDATE {} SET meta = meta || %s WHERE id = %s AND zipcode = %s'''.format(tablename)
            return execute(PSQL, meta, id, zipcode)
        else:
            PSQL = ''' UPDATE {} SET meta = meta || %s WHERE id = %s'''.format(tablename)
            return execute(PSQL, meta, id)
    except:
        return False

# Connection aliases
def help():
    x =  r'''Async (gevent) Postgres Connection pool designed to handle high load, webserver scaling, resiliency, and speed by keeping a set pool of connections open and a queue of requests.\n
     Usage : *args can simply be separated by commas or pass the query alone\n\n
     set_dsn() to set your connection details (can be a connection string or <host>, <port>, <database>, <username>, <password>, <timeout>, <application_name>, <number of connections>\n
     cursor() for an open cursor\n
     execute(<sqlstatement>, args) \n
     fetchone(<sqlstatement>, args) \n
     fetchall(<sqlstatement>, args) \n
     logsql(server, message, tablename) \n
      '''
    print(x)
    return x

# update event.cache
#     set notification_event_status='PROCESSING'
#     where id IN (
#         select id from notification_events e
#         where notification_event_status = 'ENQUEUED'
#         order by created_at
#         FOR UPDATE SKIP LOCKED
#         limit ?1)
# RETURNING *