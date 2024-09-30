import gevent.socket
import pymssql
import traceback
import sys
from API.config import sqlserver
import pandas as pd
import os
from gevent.queue import Queue
from gevent.pool import Pool
from contextlib import contextmanager
import logging

logger = logging.getLogger('')
POOL_SIZE = 5

def wait_callback(read_fileno):
    gevent.socket.wait_read(read_fileno)

pymssql.set_wait_callback(wait_callback)

class SQLcursor:
    def __init__(self, dsn=sqlserver):
        self._conn = None
        # Auto Reconnect
        if not self._conn:
            # logger.info('Attempting Connection To SQL...')
            while not self._conn:
                try:
                    self._conn = pymssql.connect(server=dsn.server, user=dsn.user, password=dsn.password, database=dsn.database, autocommit=True)
                    # logger.info('Connection To SQL Established')
                except:
                    traceback.print_exc()
                    # logger.info('Nope')
                    gevent.sleep(0.5)

    def conn(self):
        return self._conn

    def fetchone(self, SQL, *args):
        with self._conn as c:
            with c.cursor() as cursor:
                try:
                    cursor.execute(SQL, args)
                    return cursor.fetchone()
                except TypeError:
                    cursor.execute(SQL, args[0])
                except Exception as exc:
                    logger.info(sys._getframe().f_back.f_code)
                    logger.info(sys._getframe().f_back.f_code.co_name)
                    logger.info(exc)
                finally:
                    self._conn.close()


    def fetchall(self, SQL, *args):
        with self._conn as c:
            with c.cursor() as cursor:
                try:
                    cursor.execute(SQL, args)
                    return cursor.fetchall()
                except TypeError:
                    cursor.execute(SQL, args[0])
                except Exception as exc:
                    logger.info(sys._getframe().f_back.f_code)
                    logger.info(sys._getframe().f_back.f_code.co_name)
                    logger.info(str(exc))
                finally:
                    self._conn.close()


    def fetchalld(self, SQL, *args):
        with self._conn as c:
            with c.cursor() as cursor:
                try:
                    cursor.execute(SQL, args)
                    return cursor.fetchall(), [y[0] for y in cursor.description]
                except TypeError:
                    cursor.execute(SQL, args[0])
                    return cursor.fetchall(), [y[0] for y in cursor.description]
                except Exception as exc:
                    logger.info(sys._getframe().f_back.f_code)
                    logger.info(sys._getframe().f_back.f_code.co_name)
                    logger.info(str(exc))
                finally:
                    self._conn.close()

    def execute(self, PSQL, *args):
        with self._conn as c:
            with c.cursor() as cursor:
                try:
                    cursor.execute(PSQL, args)
                except TypeError:
                    cursor.execute(PSQL, args[0])
                except ValueError:
                    cursor.execute(PSQL, tuple(args[0]))
                except:
                    logger.info(sys._getframe().f_back.f_code)
                    logger.info(sys._getframe().f_back.f_code.co_name)
                    traceback.print_exc()
                finally:
                    self._conn.close()

    def execute2(self, PSQL, *args):
        with self._conn as c:
            with c.cursor() as cursor:
                cursor.execute(PSQL, tuple(args[0]))
        return

    def executemany(self, PSQL, args):
        with self._conn as c:
            with c.cursor() as cursor:
                try:
                    cursor.executemany(PSQL, args)
                except Exception as exc:
                    logger.info(sys._getframe().f_back.f_code)
                    logger.info(sys._getframe().f_back.f_code.co_name)
                    logger.info(str(exc))
                finally:
                    self._conn.close()

    def fetchmany(self, PSQL, *args):
        with self._conn as c:
            with c.cursor() as cursor:
                try:
                    cursor.execute(PSQL, args)
                except TypeError:
                    cursor.execute(PSQL, args[0])
                while 1:
                    items = cursor.fetchmany(size=1000)
                    if not items:
                        break
                    for item in items:
                        yield item

    def fetchall_df(self, SQL,  *args):
        return pd.read_sql(SQL, self._conn)


def load_bcp_db(table, filename, _async=False):
    root = os.path.dirname(filename)
    # logger.info(table)
    # logger.info(filename)
    # bcp = f'bcp {table} in "{filename}" -b 50000 -S {bpk.server} -U {bpk.user} -P {bpk.password} -e "error.txt" -h TABLOCK -a 16384 -q -c -t "|" ; rm "{filename}" ' LINUX
    bcp = f'bcp {table} in "{filename}" -b 50000 -S {bpk.server} -U {bpk.user} -P {bpk.password} -e "{root}\error.txt" -h TABLOCK -a 16384 -q -c -t "|" & del "{filename}" '
    # bcp = f'bcp {table} in "{filename}" -b 50000 -S {bpk.server} -U {bpk.user} -P {bpk.password} -e "{root}\error.txt" -h TABLOCK -a 16384 -q -c -t "|" '
    # logger.info(bcp)
    if not _async:
        os.system(bcp)
    else:
        os.popen(bcp)
    return


class AsyncConnectionPool:
    def __init__(self, dsn, pool_size=POOL_SIZE):
        """
        Initialize the AsyncConnectionPool using gevent.
        """
        self.pool = gevent.pool.Pool(pool_size)
        self.queue = gevent.queue.Queue()

        # Populate the queue with connections.
        for _ in range(pool_size):
            conn = pymssql.connect(server=dsn.server, user=dsn.user, password=dsn.password, database=dsn.database, autocommit=True)
            self.queue.put(conn)

    @contextmanager
    def get_conn(self):
        conn = self.queue.get()
        try:
            yield conn
        finally:
            self.queue.put(conn)

    def active_connections(self):
        return self.queue.qsize()

    def execute(self, sql, *args):
        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                return self.pool.apply(cursor.execute, (sql, args))

    def executemany(self, sql, *args):
        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                return self.pool.apply(cursor.executemany, (sql,) + args)

    def fetchall(self, sql, *args):
        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, args)
                return cursor.fetchall()

    def fetchone(self, sql, *args):
        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, args)
                return cursor.fetchone()


gen4_db = AsyncConnectionPool(sqlserver)

def fetchone(PSQL, *args): return gen4_db.fetchone(PSQL, *args)
def fetchall(PSQL, *args): return gen4_db.fetchall(PSQL, *args)
def execute(PSQL, *args): return gen4_db.execute(PSQL, *args)
def executemany(PSQL, *args): return gen4_db.executemany(PSQL, *args)
def fetchall_df(PSQL, *args): return SQLcursor(sqlserver).fetchall_df(PSQL, *args)