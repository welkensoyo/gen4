import gevent.socket
import pymssql
import traceback
import sys
from API.config import sqlserver
from API.config import bpk, denticon
import pandas as pd

def wait_callback(read_fileno):
    gevent.socket.wait_read(read_fileno)

pymssql.set_wait_callback(wait_callback)

_conn = None

def conn(dsn):
    return pymssql.connect(server=dsn.server, user=dsn.user, password=dsn.password, database=dsn.database, autocommit=True)

def close():
    global _conn
    if _conn:
        try:
            _conn.close()
        except:
            pass
        _conn = None

class ConnectionError(Exception):
    pass


def fetchone(PSQL, *args): return SQLcursor(sqlserver).fetchone(PSQL, *args)
def fetchall(PSQL, *args): return SQLcursor(sqlserver).fetchall(PSQL, *args)
def execute(PSQL, *args): return SQLcursor(sqlserver).execute(PSQL, *args)
def executemany(PSQL, *args): return SQLcursor(sqlserver).executemany(PSQL, *args)
def fetchall_df(PSQL, *args): return SQLcursor(sqlserver).fetchall_df(PSQL, *args)

def bpkfetchone(PSQL, *args): return SQLcursor(bpk).fetchone(PSQL, *args)
def bpkfetchall(PSQL, *args): return SQLcursor(bpk).fetchall(PSQL, *args)
def bpkexecute(PSQL, *args): return SQLcursor(bpk).execute(PSQL, *args)

def bpkfetchall_df(PSQL, *args): return SQLcursor(bpk).fetchall_df(PSQL, *args)


def denticonfetchone(PSQL, *args): return SQLcursor(denticon).fetchone(PSQL, *args)
def denticonfetchall(PSQL, *args): return SQLcursor(denticon).fetchall(PSQL, *args)
def denticonexecute(PSQL, *args): return SQLcursor(denticon).execute(PSQL, *args)

def denticonfetchall_df(PSQL, *args): return SQLcursor(denticon).fetchall_df(PSQL, *args)

class SQLcursor:
    def __init__(self, dsn=sqlserver):
        # Auto Reconnect
        global _conn
        if not _conn:
            # print('Attempting Connection To SQL...')
            while not _conn:
                try:
                    _conn = conn(dsn)
                    # print('Connection To SQL Established')
                except ConnectionError:
                    raise
                except:
                    close()
                    # print('Nope')
                    gevent.sleep(0.5)

    def fetchone(self, SQL, *args):
        with _conn as c:
            with c.cursor() as cursor:
                try:
                    cursor.execute(SQL, args)
                    return cursor.fetchone()
                except TypeError:
                    cursor.execute(SQL, args[0])
                    return cursor.fetchone()
                except Exception as exc:
                    print(sys._getframe().f_back.f_code)
                    print(sys._getframe().f_back.f_code.co_name)
                    print(exc)
                    return ()
                finally:
                    close()

    def fetchall(self, SQL, *args):
        with _conn as c:
            with c.cursor() as cursor:
                try:
                    cursor.execute(SQL, args)
                    return cursor.fetchall()
                except TypeError:
                    cursor.execute(SQL, args[0])
                    return cursor.fetchall()
                except Exception as exc:
                    print(sys._getframe().f_back.f_code)
                    print(sys._getframe().f_back.f_code.co_name)
                    print(str(exc))
                    return ()
                finally:
                    close()

    def fetchalld(self, SQL, *args):
        with _conn as c:
            with c.cursor() as cursor:
                try:
                    cursor.execute(SQL, args)
                except TypeError:
                    cursor.execute(SQL, args[0])
                except Exception as exc:
                    print(sys._getframe().f_back.f_code)
                    print(sys._getframe().f_back.f_code.co_name)
                    print(str(exc))
                    return ()
                return cursor.fetchall(), [y[0] for y in cursor.description]

    def execute(self, PSQL, *args):
        with _conn as c:
            with c.cursor() as cursor:
                try:
                    cursor.execute(PSQL, args)
                except TypeError:
                    cursor.execute(PSQL, args[0])
                except ValueError:
                    cursor.execute(PSQL, tuple(args))
                except:
                    print(sys._getframe().f_back.f_code)
                    print(sys._getframe().f_back.f_code.co_name)
                    traceback.print_exc()
                    return ()
                finally:
                    close()

    def execute2(self, PSQL, *args):
        with _conn as c:
            with c.cursor() as cursor:
                cursor.execute(PSQL, tuple(args[0]))
        return

    def executemany(self, PSQL, args):
        with _conn as c:
            with c.cursor() as cursor:
                try:
                    cursor.executemany(PSQL, args)
                except Exception as exc:
                    print(sys._getframe().f_back.f_code)
                    print(sys._getframe().f_back.f_code.co_name)
                    print(str(exc))
                    return ()
                finally:
                    close()

    def fetchmany(self, PSQL, *args):
        with _conn as c:
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
        print('connecting')
        df = pd.read_sql(SQL, _conn)
        return df
