from gevent import spawn
import API.dbpyodbc as dbpy
import API.dbms as dbms
import arrow
from cachetools import TTLCache
from cachetools import cached

def api_log(route, method, payload, code, result):
    # print('api_log', route, method, payload, code, result)
    def _(route, method, payload, code, result):
        PSQL = ''' INSERT INTO dev.api_log (route, method, payload, response_code, response_message) VALUES (?, ?, ?, ?, ?) '''
        dbpy.execute(PSQL, route, method, payload, code, result)
    spawn(_, route, method, payload, code, result)

def sync_log(table, pids, result):
    if isinstance(pids, (list,tuple,set)):
        pids = ','.join(map(str, pids))
    # print('api_log', route, method, payload, code, result)
    def _(table, pids, result):
        PSQL = ''' INSERT INTO dev.sync_log (tablename, pids, result) VALUES (?, ?, ?) '''
        dbpy.execute(PSQL, table, pids, result)
    spawn(_, table, pids, result)

@cached(cache=TTLCache(maxsize=10, ttl=600))
def velox_log(mode=None, error=''):
    try:
        if mode:
            SQL = "UPDATE dbo.vx_log SET last_sync=GETDATE() AT TIME ZONE 'Central Standard Time', error=? WHERE [mode] = ?"
            dbpy.execute(SQL, error, mode)
        SQL = "SELECT mode, CONVERT(VARCHAR, last_sync, 120), error FROM dbo.vx_log"
        return [(x[0], arrow.get(x[1]).to('US/Central').format('YYYY-MM-DD HH:mm:ss'), x[2]) for x in dbms.fetchall(SQL)]
    except:
        return []

@cached(cache=TTLCache(maxsize=2, ttl=1200))
def velox_stats():
    SQL = 'SELECT * FROM cached.missed_last_sync ORDER BY 2,3,4'
    return [(x[0], x[1], x[2], arrow.get(x[3]).to('US/Central').format('YYYY-MM-DD HH:mm:ss'), arrow.get(x[4]).to('US/Central').format('YYYY-MM-DD HH:mm:ss')) for x in dbms.fetchall(SQL)]


def velox_sync(status=None):
    if status not in (None, 'running', 'idle'):
        return 'idle'
    if status:
        SQL = ''' UPDATE dev.settings SET value = ? WHERE setting = 'sync_state' '''
        dbpy.execute(SQL, status)
        return status
    SQL = ''' SELECT value FROM dev.settings WHERE setting = 'sync_state' '''
    status = dbms.fetchall(SQL)[0][0]
    return status