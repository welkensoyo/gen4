import traceback
from API.config import port
from gevent import spawn
import API.dbpyodbc as dbpy
import API.dbms as db
import arrow

def api_log(route, method, payload, code, result):
    print('api_log', route, method, payload, code, result)
    def _(route, method, payload, code, result):
        PSQL = ''' INSERT INTO dev.api_log (route, method, payload, response_code, response_message) VALUES (?, ?, ?, ?, ?) '''
        dbpy.execute(PSQL, route, method, payload, code, result)
    spawn(_, route, method, payload, code, result)


def velox_log(mode=None, error=''):
    if mode:
        SQL = "UPDATE dbo.vx_log SET last_sync=GETDATE() AT TIME ZONE 'Central Standard Time', error=? WHERE [mode] = ?"
        dbpy.execute(SQL, error, mode)
    SQL = "SELECT mode, CONVERT(VARCHAR, last_sync, 120), error FROM dbo.vx_log"
    return [(x[0], arrow.get(x[1]).to('US/Central').format('YYYY-MM-DD HH:mm:ss'), x[2]) for x in dbpy.fetchall(SQL)]
