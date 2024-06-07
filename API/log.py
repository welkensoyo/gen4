from gevent import spawn
import API.dbpyodbc as dbpy
import API.dbms as db
import arrow

def log(route, method, payload, code, result):
    def _(route, method, payload, code, result):
        try:
            PSQL = ''' INSERT INTO dev.api_log (route, method, payload, response_code, response_message) VALUES (?, ?, ?, ?, ?) '''
            dbpy.execute(PSQL, route, method, payload, code, result)
        except:
            PSQL = ''' INSERT INTO dev.api_log (route, method, payload, response_code, response_message) VALUES (%s, %s, %s, %s, %s) '''
            db.execute(PSQL, route, method, payload, code, result)
    spawn(_, route, method, payload, code, result)


def velox_log(mode=None, error=''):
    if mode:
        try:
            SQL = "UPDATE dbo.vx_log SET last_sync=GETDATE() AT TIME ZONE 'Central Standard Time', error=? WHERE [mode] = ?"
            return dbpy.execute(SQL, error, mode)
        except:
            SQL = "UPDATE dbo.vx_log SET last_sync=GETDATE() AT TIME ZONE 'Central Standard Time', error=%s WHERE [mode] = %s"
            return db.execute(SQL, error, mode)
    SQL = "SELECT mode, CONVERT(VARCHAR, last_sync, 120), error FROM dbo.vx_log"
    try:
        return [(x[0], arrow.get(x[1]).to('US/Central').format('YYYY-MM-DD HH:mm:ss'), x[2]) for x in dbpy.fetchall(SQL)]
    except:
        return [(x[0], arrow.get(x[1]).to('US/Central').format('YYYY-MM-DD HH:mm:ss'), x[2]) for x in db.fetchall(SQL)]