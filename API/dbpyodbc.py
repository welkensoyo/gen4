import pyodbc
import traceback
import sys
from API.config import sqlserver, port

if port == 80:
    connection_string = (
        'DRIVER={ODBC Driver 18 for SQL Server};'
        f'SERVER={sqlserver.server};'
        f'DATABASE={sqlserver.database};'
        f'UID={sqlserver.user};'
        f'PWD={sqlserver.password};'
        'TrustServerCertificate=yes;'
    )
else:
    connection_string = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={sqlserver.server};'
        f'DATABASE={sqlserver.database};'
        f'UID={sqlserver.user};'
        f'PWD={sqlserver.password};'
        'TrustServerCertificate=yes;'
    )
_conn = None


def fetchall(SQL, *args):
    conn= pyodbc.connect(connection_string, autocommit=True)
    c = conn.cursor()
    try:
        c.execute(SQL, args)
        return c.fetchall()
    except TypeError:
        c.execute(SQL, args[0])
        return c.fetchall()
    except Exception as exc:
        print(sys._getframe().f_back.f_code)
        print(sys._getframe().f_back.f_code.co_name)
        print(str(exc))
        return ()
    finally:
        c.close()

def execute(PSQL, *args):
    conn= pyodbc.connect(connection_string, autocommit=True)
    c = conn.cursor()
    try:
        c.execute(PSQL, args)
    except TypeError:
        c.execute(PSQL, args[0])
    except ValueError:
        c.execute(PSQL, tuple(args))
    except:
        print(sys._getframe().f_back.f_code)
        print(sys._getframe().f_back.f_code.co_name)
        traceback.print_exc()
        return ()
    finally:
        c.close()



if __name__=='__main__':
    from njson import jc
    from gevent import monkey
    monkey.patch_all()
    # x = pyodbc.connect(connection_string, autocommit=True)
    # c = x.cursor()
    # c.execute('SELECT * FROM dbo.vx_log')

    x = fetchall('SELECT * FROM dbo.vx_log')
    print(jc(x))

