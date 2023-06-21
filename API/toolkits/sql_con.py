import pandas as pd
import pymssql
import urllib.parse as parse
from API.config import bpk

cursor = None

class SqlConnFrodo:
    def __init__(self):
        self.table = {pd.set_option('display.max_columns', None),
                      pd.set_option('display.max_rows', None),
                      pd.set_option('display.width', 1000)}
        self.server = 'sdb-frodo.database.windows.net'
        self.database = 'Monthly_Reports'
        self.user = 'Sauron'
        self.password = 'BR@5000!'

    def conn(self):
        return pymssql.connect(server=self.server, user=self.user, password=self.password, database=self.database)

    def fetchall_df(self, query):
        conn = pymssql.connect(server=self.server, user=self.user, password=self.password, database=self.database)
        df = pd.read_sql(query, conn)
        conn.close()
        return df


def engine():
    global cursor
    from sqlalchemy import create_engine
    import pyodbc
    server = 'sdb-frodo.database.windows.net'
    database = 'Monthly_Reports'
    username = 'Sauron'
    password = 'BR@5000!'
    if not cursor:
        connx = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
        cursor = connx.cursor()

    params = parse.quote_plus(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    return create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)



class SqlConnBPK:
    def __init__(self):
        self.table = {pd.set_option('display.max_columns', None),
                      pd.set_option('display.max_rows', None),
                      pd.set_option('display.width', 1000)}
        self.server = bpk.server
        self.database = bpk.database
        self.user = bpk.user
        self.password = bpk.password

    def conn(self):
        return pymssql.connect(server=self.server, user=self.user, password=self.password, database=self.database)

    def fetchall(self, query):
        conn = pymssql.connect(server=self.server, user=self.user, password=self.password, database=self.database)
        cursors = conn.cursor()
        cursors.execute(query)
        result = cursors.fetchall()
        conn.close()
        return result

    def fetchall_df(self, query):
        conn = pymssql.connect(server=self.server, user=self.user, password=self.password, database=self.database)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
