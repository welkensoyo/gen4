from API import dbms as db

qry = {
    'new' : ''' INSERT INTO dev.TRACKER (method, userid, action) VALUES (%s, %s, %s) '''
}

def report_clicked(method, userid, action):
    return db.execute(qry['new'], method, userid, action)

