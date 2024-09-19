from API import dbms as db

qry = {
    'new' : ''' INSERT INTO dev.TRACKER (method, userid, action, last_updated) VALUES (%s, %s, %s, GETDATE() ) '''
}

def report_clicked(method, userid, action):
    return db.execute(qry['new'], method, userid, action)

