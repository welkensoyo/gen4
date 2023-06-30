import API.dbms as db

qry = {'practices': ''' SELECT [id],[name],[owner_name],[address_line1],[address_line2],[city],[state],[zip],[phone_number],[email],[third_party_id],[enabled],[last_sync]
        FROM [dbo].[vx_practice] ORDER BY id ''',
       'upsert' : ''' 
IF EXISTS ( SELECT * FROM dbo.vx_practice with (UPDLOCK, SERIALIZABLE)
WHERE id = %s)
UPDATE dbo.vx_practice
SET [name]=%s,[owner_name]=%s,[address_line1]=%s,[address_line2]=%s,[city]=%s,[state]=%s,[zip]=%s,[phone_number]=%s,[email]=%s,[third_party_id]=%s,[enabled]=%s,[last_sync]=%s
WHERE id = %s 
ELSE
INSERT INTO dbo.vx_practice ([id],[name],[owner_name],[address_line1],[address_line2],[city],[state],[zip],[phone_number],[email],[third_party_id],[enabled],[last_sync])
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)  '''
       }

class API:
    def __init__(self):
        self.id = ''

    def upsert(self, table):
        for r in table:
            db.execute(qry['upsert'],r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[10],r[11],r[12],r[0],r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[10],r[11],r[12])
        return

    def get(self):
        return db.fetchall(qry['practices'])