import API.PMS.velox as v
import arrow
import API.dbms as db

print(arrow.get().format('YYYY-MM-DD HH:mm:ss.SSSSSS'))

for each in v.full_tables:
    SQL = '''ALTER TABLE dbo.{}
ADD last_updated datetime2 DEFAULT GETDATE();'''
    db.execute(SQL.format(f'vx_{each}'))