# import API.dbms as db
#
# SQL = '''INSERT INTO dbo.vx_patients VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
# x = ['19792976', 1438, 'False', '282401', 'Brian', 'W', '1987-11-28T00:00:00.000Z', 'M', 'Rocketchip1@gmail.com', '(269) 568 0324', 'Deibel', '382-3260', '', '', '19792976', '4763 pinefield ave', '', 'Portage', 'MI', '49024', '1', 'False', '2319846', '911051219', '2319846', '911051219', '', '']
# db.execute(SQL, '19792976', 1438, 'False', '282401', 'Brian', 'W', '1987-11-28T00:00:00.000Z', 'M', 'Rocketchip1@gmail.com', '(269) 568 0324', 'Deibel', '382-3260', '', '', '19792976', '4763 pinefield ave', '', 'Portage', 'MI', '49024', '1', 'False', '2319846', '911051219', '2319846', '911051219', '', '')

import API.PMS.velox as velox
v = velox.API()
v.table = 'ledger'
v.create_split_files()