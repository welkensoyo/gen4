from toolkit import sql_con as q, smartsheet as s, tools as t

t.been_warned()

smart = s.Smartsheet(5560894068418436)
df = smart.smart_df()

sql = q.SqlConnFrodo()
geo = sql.fetchall_df('''SELECT * FROM Practice_Locations_Geo''')

print("done")