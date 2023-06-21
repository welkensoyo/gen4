import API.toolkits.getexcel as e, API.toolkits.sql_con as s

excel = e.GetExcel('CURRENT_Master Finance Mapping File.xlsx', 'EOD Location Mapping')
sql = s.SqlConnFrodo()
df = excel.excel_fd()

mapping = df[['Location Abbrev', 'VP of Operations', 'Director of Operations', 'Regional Manager']]
mapping.dropna(subset=['VP of Operations'], inplace=True)
mapping.drop_duplicates(subset=['Location Abbrev'], inplace=True
                        )
mapping.to_sql(name='OperationalUsers', schema='dbo', con=s.engine, index=True, if_exists='replace', method=None, chunksize=200)
print('done')

#testx