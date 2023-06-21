from toolkit import smartsheet as s, sql_con as q
import googlemaps
gmaps = googlemaps.Client(key='AIzaSyA45p41Zv_0KHbLtaSMfeqITyCxLGZDOGs')

smart = s.Smartsheet(5560894068418436)
sql = q.SqlConnFrodo()

df = smart.smart_df()

df['Full Address'] = df['Address'] + ", " + df['City'] + ", " + df['State']
ticker = 1
addresses = df['Full Address'].values.tolist()
latitude = []
longitude = []

for i in addresses:
    geocode_result = gmaps.geocode(i)
    latitude.append(geocode_result[0]['geometry']['location']['lat'])
    longitude.append(geocode_result[0]['geometry']['location']['lng'])
    # print(ticker)
    ticker += 1
df["Latitude"] = latitude
df['Longitude'] = longitude

print('done')
df.to_sql(name='SDBMap', schema='dbo', con=q.engine, index=True, if_exists='replace', method=None, chunksize=200)