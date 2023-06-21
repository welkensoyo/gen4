import API.toolkits.smartsheet as s
import API.toolkits.sql_con as q
import googlemaps

gmaps = googlemaps.Client(key='AIzaSyA45p41Zv_0KHbLtaSMfeqITyCxLGZDOGs')

smart = s.Smartsheet('5560894068418436')

smart = smart.smart_df()

smart = smart[smart['Office'].notnull()]
smart['Full_Address'] = smart['Address'] + ", " + smart['City'] + ", " + smart['State']
ticker = 1
addresses = smart['Full_Address'].values.tolist()
latitude = []
longitude = []

for i in addresses:
    geocode_result = gmaps.geocode(i)
    latitude.append(geocode_result[0]["geometry"]["location"]["lat"])
    longitude.append(geocode_result[0]["geometry"]["location"]["lng"])
    ticker += 1

smart["Latitude"] = latitude
smart['Longitude'] = longitude
smart['PMS Peds'] = smart['PMS Peds'].fillna('')
smart['PMS OMS'] = smart['PMS OMS'].fillna('')
smart['PMS Ortho'] = smart['PMS Ortho'].fillna('')
smart['PMS'] = smart['PMS Peds'] + ' ' + smart['PMS OMS'] + ' ' + smart['PMS Ortho']
pms = smart["PMS"].values.tolist()
smart["PMS"] = [str(i).replace('nan', '') for i in pms]
smart['PMS'] = smart['PMS'].str.strip()
smart.dropna(subset=['Office'], inplace=True)

smart.to_sql(name='Practice_Locations_Geo_v2', schema='dbo', con=q.engine(), index=True, if_exists='replace', method=None, chunksize=200)

print(smart)
