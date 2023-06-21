import googlemaps
from googlemaps import places
from pprint import pprint
g = googlemaps.Client(key='AIzaSyA45p41Zv_0KHbLtaSMfeqITyCxLGZDOGs')

pid = 'ChIJb-PjhBZhZIgR2XOSDrXg6Fg'
pid = 'ChIJS-KESx1iZIgRAJ0xyfumKNQ'
x = g.places(query='6814 Charlotte Pike, Nashville, TN 37209, USA')
pprint(x) # grab lat and long

x = g.places_nearby(location={'lat': 36.1349018, 'lng': -86.8921156}, radius=1000, type='shopping')
pprint(x)

# x = g.place(pid)
# x = g.place(pid, language='en')
# x = g.find_place(input='6814 Charlotte Pike, Nashville, TN 37209', input_type='textquery', fields=["name", "formatted_address", "business_status", "geometry", "photos", "types", "opening_hours", "price_level", "rating", "user_ratings_total"], location_bias="circle:2000@37.4222339,-122.0854804", language="en")
# x = g.place('ChIJPxPtNB1iZIgRo6pHeU7zYOA')
# pprint(x)

