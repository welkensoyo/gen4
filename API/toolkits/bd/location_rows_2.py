import API.smart as smart

s = smart.API('SDB Directory')
s.open()
print(s.columns())

