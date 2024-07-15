
import arrow
x = '2001-01-01T00:00:00.000Z'

print(arrow.get(x).format('YYYY-MM-DD HH:mm:ss'))