import arrow
x = arrow.get('2024-08-08T13:23:28.013Z').shift(minutes=-16).format('YYYY-MM-DD[T]HH:mm:ss.SSS[Z]')
print(x)


