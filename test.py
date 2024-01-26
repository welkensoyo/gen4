import arrow

now = arrow.now()

if 6 <= int(now.format('HH')) <= 19:
    print(True)