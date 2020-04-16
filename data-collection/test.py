import datetime as dt

bst = dt.timezone(dt.timedelta(hours = 1))
gmt = dt.timezone.utc

now = dt.datetime.now(tz = gmt)
eta = "2020-04-15 05:16:18"
year = int(eta[:4])
month = int(eta[5:7])
day = int(eta[8:10])
hour = int(eta[11:13])
minute = int(eta[14:16])
second = int(eta[17:19])

eta = dt.datetime(year, month, day, hour, minute, second)
eta_aware = eta.replace(tzinfo=gmt)
aware = now.replace(tzinfo=gmt)

print(now)
print(eta)
print(eta_aware)
print(aware)

string = "37, 23, 12"
routes = string.split(',')
print(routes)