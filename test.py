# from API.PMS import velox
# velox.reset_table('ledger')
# velox.reset_table('treatments')
#
import arrow

a = arrow.now()
print(a)

print(a.format('HH'))