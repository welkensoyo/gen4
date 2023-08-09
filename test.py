# from API.PMS import velox
# velox.reset_table('ledger')
# velox.reset_table('treatments')
#
import arrow

if arrow.now().format('HH') >= '05':
    print('True')
    import API.PMS.velox as v
    v.scheduled(24)
