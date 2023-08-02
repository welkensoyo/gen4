from gevent import spawn
import schedule
import arrow

def velox_sync():
    if arrow.now().format('HH') >= '05':
        import API.PMS.velox as v
        v.scheduled(24)

def set():
    schedule.every().hour.do(velox_sync)
