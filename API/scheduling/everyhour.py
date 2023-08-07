from gevent import spawn
import schedule
import arrow

pause = False

def velox_sync():
    if arrow.now().format('HH') >= '05' and not pause:
        import API.PMS.velox as v
        v.scheduled(24)

def set():
    schedule.every().hour.do(velox_sync)
