from gevent import spawn
import schedule
import arrow

pause = False

def velox_sync():
    if not pause:
        import API.PMS.velox as v
        v.scheduled(2)

def set():
    schedule.every(15).minutes.do(velox_sync)
