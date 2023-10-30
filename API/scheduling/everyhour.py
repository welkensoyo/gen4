from gevent import spawn
import schedule
import arrow

pause = False

def velox_sync():
    if not pause:
        import API.PMS.velox as v
        v.scheduled(24)

def set():
    schedule.every().hour.do(velox_sync)
