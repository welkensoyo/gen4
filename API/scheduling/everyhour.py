from gevent import spawn
import schedule
import API.PMS.velox as v

pause = False

def velox_sync():
    if not pause:
        v.scheduled(2)

def set():
    schedule.every(15).minutes.do(velox_sync)
