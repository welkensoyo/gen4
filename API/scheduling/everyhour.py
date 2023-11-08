from gevent import spawn
import schedule

pause = False

def velox_sync():
    import API.PMS.velox as v
    if not pause:
        v.scheduled(2)

def set():
    schedule.every(15).minutes.do(velox_sync)
