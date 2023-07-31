from gevent import spawn
import schedule

def velox_sync():
    import API.PMS.velox as v
    v.scheduled(24)

def set():
    schedule.every().hour.do(velox_sync)
