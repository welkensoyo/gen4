from gevent import spawn
import schedule

def velox_sync():
    import API.PMS.velox as v
    v.scheduled(2)

def set():
    schedule.every().hour.do(velox_sync)
