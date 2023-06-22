from gevent import spawn
import schedule

def velox_sync():
    print('velox syncing...')
    import API.PMS.velox as v
    v.scheduled(10)

def set():
    schedule.every().hour.do(velox_sync)
