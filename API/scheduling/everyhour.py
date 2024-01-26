from gevent import spawn
import schedule

pause = False

def velox_sync():
    import API.PMS.velox as v
    if not pause:
        v.scheduled()

def velox_appointments():
    import API.PMS.velox as v
    if not pause:
        va = v.API()
        va.available_appointments()
def set():
    schedule.every(15).minutes.do(velox_sync)
    schedule.every(1).hour.do(velox_appointments)
