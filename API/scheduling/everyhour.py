from gevent import spawn
import schedule
import API.PMS.velox as v

pause = False

def velox_sync():
    if not pause:
        v.scheduled()

def velox_appointments():
    if not pause:
        va = v.API()
        va.practices()
        # va.available_appointments()

def _set():
    schedule.every(15).minutes.do(velox_sync)
    schedule.every(1).hour.do(velox_appointments)
