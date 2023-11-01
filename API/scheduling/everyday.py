from gevent import spawn
import schedule

def nightly():
    from API.PMS import velox
    velox.refresh()

def set():
    schedule.every().day.at("02:00").do(nightly)

