from gevent import spawn
import schedule

def nightly():
    from API.PMS import velox
    velox.refresh()

def set():
    schedule.every().day.at("04:00").do(nightly)

