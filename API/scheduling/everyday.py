from gevent import spawn
import schedule

def midnight():
    from API.PMS import velox
    velox.refresh()

def set():
    schedule.every().day.at("00:00").do(midnight)

