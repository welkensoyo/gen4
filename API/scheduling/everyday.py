from gevent import spawn
import schedule

def midnight():
    from API.PMS import velox
    velox.reset()

def set():
    pass
    # schedule.every().day.at("00:00").do(midnight)

