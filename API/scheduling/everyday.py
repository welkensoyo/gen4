from gevent import spawn
import schedule

def midnight():
    pass
#     from API.PMS import velox
#     spawn(velox.reset)

def set():
    schedule.every().day.at("00:00").do(midnight)

