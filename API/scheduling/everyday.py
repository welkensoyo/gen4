from gevent import spawn
import schedule

def sixam():
    return

def set():
    schedule.every().day.at("06:00").do(sixam)

