from gevent import spawn
import schedule
from API.PMS import velox

def nightly():
    velox.nightly()


def set():
    schedule.every(1).day.at("03:00").do(nightly)
