from gevent import spawn
import schedule
from API.PMS import velox


def threeam():
    spawn(velox.nightly)


def _set():
    schedule.every().day.at("03:00").do(threeam)
