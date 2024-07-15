from gevent import spawn
import schedule
from API.PMS import velox


def threeam():
    spawn(velox.nightly)

def fiveam():
    spawn(velox.check_for_missing_records)


def _set():
    schedule.every().day.at("03:00").do(threeam)
    schedule.every().day.at("05:00").do(fiveam)