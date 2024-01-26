from gevent import spawn
import schedule


def nightly():
    from API.PMS import velox
    velox.nightly()


def set():
    schedule.every().day.at("03:00").do(nightly)
