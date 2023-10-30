import schedule
from API.PMS import velox
def v_reset():
    velox.reset()


def v_refresh():
    velox.refresh()

def set():
    pass
    # schedule.every().saturday.at("00:00").do(v_reset)
    # schedule.every().saturday.at("03:00").do(v_refresh)