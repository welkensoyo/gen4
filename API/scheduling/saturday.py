import schedule

def nightly():
    from API.PMS import velox
    velox.reset()
def _set():
    pass
    # schedule.every().day.at("06:00").do)