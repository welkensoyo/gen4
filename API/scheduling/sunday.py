import schedule

def v_reset():
    from API.PMS import velox
    velox.reset()

def set():
    schedule.every().saturday.at("00:00").do(v_reset)