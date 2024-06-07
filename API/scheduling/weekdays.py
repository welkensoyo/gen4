from gevent import spawn
import schedule

def set_weekday(job, time, *args):
    schedule.every().monday.at(time).do(job, *args)
    schedule.every().tuesday.at(time).do(job, *args)
    schedule.every().wednesday.at(time).do(job, *args)
    schedule.every().thursday.at(time).do(job, *args)
    schedule.every().friday.at(time).do(job, *args)

def _set():
    # set_weekday(run_eod_attachments, '11:00')
    pass
