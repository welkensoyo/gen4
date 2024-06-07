import logging
import schedule
from gevent import sleep as gsleep, spawn as gspawn
from API.scheduling import everyday, saturday, sunday, weekdays, everyhour
from API.config import scheduler as check

logger = logging.getLogger('')

def start():
    if not check:
        return
    def start_thread():
        everyhour._set()
        everyday._set()
        weekdays._set()
        saturday._set()
        sunday._set()
        while 1:
            try:
                schedule.run_pending()
            except:
                logger.exception('Scheduler Exception')
            gsleep(5)
    gspawn(start_thread)
    print('Scheduler Started...')

# def set():
    # schedule.every(180).seconds.do(func)
    # schedule.every().day.at("00:00").do(run, 'boarded')
    # schedule.every().day.at("06:00").do(processScheduledPayments)



