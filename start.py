from gevent import monkey, spawn, sleep
monkey.patch_all()
import signal
from gevent import signal_handler as sig
import bottle
import API.scheduler as scheduler
import API.config as config
# import logging, sys
# from logging import handlers
from gevent.pywsgi import WSGIServer
from geventwebsocket.handler import WebSocketHandler
from beaker.middleware import SessionMiddleware
from mainapp import mainappRoute
from whitenoise import WhiteNoise, compress
from pathlib import Path
from contextlib import redirect_stdout

debug = False
staticfolder = 'static'
bottle.TEMPLATE_PATH.insert(0, 'templates/')
bottle.TEMPLATE_PATH.insert(0, 'templates/render/')

# level = logging.getLevelName('INFO')
# log = logging.getLogger('')
# log.setLevel(level)
# format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# fh = handlers.RotatingFileHandler('logs/servicelog.txt', maxBytes=(1048576*5), backupCount=3)
# fh.setFormatter(format)
# log.addHandler(fh)

# if debug:
#     ch = logging.StreamHandler(sys.stdout)
#     ch.setFormatter(format)
#     log.addHandler(ch)

def check_compress():
    ignore =  ["pdf","jpg","jpeg","png","gif","7z","zip","gz","tgz","bz2","tbz","xz","br","swf","flv","woff","woff2","eot","py","pdf","docx","svg"]
    import os
    c = compress.Compressor(extensions=ignore)
    ds = ('static', 'dist')
    for d in ds:
        for dirpath, _dirs, files in os.walk(d):
            for filename in files:
                if c.should_compress(filename):
                    if not os.path.exists(dirpath+'/'+filename+'.gz'):
                        for each in c.compress(os.path.join(dirpath, filename)):
                            print(each)
    print('File optimization completed')

if __name__ == '__main__':
    from contextlib import redirect_stdout
    import os
    # location = os.devnull
    # location = '/opt/gen4/velox.log'
    # location = '/home/gen4it/velox.log'
    # print(f'Print Redirected to log {location}')
    # with open(location, 'w') as f:
    # with redirect_stdout(None):
    if config.compress:
        spawn(check_compress)
    print(Path.home())
    print('Started...')
    botapp = bottle.app()
    for Route in (mainappRoute,):
        botapp.merge(Route)
    botapp = SessionMiddleware(botapp, config.beakerconfig)
    botapp = WhiteNoise(botapp)
    botapp.add_files(staticfolder, prefix='static/')
    # botapp.add_files('dist', prefix='dist/')
    scheduler.start()
    server = WSGIServer(("0.0.0.0", 8080), botapp , handler_class=WebSocketHandler)

    def shutdown():
        print('Shutting down ...')
        server.stop(timeout=5)
        exit(signal.SIGTERM)

    sig(signal.SIGTERM, shutdown)
    sig(signal.SIGINT, shutdown)
    server.serve_forever()


''' #Service on Linux to run python
#! /bin/sh
# /etc/init.d/pythonsvc
 
case "$1" in
  start)
    echo "Starting App Service via opt/wwww/sdbservice.py"
    # run application you want to start
    cd /
    cd /opt/www
    /root/miniconda2/bin/python sdbservice.py &
    ;;
  stop)
    echo "Stopping App Service"
    # kill application you want to stop
    pkill -9 python
    ;;
  *)
    echo "Usage: /etc/init.d/wllsvc{start|stop}"
    exit 1
    ;;
esac 
'''

''' COPY SCRIPT
cd /opt
rm -R www
mv /tmp/www /opt
sudo /etc/init.d/wllsvc.sh stop
sudo /etc/init.d/wllsvc.sh start

http://support.worldpay.com/support/CNP-API/content/dpayfacdp.htm
'''