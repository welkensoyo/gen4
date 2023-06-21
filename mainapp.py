import traceback
from gevent import Timeout, sleep
from geventwebsocket import WebSocketError
from API.reports import Report as r
import API.api as a
import API.njson as json
from API.render import Render
from bottle import Bottle, get, request, response, abort, route, error, template, redirect, static_file, hook

mainappRoute = Bottle()

@hook('after_request')
def enable_cors():
    response.add_header('Access-Control-Allow-Origin','*')
    response.add_header('Access-Control-Allow-Methods','PUT, GET, POST, DELETE, OPTIONS')
    response.add_header('Access-Control-Allow-Headers','Authorization, Origin, Accept, Content-Type, X-Requested-With')


@route('/favicon.ico')
def serve_icon():
    return static_file('favicon.ico', root='static/images/')


@get('/healthcheck')
@get('/healthcheck/index.html')
@get('/index.html')
def hcheck():
    return 'Running....'

@get('/')
def _index():
    return 'API'

@route('/api/<command>', method=['GET','POST'])
@route('/api/<command>/<option>', method=['GET','POST'])
@route('/api/<command>/<option>/<option2>', method=['GET','POST'])
def _api(command=None, option='', option2=''):
    response.headers['Content-Type'] = 'application/json'
    response.headers['Cache-Control'] = 'no-cache'
    query = json.merge_dicts(dict(request.forms), dict(request.query.decode()))
    apikey = query.pop('apikey', None) or request.headers.get('Authorization', '').replace('bearer', '').replace('Bearer', '').replace('BEARER', '').strip() or query.pop('sessionkey', None)
    payload = json.dc(request.json) or query
    # if not apikey:
    #     abort(401, 'Oops, Please check API specifictions')
    payload['option'] = option
    payload['option2'] = option2
    wapi = a.API(payload, apikey, request.environ, request.method.lower())
    func = getattr(wapi, "{}" .format(command), None)
    try:
        if callable(func):
            result = func()
            if result:
                return json.jc(result)
            else:
                return json.jc({})
    except KeyError as exc:
        traceback.print_exc()
        sleep(1)
        return json.jc({'status': False, 'message': 'Missing ' + str(exc)})
    except Exception as exc:
        traceback.print_exc()
        sleep(5)
        return json.jc({'status': False, 'message': 'Oops, please check API specifications...'})


#websocket
@route('/ws/api')
@route('/ws')
def handle_websocket():
    ws = request.environ.get('wsgi.websocket')
    if not ws:
        abort(400, 'Expected WebSocket request.')
    query = json.merge_dicts(dict(request.forms), dict(request.query.decode()))
    apikey = query.pop('apikey', None) or request.headers.get('Authorization', '').replace('bearer', '').replace('Bearer', '').replace('BEARER', '').strip() or query.pop('sessionkey', None) or query.pop('iframekey', None)
    while 1:
        message = None
        try:
            with Timeout(2, False) as timeout:
                message = ws.receive()
            if message:
                message = json.dc(message)
                apikey = apikey or message.pop('apikey','')
                if not apikey:
                    sleep(10)
                    break
                for command, payload in message.items():
                    wapi = a.API(json.dc(payload), apikey, request.environ, 'ws')
                    func = getattr(wapi, command, None)
                    if callable(func):
                        try:
                            x = func()
                            ws.send(json.jc({command:x}))
                        except KeyError as exc:
                            traceback.print_exc()
                            ws.send(json.jc({command: {'status': False, 'message': 'Missing '+str(exc)}}))
                        except Exception as exc:
                            traceback.print_exc()
                            sleep(5)
                            ws.send(json.jc({command:{'status': False, 'message':str(exc)}}))
        except WebSocketError:
            break
        except Exception as exc:
            traceback.print_exc()
            sleep(1)
    abort(400, 'Closed Connection.')


@get('/report/<name>')
def _report(name=None):
    report = r(name)
    return template(f'reports/{name}.tpl', name=name, meta=json.jc(report.meta), schema=report.schema)


@get('/testgrid')
def _testgrid():
    return template('reports/default.tpl')


@get('/sample')
def _sameple():
    r = Render('test',{})

    return template('templates/render/sample.tpl', route='TEST ROUTE', body=r.config().container().body)

@error(404)
@error(400)
def error400s(error):
    tpl = 'templates/error.tpl'
    return template(tpl, msg="ERROR : 404", body=error.body)


@error(500)
@error(502)
@error(501)
def error500s(error):
    tpl = 'templates/error.tpl'
    return template(tpl, msg="ERROR: 500, Oops...", body=error.body)