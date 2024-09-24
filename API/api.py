from gevent import spawn
import API.reports as r
from bottle import abort
from API.njson import dc, lc, checkuuid, b64e, jc, loads
import arrow
import API.cache as cache
from API.clinic import API as Practice
from API.PMS.birdeye import API as Birdeye
from API.tracking import report_clicked
import API.PMS.velox as velox

def keygen():
    import uuid
    return uuid.uuid4()

def merge_dicts(*args):
    result = {}
    for dictionary in args:
        result.update(dictionary)
    return result

def _master(f):
    def d(self):
        if self.apikey == '8725c240-f1bc-41df-87c5-b9738b3cc75a':
            return f(self)
        return {}
    return d

class API:

    def _response(self, id, message, method):
        if self.option == 'raw' or self.option2 == 'raw':
            self.option2 = ''
            return message
        try:
            return {'status':message.pop('status', True),'id':id,'message':message, 'method':method}
        except (TypeError, AttributeError): #handle when message is list
            return {'status': False if isinstance(message, (str)) else True, 'id': id, 'message': message, 'method': method}

    def __init__(self, payload, apikey, environ, mode):
        self.r = r.Report
        self.mode = mode
        self.apikey = apikey
        self.environ = environ
        self.pl = payload
        self.option = self.pl.pop('option', '').lower()
        self.option2 = self.pl.pop('option2', '').lower()
         #make dynamic query into database at some point for future gateways.
        try:
            self.request_details = {k: v for k, v in dict(self.environ).items() if isinstance(v, (str, int, list, dict, tuple))}
        except:
            self.request_details = {'mode':'Internal'}
        self.pl['ip'] = self.request_details.get('REMOTE_ADDR') or '127.0.0.1'
        if self.apikey not in ('8725c240-f1bc-41df-87c5-b9738b3cc75a','tracker'):
            abort(500, 'Oops please check API specs and try again...')
        if self.apikey == 'tracker' and self.option != 'clicked':
            abort(500, 'Oops please check API specs and try again...')

    # def login(self):
    #     if self.apikey:
    #         self.pl.update({'apikey':self.apikey})
    #     return User().login(self.pl)
    #
    # def logout(self):
    #     return User().logout()

    def _sesh(self):
        return dict(self.u._session())

    def session(self):
        expires = self.pl.get('expires',5)
        return {'session':self.c.session_create(expires)}

    def users(self):
        if self.option == 'new':
            return {'users':['blah']}

    def log(self):
        if self.option == 'flush':
            return cache.flush_log()
        return cache.retrieve_log(limit=self.pl.get('limit', 20))

    def report(self):
        name = self.pl['name']
        report = self.r(name)
        if 'meta' in self.pl:
            return report.update(self.pl['meta'])
        return report.meta or {}

    def hello(self):
        if self.mode == 'post':
            return {'hello':'world'}
        if self.mode =='get':
            return arrow.now().format('YYYY-MM-DD HH:mm')

    def azuser(self):
        import API.toolkits.azure as a
        if self.option == 'delete':
            return a.AzureSDB().delete_user(self.pl['email'])

    def paylocity(self):
        from API.paylocity import Paylocity
        p = Paylocity()
        if self.option=='employees':
            return p.employees()
        if self.option=='aztopay':
            p.az_pay()
            return [[u[8], u[6], 'U', u[2]] for u in cache.retrieve('aztopay') if u[2]]

    def removed_ad_users(self):
        return cache.removed_ad_user('get')

    def practices(self):
        p = Practice()
        if self.mode == 'post':
            return p.upsert(self.pl['table'])
        return p.get()

    def velox(self):
        from API.scheduling import everyhour
        if self.option == 'practices':
            return velox.API().practices().get_pids().pids
        elif self.option == 'reset':
            # spawn(velox.reset)
            return 'No longer supported...'
        elif self.option == 'sync':
            if everyhour.pause:
                return 'Sync already in progress...'
            spawn(velox.scheduled, self.pl.get('hour'))
            return 'Running...'
        elif self.option in ('stats',):
            return velox.stats()
        elif self.option in ('lastupdated','log'):
            return velox.log()
        elif self.option =='pause':
            everyhour.pause = True
        elif self.option =='resume':
            everyhour.pause = False
        elif self.option == 'fullrefresh':
            # spawn(velox.refresh, self.pl.get('pids'))
            return 'No Longer Supported...'
        elif self.option == 'refresh':
            table = self.option2
            pids = self.pl.get('pids')
            if pids == 'ALL':
                return 'Must Provide Practice IDs (action is canceled)...'
            spawn(velox.resync_table, table, pids)
            return 'Refreshing table: '+table
        return str(everyhour.pause)


    def birdeye(self):
        b = Birdeye()
        methods = {
            'search_business': b.search_business,
            'business_review': b.business_review,
            'employees': b.employees,
            'survey': b.survey,
            'conversations': b.employees,
            'reviews': b.reviews,
            'competitor': b.competitor,
            'search_child_business': b.search_child_business,
            'user': b.user
        }
        return methods[self.option](self.pl)

    def tracking(self):
        if self.option == 'clicked':
            method = self.pl.get('method')
            userid = self.pl.get('userid')
            action = self.pl.get('action')
            if method and userid and action:
                report_clicked(method, userid, action)
            return 'REPORT ACKNOWLEDGEMENT SUCCESSFUL'
