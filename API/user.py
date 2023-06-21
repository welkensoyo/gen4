import traceback
from gevent import spawn, sleep
from bottle import request, response, redirect
import arrow
from ldap3 import Server, Connection, ALL, NTLM, ObjectDef, AttrDef, Reader, Entry, Attribute, OperationalAttribute
import njson as json

dns_servers = ['172.20.0.20', '172.20.0.19']


def check_login(fn):
    def check_uid(**kwargs):
        u = User()
        if u.status():
            return fn(**kwargs)
        u.logout()
        url = u.encode(f'{request.urlparts.path}?{request.query_string}')
        redirect(f"/login?redirect={url}")

    return check_uid


def check_admin(fn):
    def check_uid(**kwargs):
        u = User()
        if u.status() and u.role == 'Admin':
            return fn(**kwargs)
        elif u.status():
            redirect('/')
        redirect("/login")

    return check_uid


def check_it(fn):
    def check_uid(**kwargs):
        u = User()
        if u.status() and 'Information Systems' in u.group:
            return fn(**kwargs)
        elif u.status():
            redirect('/')
        redirect("/login")

    return check_uid


class User(object):
    def __init__(self):
        self.email = ''
        self.id = ''
        self.role = ''
        self.group = ''
        self.firstname = ''
        self.lastname = ''
        self.session = request.environ['beaker.session']
        self.__dict__.update(self.session)
    def parseurl(self, url):
        url = url.split('/')
        if len(url) == 7:
            return url[5]
        return None

    def status(self):
        return self.id or False

    def dicted(self):
        return {k: v for k, v in self.__dict__.items() if
                k not in ('session', '_expires', '_creation_time', '_accessed_time')}

    def set(self, **kwargs):
        for k, v in kwargs.items():
            self.session[k] = v
        self.__dict__.update(self.session)
        return self.save(meta=kwargs)

    def save(self, meta=None):
        # Update user database
        return self

    def adgroups(self):
        search_base = 'DC=sdbmail,DC=com'
        search_filter = f'(&(objectclass=user)(sAMAccountName={self.ad}))'
        s = Server(dns_servers[0], get_info=ALL)
        c = Connection(s, user=f'sdb\\{ldapuser.user}', password=ldapuser.password, authentication=NTLM,
                       auto_bind=True, check_names=True, auto_referrals=False)
        c.search(search_base, search_filter, attributes=['memberOf'])
        try:
            self.set(groups=[group for i in c.response[0]['attributes']['memberOf'] for _, group in
                             [i.split(',')[0].split('=')]])
        except:
            self.set(groups=[])
        return self.groups


    def logout(self):
        try:
            self.session.delete()
            sleep(1)
            self.session.delete()
        except:
            pass

    def ntlogin(self, user, password):
        if 'wldd\\' in user:
            domain_user = user
            user = domain_user.split('\\')[1]
        else:
            domain_user = f'wldd\\{user}'
        try:
            s = Server(dns_servers[0], get_info=ALL)
            c = Connection(s, user=domain_user, password=password, authentication=NTLM)
            c.bind()
            if c.result['result'] == 0:
                PSQL = sql.csql['userid'] + ' or hp.login = %s'
                id = dbsql.fetchone(PSQL, user, user)[0]
                self.idlogin(str(id))
                self.adgroups()
                return self.dicted()
        except Exception as exc:
            traceback.print_exc()
        return False

    def authlogin(self, altuser):
        altuser = json.dictcheck(altuser)
        f = self.checkToken(altuser['token']) or redirect('/logout')
        meta = {
            'source': altuser.get('origin') or 'firebase',
            'login': f['email'],
            'avatar': f.get('picture'),
            'fuid': f['uid'],
            'email_verified': f['email_verified'],
            'mention': f.get('name') or f['email'].split('@')[0]
        }
        PSQL = '''SELECT id, meta FROM website.users WHERE meta->>'email' = %s'''
        login = db.Pcursor().fetchone(PSQL, meta['login'])
        if not login:
            return False, meta['login']
            # PSQL = ''' INSERT INTO website.users (mention, meta) VALUES (%s, %s) RETURNING id;'''
            # userid = db.Pcursor().fetchone(PSQL, meta['mention'].replace(' ',''), json.dumps(meta))[0]
            # meta['id'] = userid
            # self.set(**meta)
            # self.setcookie({'id': userid})
            # self.save()
            # return self.id, meta
        self.set(id=login[0])
        self.set(**json.dictcheck(login[1]))
        return self.id, meta

    def today(self):
        return arrow.get().format('MM/DD/YYYY')
