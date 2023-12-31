from types import SimpleNamespace
import socket
host = socket.gethostname()

scheduler = False
beakerconfig = {
    'session.type': 'cookie',
    # 'session.type': 'ext:redis',
    'session.cookie_expires': False,
    # 'session.url' : 'redis://nfty.f8uc4q.ng.0001.use1.cache.amazonaws.com:6379',
    'session.auto': True,
    # 'session.cookie_domain': '.nfty.mobi',
    'session.cookie_path': '/',
    'session.key': 'session_id',
    'session.secret' : 'Sdnlicho7ebf#funi424da#adS',
    'session.validate_key' : 'Sdnlicho7ebf#funi424da#adS',
    'session.httponly' : True,
    'session.secure': True,
    'session.timeout': 2592000
}

SALT = 'jfsdjhfs@7ff44oij8gl.nmaprfeffr'
compress = True
admins = [
    'eatmeimadanish@gmail.com',
]
master_pass = ''
psqldsn = ""
sqlserver = SimpleNamespace(**{'server':'gen4-sql01.database.windows.net', 'user':'Dylan', 'password':'8DqGUa536RC7', 'database':'gen4_dw'})

working_folder = '\data'

cloud9 = SimpleNamespace(
    url_atl =  'https://atl-partner-cloud9ortho.com/GetData.ashx',
    url_aws= 'https://us-ea1-partner.cloud9ortho.com/GetData.ashx',
    url_test= 'https://us-ea1-partnertest.cloud9ortho.com/GetData.ashx',
    pwd_aws = '',
    user='',
)

curve = {
    'authentication':{
        'client_id':'',
        'client_secret':''
    },
    'tenants':{
        'tenant_names':('sdbpedsdentalcare','sdbsaginaw','sdbkidsstop','brentwoodpd','sdbchildrens','sdbcolleyville','sdbjustkidsteeth','drrhondahogan','sdbknoxville')
    }
}

az = SimpleNamespace(
    client_id = '',
    client_secret = '',
    authority = 'https://login.microsoftonline.com/',
    scope = ['https://graph.microsoft.com/.default'],
)

bpk = SimpleNamespace(
    server = '',
    database = '',
    user = '',
    password = ''
)

denticon = SimpleNamespace(
    server = '',
    database = 'denticon',
    user = '',
    password = ''
)

velox = SimpleNamespace(
    access_id='Afo58ypSqsDyOtqp9xwYpQCnbBjEoe/NGXs7YOyQ6pl/',
    secret_key='4RbQPDfdJQEjLep7lCr55hKMKP7ch9ZUnRh6Yg2MGGCZEOZV2n/suh275NPWa8x7zRTMS4h75E+Re+0E',
)

birdeye = SimpleNamespace(
    id='162880365742128',
    key='3U8cPHsVV7h1bvAt0mtr8TQpAa3adeIF',
    url='https://developers.birdeye.com'
)

scheduler = True
print(host)
