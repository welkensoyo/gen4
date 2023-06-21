import arrow
import base64
import zlib
import traceback

from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes

import API.njson as json
from API.config import SALT

#cryptography
def cryptkey(password=''):
    digest = hashes.Hash(hashes.SHA256(), backend=default_backend())
    digest.update(bytes(SALT+password, 'utf-8'))
    return Fernet(base64.urlsafe_b64encode(digest.finalize()))

def encrypt(meta, password=''):
    if not meta:
        return meta
    meta = json.jc(meta).encode()
    meta = zlib.compress(meta, 9)
    f = cryptkey(password)
    return base64.urlsafe_b64encode(f.encrypt(bytes(meta))).decode()

def decrypt(meta, password=''):
    if not meta:
        return meta
    meta = base64.urlsafe_b64decode(meta)
    f = cryptkey(password)
    meta = f.decrypt(bytes(meta))
    meta = zlib.decompress(meta)
    return json.loads(meta)



def create_session(apikey, expires):
    f = Fernet(SALT)
    meta = {'apikey':apikey, 'expires':arrow.get().shift(minutes=int(expires)).format()}
    return f.encrypt(json.jc(meta).encode()).decode()

def check_session(session):
    try:
        f = Fernet(SALT)
        x = json.dc(f.decrypt(session.encode()).decode())
        if arrow.get() < arrow.get(x['expires']):
            return x['apikey']
    except:
        traceback.print_exc()
    return False