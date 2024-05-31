from API import njson as j
from API.comms import upool
class API:
    def __init__(self):
        self.access_token = None
        self.headers = {}

    def authorization(self, refresh_token=None):
        self.url = self.pre_url+'/api/oauth/token'
        meta = {
            "grant_type": "password",
            "username": peer.user,
            "password": peer.secret,
            "format": "json"
        }
        if refresh_token:
            meta['refresh_token'] = refresh_token
        headers = {'content-type':'application/json'}
        r = upool.request('POST', self.url, body=j.jc(meta), headers=headers, retries=3)
        cookie = j.dc(r.data.decode())
        self.headers['Cookie'] = f"authToken={cookie['token']}"
        return self