from gevent import monkey, joinall, spawn, sleep
monkey.patch_all()
import urllib3
import json
import ndjson
import traceback
from pathlib import Path

CA = 'keys/sites-chain.pem' # where you contain private SSL cert
upool = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=CA, num_pools=10, block=False, retries=1)

class Velox:
    def __init__(self, qa=False):
        self.root = str(Path.home()) + '/dataload/'
        self.pre_url = 'https://ds-prod.tx24sevendev.com/v1'
        self.access_id = 'ACCESSID' #however you pull your creds
        self.secret_key = 'SECRET'
        self.table = ''
        self.next = ''
        if qa:
            self.pre_url = 'https://ds-test.tx24sevendev.com/v1'

        self.headers = {
            'Cookie': 'authToken=###########'
        }
        self.authorization() #get your auth header

    def _next(self, next):
        #where you store your last synced value and how you want to manage that do here
        if next:
            self.next=next
        if not self.next:
            #pull from a saved file or database entry of the next timestamp so it lives after relaunch
            pass
        return self.next

    def authorization(self):
        self.url = self.pre_url + '/public/auth'
        meta = {
            "access_id": self.access_id,
            "secret_key": self.secret_key
        }
        headers = {'content-type': 'application/json'}
        r = upool.request('POST', self.url, body=j.jc(meta), headers=headers, retries=3)
        cookie = json.loads(r.data.decode())
        self.headers['Cookie'] = f"authToken={cookie['token']}"
        return self

    def stream(self, url, meta=None):
        self.headers['Accept'] = 'application/x-ndjson'
        self.headers['Content-Type'] = 'application/json'
        if meta:
            meta = json.dumps(meta)
            try:
                with upool.request('POST', url, body=meta, headers=self.headers, retries=3, preload_content=False) as each:
                    each.auto_close = False
                    self._next(each.headers.get('X-Next-Timestamp'))
                    yield each.data
            except:
                # log your error
                yield {}

    def get_stream(self, pid):
        meta = {
            "practice": {
                "id": int(pid),
                "fetch_modified_since": self.next
            },
            "version": 1,
            "data_to_fetch": {
                f"{self.table}": {"records_per_entity": 5000}
            }}
        for s in self.stream(self.pre_url+'/private/datastream', meta=meta):
            try:
                x = ndjson.loads(s)
                self.save_data(x, pid)
                sleep(0) #this allows other gevent async processes to grab the next context between streams
            except:
                traceback.print_exc()
                continue

    def save_data(self, data, practice_id):
        #do something with the data here
        return self


#async example:
if __name__ == '__main__':
    v = Velox()
    threads = []
    practice_id = '00001'
    for i in ('treatments', 'ledger'):
        threads.append(spawn(v.get_stream(practice_id)))
    joinall(threads)
