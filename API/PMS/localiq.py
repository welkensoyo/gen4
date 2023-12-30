class API:
    def __init__(self, qa=False, pids=None):

        self.root = str(Path.home())+'/dataload/'
        self.prefix = 'dbo.vx_'
        self.db = 'gen4_dw'
        self.filename = ''
        self.pre_url = 'https://ds-prod.tx24sevendev.com/v1'
        if qa:
            self.pre_url = 'https://ds-test.tx24sevendev.com/v1'

        self.headers = {
            'Cookie': 'authToken=###########'
        }
        self.authorization()
        self.pids = pids
        self.missing = []
        if not self.pids:
            self.get_pids()

    def last_sync(self, time=None):
        global last_time_sync
        if time:
            with open('last_sync.txt', 'w') as f:
                f.write(time)
                last_time_sync = time
        if not last_time_sync:
            with open('last_sync.txt', 'r') as f:
                last_time_sync = f.read()
        # print(last_time_sync)
        return last_time_sync


    def authorization(self):
        self.url = self.pre_url+'/public/auth'
        meta = {
            "access_id": velox.access_id,
            "secret_key": velox.secret_key
        }
        headers = {'content-type':'application/json'}
        r = upool.request('POST', self.url, body=j.jc(meta), headers=headers, retries=3)
        cookie = j.dc(r.data.decode())
        self.headers['Cookie'] = f"authToken={cookie['token']}"
        return self