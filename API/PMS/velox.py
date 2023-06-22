from gevent import monkey
monkey.patch_all()
from gevent import sleep
from API.config import velox, sqlserver as ss
import os
from pathlib import Path
import urllib3
import API.njson as j
import ndjson
import API.dbms as db
import arrow
import traceback
import csv


CA = 'keys/sites-chain.pem'
# CA = '../../keys/sites-chain.pem'
upool = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=CA, num_pools=10, block=False, retries=1)
pids = [1438, 1404, 1446, 1429, 1442, 1489, 1425, 1405, 1413, 1402, 1450, 1464, 1401, 1444, 1408, 1398, 1485, 1403, 1458, 1455, 1382, 1406, 1381, 1439, 1441, 1440, 1498, 1448, 1407, 1414, 1410, 1396, 1486, 1449, 1443, 1409, 1068, 1431, 1426, 1397, 1380, 1453, 1435, 1447, 1434, 1411, 1436, 1379, 1400, 1432, 1399, 1020, 1430, 1433, 1412, 1454, 1606, 1605]
pids.sort()

class API:
    def __init__(self, qa=False):

        self.root = str(Path.home())+'/dataload/'
        self.filename = ''
        self.pre_url = 'https://ds-prod.tx24sevendev.com/v1'
        if qa:
            self.pre_url = 'https://ds-test.tx24sevendev.com/v1'

        self.headers = {
            'Cookie': 'authToken=###########'
        }
        self.authorization()

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


    def insurance_carriers(self): return self.datastream('insurance_carriers')
    def image_metadata(self): return self.datastream('image_metadata')
    def patient_recall(self): return self.datastream('patient_recall')
    def operatory(self): return self.datastream('operatory')
    def procedure_codes(self): return self.datastream('procedure_codes')
    def patients(self): return self.datastream('patients')
    def providers(self): return self.datastream('providers')
    def appointments(self): return self.datastream('appointments')
    def treatments(self): return self.datastream('treatments')
    def ledger(self): return self.datastream('ledger')


    def datastream(self, path):
        self.table = f'{path}'
        self.url = f"{self.pre_url}/private/datastream/{path}"
        return self.transmit(self.url)

    def stream(self, url, meta=None):
        self.headers['Accept'] = 'application/x-ndjson'
        self.headers['Content-Type'] = 'application/json'
        if meta:
            meta = j.jc(meta)
            for each in upool.request('POST', url, body=meta, headers=self.headers, retries=3, preload_content=False):
                yield each

    def transmit(self, url, meta=None, mode='GET'):
        if meta:
            if mode == 'GET':
                r = upool.request(mode, url, body=j.jc(meta), headers=self.headers, retries=3)
            else:
                # meta = j.dumps(meta).encode('utf-8')
                meta = j.jc(meta)
                r = upool.request(mode, url, body=meta, headers=self.headers, retries=3)
        else:
            r = upool.request(mode, url, headers=self.headers, retries=3)
        try:
            return r.data.decode()
        except ValueError as exc:
            return False

    def load_split_files(self, table, start="2001-01-01T00:00:00.000Z", reload=False):
        self.table = table
        curpath = os.path.abspath(os.curdir)
        print("Current path is: %s" % (curpath))
        def cleanup(val):
            val = str(val)
            if val == 'None':
                return ''
            if val[10]=='T' and val[19]=='.' and val[-1] =='Z':
                val = arrow.get(val).format('YYYY-MM-DD hh:mm:ss')
            return val

        for pid in pids:
            self.filename = f'dbo.vx_{self.table}-{pid}.csv'
            with open(self.root+self.filename, 'w') as f:
                cw = csv.writer(f)
                print(pid)
                meta = {
                    "practice": {
                        "id": pid,
                        "fetch_modified_since": start
                    },
                    "version": 1,
                    "data_to_fetch": {
                        f"{self.table}": {"records_per_entity": 5000}
                    }}
                for s in self.stream('https://ds-prod.tx24sevendev.com/v1/private/datastream', meta=meta):
                    x = ndjson.loads(s)
                    for p in x:
                        for i in p.get('data',[]):
                            l = [cleanup(_) for _ in list(i.values())]
                            l.insert(1, pid)
                            cw.writerow(l)
            self.create_table(self.table)
            self.load_bcp_db()

    def load_tmp_file(self, table, start="2001-01-01T00:00:00.000Z", reload=False):
        self.table = table
        self.filename = f'dbo.vx_{self.table}.csv'
        def cleanup(val):
            val = str(val)
            if val == 'None':
                return ''
            try:
                if val[10]=='T' and val[19]=='.' and val[-1] =='Z':
                    val = arrow.get(val).format('YYYY-MM-DD hh:mm:ss')
            except:
                return val
            return val
        try:
            with open(self.root+self.filename, 'w') as f:
                cw = csv.writer(f, delimiter='|')
                for pid in pids:
                    print(pid)
                    meta = {
                        "practice": {
                            "id": pid,
                            "fetch_modified_since": start
                        },
                        "version": 1,
                        "data_to_fetch": {
                            f"{self.table}": {"records_per_entity": 5000}
                        }}
                    for s in self.stream('https://ds-prod.tx24sevendev.com/v1/private/datastream', meta=meta):
                        x = ndjson.loads(s)
                        for p in x:
                            for i in p.get('data', []):
                                l = [cleanup(_) for _ in list(i.values())]
                                l.insert(1, pid)
                                cw.writerow(l)
        except:
            sleep(10)
            self.load_tmp_file(table, start)
        if reload:
            self.drop_table()
        self.create_table(self.table)
        self.load_bcp_db()

    def load_bcp_db(self, table= ''):
        if table:
            self.table = table
        if not self.filename:
            self.filename = f'dbo.vx_{self.table}.csv'
        # bcp = f'/opt/mssql-tools/bin/bcp gen4_dw.dbo.vx_{self.table} in "{self.root}{self.filename}" -S {ss.server} -U {ss.user} -P {ss.password} -e "{self.root}error.txt" -h TABLOCK -q -c -t "," '
        bcp = f'/opt/mssql-tools/bin/bcp gen4_dw.dbo.vx_{self.table} in "{self.root}{self.filename}" -b 5000 -S {ss.server} -U {ss.user} -P {ss.password} -e "{self.root}error.txt" -h TABLOCK -a 16384 -q -c -t "|" '
        print(bcp)
        os.system(bcp)
        return self

    def drop_table(self):
        PSQL = f''' DROP TABLE dbo.vx_{self.table}; '''
        db.execute(PSQL)
        return self

    def delete_file(self):
        os.remove(self.filename)
        return self

    def create_table(self, tablename):
        try:
            print(tablename)
            x = j.dc(self.datastream(tablename))
            txt = ''
            for col in x['properties']['fields']['items']['enum']:
                if col == 'id':
                    txt = f'''IF NOT EXISTS (select * from sysobjects where name='vx_{tablename}' and xtype='U') CREATE TABLE dbo.vx_{tablename} 
                    (id bigint, practice_id int, '''
                elif '_id' in col:
                    txt += f'{col} varchar(255),'
                elif col in ('duration', 'status', 'tx_status'):
                    txt += f'{col} INT,'
                elif col in ('amount','cost','co_pay'):
                    txt += f'{col} DECIMAL(19, 4),'
                elif col in ('',):
                    txt += f'{col} DECIMAL(19, 4),'
                elif 'date' in col:
                    txt += f'{col} DATETIME2,'
                elif col in ('dob'):
                    txt += f'{col} DATETIME2,'
                else:
                    txt += f'{col} varchar(255),'
            txt = txt[:-1]+f''');'''
            db.execute(txt)
            db.execute(f'''CREATE UNIQUE INDEX ux_{tablename}_pid ON dbo.vx_{tablename}  (practice_id, id); ''')
        except:
            print(x)
            traceback.print_exc()
        return self

def reset():
    import time
    start = time.perf_counter()
    # v.table = 'appointments'
    # v.load_tmp_file()
    # v.load_bcp_db()
    tables = ('ledger', 'treatments', 'appointments', 'patients', 'image_metadata', 'providers', 'insurance_carriers', 'patient_recall', 'operatory', 'procedure_codes', 'image_metadata',)
    for t in tables:
        v = API()
        v.load_tmp_file(t, reload=True)
    print(f'IT TOOK: {time.perf_counter() - start}')
    return

def scheduled(interval):
    import time
    start = time.perf_counter()
    # v.table = 'appointments'
    # v.load_tmp_file()
    # v.load_bcp_db()
    x = arrow.now().shift(hours=-interval).format('YYYY-MM-DD[T]HH:mm:ss.SSS[Z]')
    tables = (
    'ledger', 'treatments', 'appointments', 'patients', 'image_metadata', 'providers', 'insurance_carriers',
    'patient_recall', 'operatory', 'procedure_codes', 'image_metadata',)
    for t in tables:
        v = API()
        v.load_tmp_file(t, start=x)
    print(f'IT TOOK: {time.perf_counter() - start}')
    return

if __name__=='__main__':
    os.chdir('../../')
    reset()
    #45052.6 rows per sec.
    # v.create_split_files()
    # v.filename = 'dbo.vx_ledger.csv'
    # print(' varchar(256), '.join(j.dc(v.providers())['properties']['fields']['items']['enum']))
    # v.load_bcp_db()





