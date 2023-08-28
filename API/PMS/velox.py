from gevent import monkey, spawn
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
        if not self.pids:
            self.get_pids()

    def get_pids(self):
        SQL = f'SELECT id FROM {self.prefix}practices'
        pids = db.fetchall(SQL)
        self.pids = [x[0] for x in pids]
        return self

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


    def practices(self):
        error = ''
        try:
            txt = ''
            tablename = 'practices'
            self.url = f"{self.pre_url}/private/practices"
            x = j.dc(self.transmit(self.url))
            cols = list(x['practices'][0].keys())
            for col in cols:
                if col == 'id':
                    txt = f'''IF NOT EXISTS (select * from sysobjects where name='vx_{tablename}' and xtype='U') CREATE TABLE {self.prefix}{tablename} 
                        (id bigint, '''
                elif '_id' in col:
                    txt += f'{col} varchar(255),'
                elif col in ('duration', 'status', 'tx_status'):
                    txt += f'{col} INT,'
                elif col in ('amount', 'cost', 'co_pay'):
                    txt += f'{col} DECIMAL(19, 4),'
                elif col in ('',):
                    txt += f'{col} DECIMAL(19, 4),'
                elif 'date' in col:
                    txt += f'{col} DATETIME2,'
                elif col in ('dob','last_sync'):
                    txt += f'{col} DATETIME2,'
                else:
                    txt += f'{str(col)} varchar(255),'

            txt = txt[:-1] + f''');'''
            db.execute(f''' DROP TABLE {self.prefix}{tablename}; ''')
            db.execute(txt)
            db.execute(f'''CREATE UNIQUE INDEX ux_{tablename}_pid ON {self.prefix}{tablename}  (id) with ignore_dup_key; ''')
            vars = '%s,'*len(cols)
            PSQL = f'INSERT INTO {self.prefix}{tablename} VALUES ({vars[0:-1]})'
            for p in x['practices']:
                row = []
                for c in cols:
                    row.append(str(p[c]))
                db.execute(PSQL, *row)
        except:
            error = traceback.format_exc()
        log(mode='practices', error=str(error))
        return self


    def datastream(self, path):
        self.table = f'{path}'
        self.url = f"{self.pre_url}/private/datastream/{path}"
        return self.transmit(self.url)

    def stream(self, url, meta=None):
        self.headers['Accept'] = 'application/x-ndjson'
        self.headers['Content-Type'] = 'application/json'
        if meta:
            meta = j.jc(meta)
            try:
                for each in upool.request('POST', url, body=meta, headers=self.headers, retries=3, preload_content=False):
                    yield each
            except:
                yield {}

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

    def load_sync_files(self, table, start="2001-01-01T00:00:00.000Z", reload=False):
        print('LOAD TMP FILE')
        self.table = table
        self.filename = f'{self.prefix}{self.table}.csv'
        def cleanup(val):
            val = str(val)
            if val == 'None':
                return ''
            try:
                if val[10] == 'T' and val[19] == '.' and val[-1] == 'Z':
                    val = arrow.get(val).format('YYYY-MM-DD hh:mm:ss')
            except:
                return val
            return val

        try:
            print(f'Creating Folder {self.root + self.filename}')
            self.create_folder()
            for pid in self.pids:
                sleep(0)
                print(pid)
                meta = {
                    "practice": {
                        "id": int(pid),
                        "fetch_modified_since": start
                    },
                    "version": 1,
                    "data_to_fetch": {
                        f"{self.table}": {"records_per_entity": 5000}
                    }}
                for s in self.stream('https://ds-prod.tx24sevendev.com/v1/private/datastream', meta=meta):
                    try:
                        x = ndjson.loads(s)
                        sleep(0)
                    except:
                        break
                    with open(self.root + self.filename, 'w') as f:
                        cw = csv.writer(f, delimiter='|')
                        for p in x:
                            ids_to_delete = []
                            ia = ids_to_delete.append
                            for i in p.get('data', []):
                                l = list(i.values())
                                ia(l[0])
                                l = [cleanup(_) for _ in l]
                                if int(pid) == 1400:
                                    l.insert(1, str(1486))
                                else:
                                    l.insert(1, pid)
                                cw.writerow(l)
                            if ids_to_delete:
                                print(f'UPDATED {len(ids_to_delete)}')
                                db.execute(f'''DELETE FROM {self.prefix}{self.table} WHERE practice_id = %s AND id in ({','.join(map(str, ids_to_delete))}); ''', pid)
                    self.load_bcp_db()
        except:
            traceback.print_exc()
            sleep(10)
            self.load_sync_files(table, start)



    def load_tmp_file(self, table, start="2001-01-01T00:00:00.000Z", reload=False):
        print('LOAD TMP FILE')
        self.table = table
        self.filename = f'{self.prefix}{self.table}.csv'
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
            print(f'Creating Folder {self.root+self.filename}')
            self.create_folder()
            with open(self.root+self.filename, 'w') as f:
                cw = csv.writer(f, delimiter='|')
                for pid in self.pids:
                    sleep(0)
                    print(pid)
                    meta = {
                        "practice": {
                            "id": int(pid),
                            "fetch_modified_since": start
                        },
                        "version": 1,
                        "data_to_fetch": {
                            f"{self.table}": {"records_per_entity": 5000}
                        }}
                    for s in self.stream('https://ds-prod.tx24sevendev.com/v1/private/datastream', meta=meta):
                        try:
                            x = ndjson.loads(s)
                            sleep(0)
                        except:
                            break
                        for p in x:
                            ids_to_delete = []
                            ia = ids_to_delete.append
                            for i in p.get('data', []):
                                l = list(i.values())
                                ia(l[0])
                                l = [cleanup(_) for _ in l]
                                if int(pid) == 1400:
                                    l.insert(1, str(1486))
                                else:
                                    l.insert(1, pid)
                                cw.writerow(l)
                            if ids_to_delete and not reload:
                                print(f'UPDATED {len(ids_to_delete)}')
                                db.execute(f'''DELETE FROM {self.prefix}{self.table} WHERE practice_id = %s AND id in ({','.join(map(str, ids_to_delete))}); ''', pid)
        except:
            traceback.print_exc()
            sleep(10)
            self.load_tmp_file(table, start)
        if reload:
            self.create_table()
        self.load_bcp_db()

    def delete_updated(self,ids):
        SQL = f'''DELETE FROM {self.prefix}{self.table} WHERE id in ({','.join(map(str, ids))}); '''
        db.execute(SQL)
        return self

    def load_bcp_db(self, table= ''):
        if table:
            self.table = table
        if not self.filename:
            self.filename = f'{self.prefix}{self.table}.csv'
        # bcp = f'/opt/mssql-tools/bin/bcp {self.db}.{self.prefix}{self.table} in "{self.root}{self.filename}" -S {ss.server} -U {ss.user} -P {ss.password} -e "{self.root}error.txt" -h TABLOCK -q -c -t "," '
        bcp = f'/opt/mssql-tools/bin/bcp {self.db}.{self.prefix}{self.table} in "{self.root}{self.filename}" -b 5000 -S {ss.server} -U {ss.user} -P {ss.password} -e "{self.root}error.txt" -h TABLOCK -a 16384 -q -c -t "|" '
        # print(bcp)
        os.system(bcp)
        return self

    def create_folder(self):
        p = str(Path.home()) + '/dataload'
        isExist = os.path.exists(p)
        if not isExist:
            os.makedirs(p)

    def drop_table(self):
        PSQL = f''' DROP TABLE {self.prefix}{self.table}; '''
        db.execute(PSQL)
        return self

    def delete_file(self):
        os.remove(self.filename)
        return self

    def create_table(self):
        try:
            # print(tablename)
            x = j.dc(self.datastream(self.table))
            txt = ''
            if 'properties' in x:
                print('DROPPING TABLE')
                self.drop_table()
                for col in x['properties']['fields']['items']['enum']:
                    if col == 'id':
                        txt = f'''IF NOT EXISTS (select * from sysobjects where name='vx_{self.table}' and xtype='U') CREATE TABLE {self.prefix}{self.table} 
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
                    elif col in ('dob',):
                        txt += f'{col} DATETIME2,'
                    else:
                        txt += f'{col} varchar(255),'
                txt = txt[:-1]+f''');'''
                db.execute(txt)
                db.execute(f'''CREATE UNIQUE INDEX ux_{self.table}_pid ON {self.prefix}{self.table}  (practice_id, id) with ignore_dup_key; ''')
                if self.table in ('ledger', 'treatments', 'appointments'):
                    db.execute(f'''CREATE INDEX ix_{self.table}_clinic_id ON {self.prefix}{self.table}  (clinic_id); ''')
        except:
            traceback.print_exc()
        return self


def correct_ids():
    print("Correcting IDs")
    SQL = '''UPDATE dbo.vx_ledger SET clinic_id = '42' WHERE practice_id = '1438' AND clinic_id != '42';
UPDATE dbo.vx_patients SET clinic_id = '42' WHERE practice_id = '1438' AND clinic_id != '42';
UPDATE dbo.vx_treatments SET clinic_id = '42' WHERE practice_id = '1438' AND clinic_id != '42';
UPDATE dbo.vx_appointments SET clinic_id = '42' WHERE practice_id = '1438' AND clinic_id != '42';

UPDATE dbo.vx_ledger SET clinic_id = '50' WHERE practice_id = '1436' AND clinic_id != '50';
UPDATE dbo.vx_patients SET clinic_id = '50' WHERE practice_id = '1436' AND clinic_id != '50';
UPDATE dbo.vx_treatments SET clinic_id = '50' WHERE practice_id = '1436' AND clinic_id != '50';;
UPDATE dbo.vx_appointments SET clinic_id = '50' WHERE practice_id = '1436' AND clinic_id != '50';;

UPDATE dbo.vx_ledger SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
UPDATE dbo.vx_patients SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
UPDATE dbo.vx_treatments SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
UPDATE dbo.vx_appointments SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';'''
    spawn(db.execute, SQL)
    return


def correct_ids_local():
    print("Correcting IDs")
    SQL = '''UPDATE dbo.vx_ledger SET clinic_id = '42' WHERE practice_id = '1438' AND clinic_id != '42';
UPDATE dbo.vx_patients SET clinic_id = '42' WHERE practice_id = '1438' AND clinic_id != '42';
UPDATE dbo.vx_treatments SET clinic_id = '42' WHERE practice_id = '1438' AND clinic_id != '42';
UPDATE dbo.vx_appointments SET clinic_id = '42' WHERE practice_id = '1438' AND clinic_id != '42';

UPDATE dbo.vx_ledger SET clinic_id = '50' WHERE practice_id = '1436' AND clinic_id != '50';
UPDATE dbo.vx_patients SET clinic_id = '50' WHERE practice_id = '1436' AND clinic_id != '50';
UPDATE dbo.vx_treatments SET clinic_id = '50' WHERE practice_id = '1436' AND clinic_id != '50';
UPDATE dbo.vx_appointments SET clinic_id = '50' WHERE practice_id = '1436' AND clinic_id != '50';

UPDATE dbo.vx_ledger SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
UPDATE dbo.vx_patients SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
UPDATE dbo.vx_treatments SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
UPDATE dbo.vx_appointments SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
'''
    db.execute(SQL)
    return


def last_updated(table='ledger'):
    t = {'ledger':'transaction_date', 'practices':'last_sync'}
    SQL = f'SELECT TOP 1 {t[table]} FROM dbo.vx_{table} WHERE {t[table]} <= GETDATE() '
    return db.fetchone(SQL)[0]

def reset(tables=None, practice=True):
    error = ''
    from API.scheduling import everyhour
    try:
        everyhour.pause = True
        print(arrow.now().format('YYYY-MM-DD HH:mm:ss'))
        import time
        start = time.perf_counter()
        print('Updating practices')
        if practice:
            API().practices()
        if not tables:
            tables = ('ledger', 'treatments', 'appointments', 'patients', 'image_metadata', 'providers', 'insurance_carriers',
                  'patient_recall', 'operatory', 'procedure_codes', 'image_metadata', 'clinic', 'referral_sources',
                  'patient_referrals')
        for t in tables:
            print(t)
            API().load_tmp_file(t, reload=True)
        print(f'IT TOOK: {time.perf_counter() - start}')
        correct_ids()
    except:
        error = traceback.format_exc()
    spawn(log, mode='full', error=str(error))
    everyhour.pause = False
    return tables

def reset_table(tablename):
    import time
    start = time.perf_counter()
    print('Updating practices')
    API().practices()
    API().load_tmp_file(tablename, reload=True)
    correct_ids_local()
    print(f'IT TOOK: {time.perf_counter() - start}')
    return

def scheduled(interval):
    error = ''
    from API.scheduling import everyhour
    try:
        everyhour.pause = True
        import time
        start = time.perf_counter()
        x = arrow.now().shift(hours=-int(interval)).format('YYYY-MM-DD[T]HH:mm:ss.SSS[Z]')
        print('Updating practices...')
        API().practices()
        print(x)
        tables = ( 'ledger', 'treatments', 'appointments', 'patients', 'image_metadata', 'providers', 'insurance_carriers',
        'patient_recall', 'operatory', 'procedure_codes', 'image_metadata','clinic','referral_sources','patient_referrals')
        for t in tables:
            try:
                print(t)
                v = API()
                v.load_sync_files(t, start=x)
            except:
                error = traceback.format_exc()
        correct_ids_local()
        print(f'IT TOOK: {time.perf_counter() - start}')
    except:
        error = traceback.format_exc()
    everyhour.pause = False
    spawn(log, mode='sync', error=error)
    return

def log(mode=None, error=''):
    try:
        if mode:
            SQL = f'UPDATE dbo.vx_log SET last_sync=GETDATE(), error=%s WHERE [mode] = %s'
            return db.execute(SQL, error, mode)
        SQL = 'SELECT * FROM dbo.vx_log'
        return db.fetchall(SQL)
    except:
        traceback.print_exc()


if __name__=='__main__':
    os.chdir('../../')
    # reset(tables=('appointments', 'patients', 'image_metadata', 'providers', 'insurance_carriers',
    #           'patient_recall', 'operatory', 'procedure_codes', 'image_metadata', 'clinic', 'referral_sources',
    #           'patient_referrals'), practice=False)
    log(mode='practices', error='')





