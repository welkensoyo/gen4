from gevent import sleep, spawn
from API.config import velox, sqlserver as ss
import os
from pathlib import Path
import urllib3
import API.njson as j
import ndjson
import API.dbms as db
import API.dbpyodbc as dbpy
import arrow
import traceback
import csv
import requests
import shlex, subprocess

full_tables = ('treatments', 'ledger',  'appointments', 'patients', 'providers', 'insurance_carriers', 'insurance_claim',
              'patient_recall', 'operatory', 'procedure_codes', 'image_metadata', 'clinic', 'referral_sources', 'payment_type',
              'patient_referrals', 'clinical_notes', 'perio_charts', 'perio_tooth', 'treatment_plan', 'insurance_groups', 'fee_schedule', 'fee_schedule_procedure')

nightly_tables = ('providers', 'insurance_carriers', 'insurance_claim', 'patient_recall', 'operatory', 'procedure_codes', 'image_metadata',
                  'clinic', 'referral_sources', 'payment_type','patient_referrals', 'clinical_notes', 'perio_charts', 'perio_tooth', 'treatment_plan',
                  'insurance_groups', 'fee_schedule', 'fee_schedule_procedure')

clinic_ids = {
    '1019': '370',
    '1020': '438',
    '1068': '396',
    '1379': '445',
    '1397': '398',
    '1398': '490',
    '1399': '441',
    '1406': '440',
    '1407': '594',
    '1414': '336',
    '1438': '42',
    '1436': '50',
    '1486': '525',
    '1588': '542',
    '1606': '443'
}

clinic_position = {
    'treatments': 16,
    'ledger': 12,
    'appointments': 12,
    'patients': 26,
}

CA = 'keys/sites-chain.pem'
# CA = '../../keys/sites-chain.pem'
upool = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=CA, num_pools=10, block=False, retries=1)

current = 'No Process Running...'
last_time_sync = None
current_sync = False

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
        if not self.pids or pids == 'ALL':
            self.get_pids()

    def get_pids(self):
        SQL = f'SELECT id FROM {self.prefix}practices'
        pids = db.fetchall(SQL)
        self.pids = [x[0] for x in pids]
        self.pids.sort()
        return self

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
    def treatment_plan(self): return self.datastream('treatment_plan')
    def insurance_groups(self): return self.datastream('insurance_groups')

    def available_appointments(self, pids=None, days=7):
        self.table = 'available_appointments'
        self.filename = f'{self.prefix}{self.table}.csv'
        if not pids:
            pids = self.pids
        elif isinstance(pids, (str, int)):
            pids = [int(pids)]
        count = 999
        with open(self.root + self.filename, 'w', newline='') as f:
            cw = csv.writer(f, delimiter='|', lineterminator='\n')
            for pid in pids:
                print(f'Appointments for {pid}')
                # db.execute('DELETE FROM dbo.vx_available_appointments WHERE practice_id = %s',pid)
                query = {
                    'query':
                        '''query ($practice_id: ID! $start_date: Date! $end_date: Date!) {
          practice(id:$practice_id) { 
            apptAvailability(start_date: $start_date, end_date:$end_date){
             time_slot
             operatory {
              id
              name
              pms_id
        }
        }
        }
        }
        ''',
        'variables': {
        'practice_id': pid,
        'start_date': arrow.get().format('YYYY-MM-DD[T]00:00:00.000[Z]'),
        'end_date': arrow.get().shift(days=int(days)).format('YYYY-MM-DD[T]00:00:00.000[Z]')
                }}
                appointments = self.graphql(query)
                appointments = appointments['data']['practice']['apptAvailability']
                for appointment in appointments:
                    # print(appointment['time_slot'])
                    tslot = arrow.get(appointment['time_slot'])
                    for category in appointment.keys():
                        if category == 'time_slot':
                            continue
                        if 6 <= int(tslot.format('HH')) < 20:
                            for p in appointment[category]:
                                count+=1
                                cw.writerow((count, pid, category, tslot.format('YYYY-MM-DD HH:mm:ss'), p['id'], p['name'], p['pms_id']))
                # print(result)
        db.execute('DELETE FROM dbo.vx_available_appointments')
        self.load_bcp_db()
        return self

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
        global last_time_sync
        global current_sync
        self.headers['Accept'] = 'application/x-ndjson'
        self.headers['Content-Type'] = 'application/json'
        if meta:
            meta = j.jc(meta)
            try:
                with upool.request('POST', url, body=meta, headers=self.headers, retries=3, preload_content=False) as each:
                    each.auto_close = False
                    next = each.headers.get('X-Next-Timestamp') or last_time_sync or "2001-01-01T00:00:00.000Z"
                    if not current_sync:
                        self.last_sync(next)
                        current_sync = True
                    yield each.data
            except:
                # traceback.
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

    def graphql(self, query):
        url = f"{self.pre_url}/private/graphql"
        r = requests.post(url, json=query, headers=self.headers,  verify=CA)
        return j.dc(r.text)

    def cleanup(self, val):
        val = str(val)
        if val == 'None':
            return ''
        try:
            if val[10] == 'T' and val[19] == '.' and val[-1] == 'Z':
                val = arrow.get(val).format('YYYY-MM-DD HH:mm:ss')
        except:
            return val
        return val

    def clinic_fix(self, l):
        if self.table in list(clinic_position.keys()):
            p = clinic_position[self.table]
            if str(l[1]) in clinic_ids:
                l[p] = clinic_ids[str(l[1])]
        return l


    def load_sync_files(self, table, start="2001-01-01T00:00:00.000Z", reload=False):
        print('SYNC TMP FILE')
        self.table = table
        try:
            print(f'Creating Folder {self.root + self.filename}')
            self.create_folder()
            reload_save = reload
            for pid in self.pids:
                data_empty = True
                self.filename = f'{self.prefix}{self.table}-{pid}.csv'
                reload = reload_save #this is in case 1400 or 1486 change the reload state
                global current
                upload_pid = pid
                sleep(0)
                print(pid)
                current = f'Syncing {pid} - {table}...'
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
                        # print(x)
                        sleep(0)
                    except:
                        traceback.print_exc()
                        continue
                    with open(self.root + self.filename, 'w', newline='') as f:
                        cw = csv.writer(f, delimiter='|', lineterminator='\n')
                        for p in x:
                            ids_to_delete = []
                            ia = ids_to_delete.append
                            for i in p.get('data', []):
                                data_empty = False
                                # if i.get('plan_id'):
                                #     print(i)
                                l = list(i.values())
                                ia(l[0])
                                l = [self.cleanup(_) for _ in l]
                                if int(pid) == 1400:
                                    l.insert(1, str(1486))
                                    upload_pid = '1486'
                                    reload = False
                                else:
                                    if int(pid) == 1486:
                                        print('1486 detected, setting to false')
                                        reload = False
                                    l.insert(1, pid)
                                l = self.clinic_fix(l)
                                cw.writerow(l)
                                sleep(0)
                            if ids_to_delete and not reload:
                                print(f'UPDATING {len(ids_to_delete)}')
                                db.execute(f'''DELETE FROM {self.prefix}{self.table} WHERE practice_id = %s AND id in ({','.join(map(str, ids_to_delete))}); ''', upload_pid)
                            else:
                                self.missing.append(pid)
                    if reload and not data_empty:
                        # self.authorization()
                        print(f'WIPING {upload_pid}')
                        db.execute(f'''DELETE FROM {self.prefix}{self.table} WHERE practice_id = %s; ''', upload_pid)
                    self.load_bcp_db()
        except:
            print('******** ERROR ********')
            traceback.print_exc()
            sleep(10)
            self.load_sync_files(table, start, reload=reload)



    def bulk_load(self, table, start="2001-01-01T00:00:00.000Z", reload=False):
        print('LOAD TMP FILE')
        print(start)
        reload_save = reload
        self.table = table
        self.filename = f'{self.prefix}{self.table}.csv'

        try:
            print(f'Creating Folder {self.root+self.filename}')
            self.create_folder()
            with open(self.root+self.filename, 'w', newline='') as f:
                cw = csv.writer(f, delimiter='|', lineterminator='\n')
                for pid in self.pids:
                    reload = reload_save
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
                            for i in p.get('data', []):
                                # print(i)
                                l = list(i.values())
                                l = [self.cleanup(_) for _ in l]
                                if int(pid) == 1400:
                                    l.insert(1, str(1486))
                                else:
                                    l.insert(1, pid)
                                cw.writerow(l)
                                sleep(0)
        except:
            traceback.print_exc()
            sleep(10)
            self.bulk_load(table, start, reload=reload_save)
        if reload:
            self.create_table()
            self.authorization()
        self.load_bcp_db()
        self.create_indexes()

    def delete_updated(self,ids):
        SQL = f'''DELETE FROM {self.prefix}{self.table} WHERE id in ({','.join(map(str, ids))}); '''
        db.execute(SQL)
        return self

    def load_bcp_db(self, table= ''):
        if table:
            self.table = table
        if not self.filename:
            self.filename = f'{self.prefix}{self.table}.csv'
        # print(self.filename)
        # bcp = f'/opt/mssql-tools/bin/bcp {self.db}.{self.prefix}{self.table} in "{self.root}{self.filename}" -S {ss.server} -U {ss.user} -P {ss.password} -e "{self.root}error.txt" -h TABLOCK -q -c -t "," '
        # bcp = f'/opt/mssql-tools/bin/bcp {self.db}.{self.prefix}{self.table} in "{self.root}{self.filename}" -b 50000 -S {ss.server} -U {ss.user} -P {ss.password} -e "{self.root}error.txt" -h TABLOCK -a 16384 -q -c -t "|" '
        bcp = f'/opt/mssql-tools/bin/bcp {self.db}.{self.prefix}{self.table} in "{self.root}{self.filename}" -b 50000 -S {ss.server} -U {ss.user} -P {ss.password} -e "{self.root}error.txt" -h TABLOCK -a 16384 -q -c -t "|" ; rm "{self.root}{self.filename}" '
        # print(bcp)
        os.popen(bcp)
        # subprocess.Popen(shlex.split(bcp))
        # p1 = subprocess.Popen(shlex.split(bcp), stdout=subprocess.PIPE)
        # p2 = subprocess.Popen(fd, stdin=p1.stdout, stdout=subprocess.PIPE); p1.stdout.close()
        return self

    def create_folder(self):
        p = str(Path.home()) + '/dataload'
        isExist = os.path.exists(p)
        if not isExist:
            os.makedirs(p)

    def drop_table(self):
        print(f'DROPPING TABLE {self.prefix}{self.table}')
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
                self.drop_table()
                for col in x['properties']['fields']['items']['enum']:
                    if col == 'id':
                        txt = f'''IF NOT EXISTS (select * from sysobjects where name='vx_{self.table}' and xtype='U') CREATE TABLE {self.prefix}{self.table} 
                        (id bigint, practice_id int, '''
                    elif col in ('_id','referral_date'):
                        txt += f'{col} varchar(255),'
                    elif col in ('duration', 'status', 'tx_status'):
                        txt += f'{col} INT,'
                    elif col in ('plan_id', 'insurance_id', 'guarantor_id'):
                        txt += f'{col} BIGINT,'
                    elif col in ('amount','cost','co_pay'):
                        txt += f'{col} DECIMAL(19, 4),'
                    elif col in ('',):
                        txt += f'{col} DECIMAL(19, 4),'
                    elif 'date' in col:
                        txt += f'{col} DATETIME2,'
                    elif col in ('dob',):
                        txt += f'{col} DATETIME2,'
                    elif col in ('note', 'notes'):
                        txt += f'{col} varchar(max),'
                    else:
                        txt += f'{col} varchar(255),'
                txt = txt[:-1]+f''');'''
                db.execute(txt)
        except:
            traceback.print_exc()
            pass
        return self

    def create_indexes(self):
        db.execute(f'''CREATE UNIQUE INDEX ux_{self.table}_pid ON {self.prefix}{self.table}  (practice_id, id) with ignore_dup_key; ''')
        if self.table in ('appointments'):
            db.execute(f'''CREATE INDEX ix_{self.table}_clinic_id ON {self.prefix}{self.table}  (clinic_id, practice_id); ''')
        elif self.table == 'ledger':
            db.execute('''CREATE NONCLUSTERED INDEX [ix_ledger_clinic_id] ON [dbo].[vx_ledger]
        ( [clinic_id] ASC ) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [ix_vx_ledger_patient_id] ON [dbo].[vx_ledger]
        ( [patient_id] ASC ) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [ix_vx_ledger_payment_class] ON [dbo].[vx_ledger]
        ( [payment_class] ASC ) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [ix_vx_ledger_practice_id] ON [dbo].[vx_ledger]
        ( [practice_id] ASC ) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
        ''')

        elif self.table == 'treatments':
            db.execute('''CREATE NONCLUSTERED INDEX [ix_treatments_clinic_id] ON [dbo].[vx_treatments]
        ([clinic_id] ASC)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [ix_treatments_completion_date] ON [dbo].[vx_treatments]
        ( [completion_date] ASC ) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [ix_treatments_patient_id] ON [dbo].[vx_treatments]
        ( [patient_id] ASC ) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [ix_treatments_tx_status] ON [dbo].[vx_treatments]
        ( [tx_status] ASC ) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
        ''')

def correct_ids():
    print("Correcting IDs")

    SQL = '''
    UPDATE dbo.vx_ledger SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
    UPDATE dbo.vx_patients SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
    UPDATE dbo.vx_treatments SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
    UPDATE dbo.vx_appointments SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
    '''
    spawn(db.execute, SQL)
    global current
    current = 'No sync in progress...'
    return


def correct_ids_local():
    print("Correcting IDs")

    SQL = '''
    UPDATE dbo.vx_ledger SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
    UPDATE dbo.vx_patients SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
    UPDATE dbo.vx_treatments SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
    UPDATE dbo.vx_appointments SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
    '''
    db.execute(SQL)
    # changes = [(1379,445),
    #            (1019,370),
    #            (1020,438),
    #            (1068,396),
    #            (1397,398),
    #            (1398,490),
    #            (1399,441),
    #            (1406,440),
    #            (1407,594),
    #            (1414,336),
    #            (1486,525),
    #            (1588,542),
    #            (1606,443)]
    # SQL = ''
    # for table in ('treatments', 'appointments', 'ledger'):
    #     for change in changes:
    #         SQL += f"UPDATE dbo.vx_{table} SET clinic_id = '{change[1]}' WHERE practice_id = '{change[0]}' AND (clinic_id != '42' or clinic_id IS NULL); "
    # print(SQL)
    # db.execute(SQL)
    return


def last_updated(table='ledger'):
    t = {'ledger':'transaction_date', 'practices':'last_sync'}
    SQL = f'SELECT TOP 1 {t[table]} FROM dbo.vx_{table} WHERE {t[table]} <= GETDATE() '
    return db.fetchone(SQL)[0]

def reset(tables=None, practice=True):
    error = ''
    from API.scheduling import everyhour
    missing = {}
    try:
        everyhour.pause = True
        print(arrow.get().format('YYYY-MM-DD HH:mm:ss'))
        import time
        start = time.perf_counter()
        print('Updating practices')
        if practice:
            API().practices()
        if not tables:
            tables = full_tables
        for t in tables:
            print(t)
            v = API()
            v.bulk_load(t, reload=True)
            missing[t] = v.missing
        print(f'IT TOOK: {time.perf_counter() - start}')
        correct_ids()
    except:
        error = traceback.format_exc()
    if not error and missing:
        error = j.jc(missing)
    spawn(log, mode='full', error=str(error))
    everyhour.pause = False
    return tables

def reset_table(tablename):
    import time
    start = time.perf_counter()
    # print('Updating practices')
    # API().practices()
    x = API()
    x.bulk_load(tablename, reload=True)
    print(x.missing)
    correct_ids()
    print(f'IT TOOK: {time.perf_counter() - start}')
    return

def refresh_table(tablename, pids=None):
    print(tablename)
    print(pids)
    import time
    from API.scheduling import everyhour
    everyhour.pause = True
    start = time.perf_counter()
    # print('Updating practices')
    # API().practices()
    try:
        x = API()
        if pids:
            pids = pids.split(',')
            x.pids = pids
        x.load_sync_files(tablename, reload=True)
        correct_ids_local()
    except:
        error = traceback.format_exc()
    print(f'IT TOOK: {time.perf_counter() - start}')
    everyhour.pause = False
    global current
    current = f'No Sync In Progress... last sync took {time.perf_counter() - start} seconds...'
    return

def refresh(pids=None):
    import time
    from API.scheduling import everyhour
    start = time.perf_counter()
    error = ''
    everyhour.pause = True
    try:
        print('Updating practices')
        API().practices()
        x = API()
        if pids:
            pids = pids.split(',')
            x.pids = pids
        for table in full_tables:
            x.load_sync_files(table, reload=True)
        correct_ids()
    except:
        error = traceback.format_exc()
    everyhour.pause = False
    log(mode='full', error=str(error))
    print(f'IT TOOK: {time.perf_counter() - start}')
    global current
    current = f'No Sync In Progress... last sync took {time.perf_counter() - start} seconds...'
    return

def scheduled(interval=None):
    global current_sync
    error = ''
    from API.scheduling import everyhour
    try:
        everyhour.pause = True
        import time
        start = time.perf_counter()
        if interval:
            x = arrow.get().shift(hours=-int(interval)).format('YYYY-MM-DD[T]HH:mm:ss.SSS[Z]')
        elif not last_time_sync:
            x = arrow.get().shift(hours=-int(24)).format('YYYY-MM-DD[T]HH:mm:ss.SSS[Z]')
        else:
            x = last_time_sync
        print('Updating practices...')
        API().practices()
        print(x)
        tables = ('treatments', 'ledger', 'appointments', 'patients', 'treatment_plan', 'providers')
        for t in tables:
            try:
                # print(t)
                API().load_sync_files(t, start=x)
            except:
                error = traceback.format_exc()
        correct_ids()
        print(f'IT TOOK: {time.perf_counter() - start}')
        global current
        current = f'No Sync In Progress... last sync took {time.perf_counter() - start:.2f} seconds...'
    except:
        error = traceback.format_exc()
    log(mode='sync', error=error)
    everyhour.pause = False
    current_sync = False
    return

def log(mode=None, error=''):
    try:
        if mode:
            SQL = "UPDATE dbo.vx_log SET last_sync=GETDATE() AT TIME ZONE 'Central Standard Time', error=? WHERE [mode] = ?"
            return dbpy.execute(SQL, error, mode)
        SQL = "SELECT mode, CONVERT(VARCHAR, last_sync, 120), error FROM dbo.vx_log"
        return [(x[0], arrow.get(x[1]).to('US/Central').format('YYYY-MM-DD HH:mm:ss'), x[2]) for x in dbpy.fetchall(SQL)]

    except:
        # traceback.print_exc()
        pass

def nightly():
    import time
    start = time.perf_counter()
    error = ''
    try:
        x = API()
        x.practices()
        for table in nightly_tables:
            refresh_table(table, pids=None)
    except:
        error = traceback.format_exc()
    spawn(log, mode='full', error=str(error))
    print(f'IT TOOK: {time.perf_counter() - start}')
    return

def reload_file(table):
    v = API()
    v.table = table
    v.filename = f'{v.prefix}{v.table}.csv'
    # v.create_table()
    # v.load_bcp_db()
    v.create_indexes()

if __name__ == '__main__':
    from pprint import pprint
    os.chdir('../../')
    # scheduled()
    # correct_ids_local()
    # reset_table('appointments')
    # reload_file('ledger')
    # v = API()
    # v.practices()
    # for table in ('image_metadata',):
    #     refresh_table(table, pids='1486,1400') #13
    # v.available_appointments()

    # reload_file('appointments')
    # for table in ('fee_schedule','fee_schedule_procedure', 'insurance_claim', 'payment_type'):
    #     reset_table(table)
    # reset_table('treatments')
    # reset_table('appointments')
    # reload_file('ledger')
    # refresh_table('ledger', pids='1447,1448,1449,1450,1453,1454,1455,1464,1485,1489,1498,1588,1589,1605,1606,1616,1617,1634,1706,1707,1708,1709,1710,1714,1717,1718,1720,1734,1761,1798,1925,1938,2019,2067,2068' )
    # bcp = '/opt/mssql-tools/bin/bcp gen4_dw.dbo.vx_image_metadata in "/home/nfty/dataload/dbo.vx_image_metadata-1019.csv" -b 50000 -S gen4-sql01.database.windows.net -U Dylan -P 8DqGUa536RC7 -e "/home/nfty/dataload/error.txt" -h TABLOCK -a 16384 -q -c -t "|"'
    # delfile = 'rm "/home/nfty/dataload/dbo.vx_image_metadata-1019.csv"'




