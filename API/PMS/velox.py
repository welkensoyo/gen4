from gevent import sleep, spawn
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
import requests
import time
import shutil
from API.log import api_log as _log, velox_log as log, velox_stats as stats

sync_tables = ('procedure_codes', 'treatments', 'ledger', 'appointments', 'patients', 'treatment_plan', 'providers', 'fee_schedule', 'fee_schedule_procedure')

full_tables = ('treatments', 'ledger',  'appointments', 'patients', 'providers', 'insurance_carriers', 'insurance_claim',
              'patient_recall', 'operatory', 'procedure_codes', 'image_metadata', 'clinic', 'referral_sources', 'payment_type',
              'patient_referrals', 'clinical_notes', 'perio_chart', 'perio_tooth', 'treatment_plan', 'insurance_groups', 'fee_schedule', 'fee_schedule_procedure')


nightly_tables = ('clinic', 'providers', 'insurance_carriers', 'insurance_claim', 'patient_recall', 'operatory', 'procedure_codes','perio_tooth','perio_chart',
                  'referral_sources', 'payment_type','patient_referrals', 'clinical_notes', 'insurance_groups', 'fee_schedule', 'fee_schedule_procedure', 'image_metadata',)

cached_table_defs = {}

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
    def __init__(self, qa=False, pids=None, staging=True):
        self.root = str(Path.home())+'/dataload/'
        self.backup = str(Path.home())+'/backup/'
        self.prefix = 'restore.vx_'
        self.staging_prefix = 'staging.vx_'
        self.db = 'gen4_dw'
        self.filename = ''
        self.table = ''
        self.pid = ''
        self.url = ''
        self.import_file = []
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
        self.check_table_sync(None)
        self.staging_mode = staging
        if self.staging_mode:
            self.prefix = self.staging_prefix

    def authorization(self):
        self.headers = {
            'Cookie': 'authToken=###########'
        }

    def check_table_sync(self, tablename):
        if not tablename and not cached_table_defs:
            for each in sync_tables:
                cached_table_defs[each] = self.tables(tablename) or j.dc(self.datastream(each))
            return True
        elif not cached_table_defs.get(tablename):
            cached_table_defs[tablename] = self.tables(tablename) or j.dc(self.datastream(tablename))
            return True
        if tablename:
            table_def = j.dc(self.datastream(tablename))
            if cached_table_defs.get(tablename) != table_def:
                cached_table_defs[tablename] = table_def
                self.tables(tablename, meta=table_def)
                return False
        return True

    def tables(self, tablename, meta=None):
        if meta:
            SQL = '''DELETE FROM dev.api_tables WHERE tablename = %s'''
            db.execute(SQL, tablename)
            SQL = '''INSERT INTO dev.api_tables (tablename, meta) VALUES (%s, %s, getdate())'''
            db.execute(SQL, tablename, j.jc(meta))
            return meta
        SQL = f'SELECT meta FROM dev.api_tables WHERE tablename = %s'
        try:
            return db.fetchone(SQL, tablename)[0]
        except:
            return False

    def get_pids(self):
        SQL = f'SELECT id FROM dbo.vx_practices'
        pids = db.fetchall(SQL)
        self.pids = [x[0] for x in pids]
        # self.pids = [x[0] for x in pids if x[0] > 1442]
        self.pids.sort()
        return self

    def last_sync(self, time=None):
        global last_time_sync
        if time:
            time = arrow.get(time).shift(minutes=-16).format('YYYY-MM-DD[T]HH:mm:ss.SSS[Z]')
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

    def backup_file(self):
        try:
            shutil.copy(self.root+self.filename, self.backup+self.filename)
        except:
            traceback.print_exc()

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

    def available_appointments(self, pids=None, days=2):
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
        # 'end_date': arrow.get().shift(days=int(days)).format('YYYY-MM-DD[T]00:00:00.000[Z]')
        'end_date': arrow.get().format('YYYY-MM-DD[T]00:00:00.000[Z]')
                }}
                appointments = self.graphql(query)
                # print(appointments)
                if 'data' not in appointments:
                    sleep(5)
                    appointments = self.graphql(query)
                appointments = appointments['data']['practice']['apptAvailability']
                for appointment in appointments:
                    # print(appointment['time_slot'])
                    tslot = arrow.get(appointment['time_slot'])
                    for category in appointment.keys():
                        if category == 'time_slot':
                            continue
                        for p in appointment[category]:
                            count += 1
                            # print('writing row {}'.format(count))
                            cw.writerow((count, pid, category, tslot.format('YYYY-MM-DD HH:mm:ss'), p['id'], p['name'], p['pms_id']))
                        # if 6 <= int(tslot.format('HH')) < 20:
                        #     for p in appointment[category]:
                        #         count+=1
                        #         print('writing row {}'.format(count))
                        #         cw.writerow((count, pid, category, tslot.format('YYYY-MM-DD HH:mm:ss'), p['id'], p['name'], p['pms_id']))
                # print(result)
        db.execute('TRUNCATE TABLE staging.vx_available_appointments')
        self.load_bcp_db(_async=False)
        return self

    def procedure_code_lookup(self, pid):
        SQL = 'SELECT id, code FROM velox.vx_procedure_codes WHERE practice_id = %s'
        return {p: c for p, c in db.fetchall(SQL, pid)}

    def practices(self, run=True):
        print('PRACTICES')
        error = ''
        txt = ''
        tablename = 'practices'
        self.url = f"{self.pre_url}/private/practices"
        x = j.dc(self.transmit(self.url))
        print(x)
        cols = list(x['practices'][0].keys())
        print(cols)
        if run:
            try:
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

                txt = txt + f''' last_updated DATETIME2);'''
                db.execute(f''' DROP TABLE {self.prefix}{tablename}; ''')
                print('DROPPED TABLE')
                db.execute(txt)
                print('CREATED NEW TABLE')
                vars = '%s,'*(len(cols)+1)
                PSQL = f'INSERT INTO {self.prefix}{tablename} VALUES ({vars[0:-1]})'
                rows = []
                for p in x['practices']:
                    row = []
                    for c in cols:
                        if p[c]:
                            row.append(str(p[c]))
                        else:
                            row.append(None)
                    row.append(arrow.get().format('YYYY-MM-DD HH:mm:ss.SSSSSS'))
                    print(row)
                    rows.append(tuple(row))
                rows = tuple(rows)
                db.executemany(PSQL, rows)
                db.execute(f'''CREATE UNIQUE INDEX ux_{tablename}_pid ON {self.prefix}{tablename}  (id) with ignore_dup_key; ''')
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

    def clinic_fix(self, line):
        if self.table in list(clinic_position.keys()):
            p = clinic_position[self.table]
            if str(line[1]) in clinic_ids:
                line[p] = clinic_ids[str(line[1])]
            elif not line[p]:
                line[p] = 0
        return line

    def load_sync_files(self, table, start="0001-01-01T00:00:00.000Z", reload=False, verbose=False, _async=True, backup=False):
        print('SYNC TMP FILE')
        global current
        self.table = table
        if not self.check_table_sync(self.table):
            return reset_table(self.table, staging=self.staging_mode)
        try:
            print(f'Creating Folder {self.root + self.filename}')
            self.create_folder()
            reload_save = reload
            for pid in self.pids:
                self.pid = pid
                proc_codes = None
                if self.table == 'treatments':
                    proc_codes = self.procedure_code_lookup(pid)
                data_empty = True
                self.filename = f'{self.prefix}{self.table}_{pid}.csv'
                reload = reload_save #this is in case 1400 or 1486 change the reload state
                upload_pid = pid
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
                        if verbose:
                            print(x)
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
                                if self.table == 'treatments':
                                    l = l[:-1]
                                ia(l[0])
                                l = [self.cleanup(_) for _ in l]
                                if int(pid) == 1400:
                                    l.insert(1, str(1486))
                                    upload_pid = '1486'
                                    reload = False
                                else:
                                    if int(pid) == 1486:
                                        reload = False
                                    l.insert(1, pid)
                                l = self.clinic_fix(l)
                                l.append(arrow.get().format('YYYY-MM-DD HH:mm:ss.SSSSSS'))
                                if proc_codes and l[15]:
                                    l[13] = proc_codes.get(int(l[15]), None)
                                cw.writerow(l)
                                sleep(0)
                            if ids_to_delete and not reload:
                                print(f'UPDATING {len(ids_to_delete)}')
                                db.execute(f'''DELETE FROM {self.prefix}{self.table} WHERE practice_id = %s AND id in ({','.join(map(str, ids_to_delete))}); ''', upload_pid)
                            else:
                                self.missing.append(pid)
                    if reload and not data_empty:
                        # self.authorization()
                        print(f'WIPING {upload_pid} {self.table}')
                        db.execute(f'''DELETE FROM {self.prefix}{self.table} WHERE practice_id = %s; ''', upload_pid)
                            # db.execute(f'''DELETE FROM {self.prefix}{self.table} WHERE practice_id = %s; ''', upload_pid)
                    if backup:
                        self.backup_file()
                    self.load_bcp_db(_async=_async)
        except:
            print('******** ERROR ********')
            traceback.print_exc()
            sleep(10)
            self.load_sync_files(table, start, reload=reload, verbose=verbose, _async=_async)


    def bulk_reload(self, table, start="2001-01-01T00:00:00.000Z", reload=False):
        print('LOAD TMP FILE')
        print(start)
        self.table = table
        self.import_file = []
        try:
            print(f'Creating Folder {self.root+self.filename}')
            self.create_folder()
            for pid in self.pids:
                self.filename = f'{self.table}-{pid}.csv'
                self.import_file.append(self.filename)
                print(pid)
        except:
            pass
        if reload:
            self.create_table()
            self.authorization()
        self.load_bcp_bulk(_async=True)
        self.create_indexes()

    def bulk_bcp_reload(self, table, _async=True):
        self.table = table
        for pid in self.pids:
            self.filename = f'{self.table}_{pid}.csv'
            self.import_file.append(self.filename)
        self.load_bcp_bulk(_async=_async)

    def bulk_load(self, table, start="0001-01-01T00:00:00.000Z", reload=True):
        print('LOAD TMP FILE')
        print(start)
        reload_save = reload
        self.table = table
        print(self.table)
        self.import_file = []
        try:
            print(f'Creating Folder {self.root+self.filename}')
            self.create_folder()
            for pid in self.pids:
                print(pid)
                self.filename = f'{self.table}_{pid}.csv'
                self.import_file.append(self.filename)
                proc_codes = None
                if 'treatments' in self.table:
                    proc_codes = self.procedure_code_lookup(pid)
                reload = reload_save
                meta = {
                    "practice": {
                        "id": int(pid),
                        "fetch_modified_since": start
                    },
                    "version": 1,
                    "data_to_fetch": {
                        f"{table}": {"records_per_entity": 5000}
                    }}
                with open(self.root + self.filename, 'w', newline='') as f:
                    cw = csv.writer(f, delimiter='|', lineterminator='\n')
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
                                l = self.clinic_fix(l)
                                l.append(arrow.get().format('YYYY-MM-DD HH:mm:ss.SSSSSS'))
                                if proc_codes and l[15]:
                                    l[13] = proc_codes.get(int(l[15]), None)
                                cw.writerow(l)
                                sleep(0)
                self.backup_file()
        except:
            traceback.print_exc()
            sleep(10)
            self.bulk_load(table, start, reload=reload_save)
        if reload:
            print('RESETTING TABLE')
            self.create_table()
            self.authorization()
        self.load_bcp_bulk(_async=False)
        self.create_indexes()

    def delete_updated(self,ids):
        SQL = f'''DELETE FROM {self.prefix}{self.table} WHERE id in ({','.join(map(str, ids))}); '''
        db.execute(SQL)
        return self

    def load_bcp_db(self, table='', _async=True):
        if table:
            self.table = table
        if not self.filename:
            self.filename = f'{self.table}.csv'
        bcp = f'/opt/mssql-tools/bin/bcp {self.db}.{self.prefix}{self.table} in "{self.root}{self.filename}" -b 20000 -S {ss.server} -U {ss.user} -P {ss.password} -e "{self.root}{self.pid}_error.txt" -h TABLOCK -a 16384 -q -c -t "|" ; rm "{self.root}{self.filename}" '
        if self.staging_mode:
            bcp = f'/opt/mssql-tools/bin/bcp {self.db}.{self.staging_prefix}{self.table} in "{self.root}{self.filename}" -b 10000 -S {ss.server} -U {ss.user} -P {ss.password} -e "{self.root}staging_error.txt" -h TABLOCK -a 16384 -q -c -t "|"; rm "{self.root}{self.filename}" '
            # bcp = f'/opt/mssql-tools/bin/bcp {self.db}.{self.staging_prefix}{self.table} in "{self.root}{self.filename}" -b 10000 -S {ss.server} -U {ss.user} -P {ss.password} -e "{self.root}staging_error.txt" -h TABLOCK -a 16384 -q -c -t "|"; /opt/mssql-tools/bin/bcp {self.db}.{self.prefix}{self.table} in "{self.root}{self.filename}" -b 10000 -S {ss.server} -U {ss.user} -P {ss.password} -e "{self.root}error.txt" -h TABLOCK -a 16384 -q -c -t "|" ; rm "{self.root}{self.filename}" '
        if _async:
            os.popen(bcp)
        else:
            os.system(bcp)
        return self

    def load_bcp_bulk(self, _async=True):
        for self.filename in self.import_file:
            self.load_bcp_db(_async=_async)
        return self

    def create_folder(self):
        p = str(Path.home()) + '/dataload'
        isExist = os.path.exists(p)
        if not isExist:
            os.makedirs(p)

    def drop_table(self):
        print(f'DROPPING TABLE {self.prefix}{self.table}')
        PSQL = f''' DROP TABLE IF EXISTS {self.prefix}{self.table}; '''
        db.execute(PSQL)
        return self

    def delete_file(self):
        os.remove(self.filename)
        return self

    def create_table(self):
        try:
            x = j.dc(self.datastream(self.table))
            txt = ''
            if 'properties' in x:
                _int = {'duration', 'status', 'tx_status','mobility','plaque','bone_loss','pd_mf','pd_cf','pd_df','pd_ml','pd_cl','gm_mf','gm_cf','gm_df','gm_ml','gm_cl','gm_dl','mj_mf','mj_cf','mj_df','mj_ml','mj_cl','mj_dl','fg_mf','fg_cf','fg_df','fg_ml','fg_cl','fg_dl','bleeding_mf','bleeding_cf','bleeding_df','bleeding_ml','bleeding_cl','bleeding_dl','suppuration_mf','suppuration_cf','suppuration_df','suppuration_ml','suppuration_cl','suppuration_dl'}
                self.drop_table()
                for col in x['properties']['fields']['items']['enum']:
                    if col == 'id':
                        txt = f'''CREATE TABLE {self.prefix}{self.table} 
                        (id bigint, practice_id int, '''
                    elif col in ('_id','referral_date'):
                        txt += f'{col} varchar(255),'
                    elif col in _int:
                        txt += f'{col} INT,'
                    elif col in ('plan_id', 'insurance_id', 'guarantor_id', 'provider_id', 'patient_id'):
                        txt += f'{col} BIGINT,'
                    elif col in ('amount','cost','co_pay','bal_30_60','bal_60_90','bal_90_plus','tax'):
                        txt += f'{col} DECIMAL(19, 2),'
                    elif 'date' in col:
                        txt += f'{col} DATETIME2,'
                    elif col in ('dob',):
                        txt += f'{col} DATETIME2,'
                    elif col in ('note', 'notes'):
                        txt += f'{col} varchar(max),'
                    else:
                        txt += f'{col} varchar(255),'
                txt = txt + f''' last_updated DATETIME2);'''
                db.execute(txt)
        except:
            traceback.print_exc()
            pass
        return self

    def check_for_missing_records(self):
        problems = db.fetchall(f'''SELECT id, practice_id, count, table_name, last_updated, diff FROM staging.sync_record_check ''')
        for problem in problems:
            _log('check_for_missing_records', 'SCHED-5AM', ','.join(problem), 0, 'started')
            resync_table(problem[3], pids=problem[1], _async=True)
            _log('check_for_missing_records', 'SCHED-5AM', str(problem[5]), 0, 'completed')
        return self

    def create_indexes(self):
        now = arrow.now().format('YYYY_MM_DD')
        table = self.table+'_'+now
        db.execute(f'''CREATE UNIQUE CLUSTERED INDEX ux_{table}_pid ON {self.prefix}{self.table}  (practice_id, id) with ignore_dup_key; ''')
        if 'appointments' in table:
            db.execute(f'''CREATE INDEX ix_{table}_clinic_id ON {self.prefix}{self.table}  (clinic_id, practice_id); ''')
        elif 'ledger' in table:
            db.execute(f'''CREATE NONCLUSTERED INDEX [ix_{table}_clinic_id] ON {self.prefix}{self.table} ( [clinic_id] ASC ) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [ix_vx_{table}_patient_id] ON {self.prefix}{self.table} ( [patient_id] ASC ) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [ix_vx_{table}_payment_class] ON {self.prefix}{self.table} ( [payment_class] ASC ) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [ix_vx_{table}_practice_id] ON {self.prefix}{self.table} ( [practice_id] ASC ) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
        ''')

        elif 'treatments' in table:
            db.execute(f'''
CREATE NONCLUSTERED INDEX [sidx_{table}_practice_provider_clinic] ON {self.prefix}{self.table} ([practice_id] ASC,	[deleted] ASC,	[tx_status] ASC,	[completion_date] ASC,	[cost] ASC) INCLUDE([provider_id],[clinic_id]) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
CREATE NONCLUSTERED INDEX [ix_{table}_plan_date] ON {self.prefix}{self.table} ([practice_id] ASC, [deleted] ASC, [plan_date] ASC, [cost] ASC ) INCLUDE([completion_date],[tx_status],[clinic_id]) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
CREATE NONCLUSTERED INDEX [ix_{table}_patient_id] ON {self.prefix}{self.table} ( [patient_id] ASC ) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
CREATE NONCLUSTERED INDEX [ix_{table}_clinic_id] ON {self.prefix}{self.table} ( [clinic_id] ASC) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
CREATE NONCLUSTERED INDEX [ix_{table}__deleted_status_completion] ON {self.prefix}{self.table} ( [completion_date] ASC, [tx_status] ASC, [deleted] ASC ) INCLUDE([clinic_id],[code],[cost],[id],[patient_id],[practice_id],[provider_id]) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
        ''')

def correct_ids(staging=True):
    prefix = 'velox'
    if staging:
        prefix = 'staging'
    print("Correcting IDs")
    SQL = f'''
    UPDATE {prefix}.vx_ledger SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
    UPDATE {prefix}.vx_patients SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
    UPDATE {prefix}.vx_treatments SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
    UPDATE {prefix}.vx_treatments SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
    UPDATE {prefix}.vx_appointments SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
    UPDATE t set t.code = p.code from {prefix}.vx_treatments as t inner join {prefix}.vx_procedure_codes p on p.id = t.procedure_id and p.practice_id = t.practice_id WHERE t.code IS NULL;
    UPDATE {prefix}.vx_providers SET user_type = 'DEN' WHERE user_type NOT IN ('HYG', 'DEN');
    '''
    spawn(db.execute, SQL)
    global current
    current = 'No sync in progress...'
    return


def schema():
    v = API()
    for each in full_tables:
        print(v.datastream(each))

def correct_ids_local(staging=True):
    prefix = 'dbo.vx_'
    if staging:
        prefix = 'staging.vx_'

    print("Correcting IDs")

    SQL = f'''
    UPDATE {prefix}ledger SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
    UPDATE {prefix}patients SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
    UPDATE {prefix}treatments SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
    UPDATE {prefix}appointments SET clinic_id = '64' WHERE clinic_id = '68' or clinic_id = '63';
    UPDATE {prefix}providers SET user_type = 'DEN' WHERE user_type NOT IN ('HYG', 'DEN');
    UPDATE {prefix}providers SET code = pms_id WHERE code IS NULL
    '''
    db.execute(SQL)
    return

def check_staging_migration():
    SQL = '''SELECT value FROM dev.settings WHERE setting = 'staging_migration' '''
    return db.fetchone(SQL)[0]

def sync_in_progress(status=None):
    if current_sync is True:
        return True
    if status not in (None, 'running', 'idle'):
        return False
    if status:
        SQL = ''' UPDATE dev.settings SET value = %s WHERE setting = 'sync_state' '''
        db.execute(SQL, status)
        return status
    SQL = ''' SELECT value FROM dev.settings WHERE setting = 'sync_state' '''
    status = db.fetchone(SQL, status)[0]
    if status == 'running':
        return True
    return False

def last_updated(table='ledger'):
    t = {'ledger':'transaction_date', 'practices':'last_sync'}
    SQL = f'SELECT TOP 1 {t[table]} FROM dbo.vx_{table} WHERE {t[table]} <= GETDATE() '
    return db.fetchone(SQL)[0]


def reset(tables=None, practice=False):
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


def reset_table(tablename, staging=True):
    import time
    start = time.perf_counter()
    # print('Updating practices')
    # API().practices()
    x = API(staging)
    x.bulk_load(tablename, reload=True)
    print(x.missing)
    correct_ids()
    print(f'IT TOOK: {time.perf_counter() - start}')
    # db.execute(''' DROP TABLE staging.vx_{tablename}; '''.format(tablename=tablename))
    return


def resync_table(tablename, pids=None, verbose=False, _async=True, staging=True, backup=False):
    print(tablename)
    import time
    from API.scheduling import everyhour
    everyhour.pause = True
    start = time.perf_counter()
    # print('Updating practices')
    # API().practices()
    try:
        x = API(staging=staging)
        if pids:
            if isinstance(pids, str):
                pids = pids.split(',')
                x.pids = pids
            elif isinstance(pids, (list,tuple)):
                x.pids = pids
        x.load_sync_files(tablename, reload=True, verbose=verbose, _async=_async, backup=False)
    except:
        traceback.print_exc()
        error = traceback.format_exc()
        log(mode='full', error=str(error))
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
        # API().practices()
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


def scheduled(interval=None, staging=True):
    global current
    global current_sync
    if check_staging_migration() == 'True':
        current = f'Migration still in progress....'
        return
    if sync_in_progress():
        return 'Sync in progress...'
    sync_in_progress(status='running')
    error = ''
    from API.scheduling import everyhour
    ltime = ''
    try:
        everyhour.pause = True
        import time
        start = time.perf_counter()
        if interval:
            ltime = arrow.get().shift(hours=-int(interval)).format('YYYY-MM-DD[T]HH:mm:ss.SSS[Z]')
        elif not last_time_sync:
            ltime = arrow.get().shift(hours=-int(24)).format('YYYY-MM-DD[T]HH:mm:ss.SSS[Z]')
        else:
            ltime = arrow.get(last_time_sync).shift(minutes=-int(3)).format('YYYY-MM-DD[T]HH:mm:ss.SSS[Z]')
        print(ltime)
        for t in sync_tables:
            try:
                print(t)
                API(staging=staging).load_sync_files(t, start=ltime, _async=True)
            except:
                error = traceback.format_exc()
        correct_ids(staging=staging)
        print(f'IT TOOK: {time.perf_counter() - start}')
        current = f'No Sync In Progress... last sync took {time.perf_counter() - start:.2f} seconds...'
    except:
        error = traceback.format_exc()
    sync_in_progress(status='idle')
    log(mode='sync', error=error)
    everyhour.pause = False
    current_sync = False
    # _log('scheduled', 'scheduled', str(ltime), 0, error)
    return


def nightly():
    global current_sync
    if current_sync == True:
        sleep(600)
    current_sync = True
    start = time.perf_counter()
    error = ''
    _log('nightly', 'SCHEDULED', 'ALL', 0, 'started')
    try:
        for table in nightly_tables:
            resync_table(table,  staging=False)
    except:
        # traceback.print_exc()
        error = traceback.format_exc()
    finally:
        current_sync = False
    log(mode='full', error=str(error))
    print(f'IT TOOK: {time.perf_counter() - start}')
    if error:
        _log('nightly', 'SCHEDULED', str(time.perf_counter() - start), 0, error)
    return


def reload_file(table):
    v = API()
    v.table = table
    v.filename = f'{v.prefix}{v.table}.csv'
    # v.create_table()
    # v.load_bcp_db()
    # v.create_indexes()

def resync_main(pid):
    for t in ('providers','treatments', 'ledger', 'appointments', 'patients'):
        resync_table(t, pid, verbose=False, _async=True)

def fix_clinic_ids():
    for table in ('treatments', 'ledger', 'appointments', 'patients'):
        print(table)
        for practice, id in clinic_ids.items():
            print(practice, id)
            SQL = ''' UPDATE dbo.vx_{table} SET clinic_id = '{id}' WHERE practice_id = '{practice}' AND (clinic_id = '0' or clinic_id is null) '''.format(table=table, practice=practice, id=id)
            print(db.execute(SQL))

def create_staging():
    for table in sync_tables:
        print(table)
        x = API()
        x.table = table
        x.create_table()
        # x.create_indexes()

def check_for_missing_records():
    for table in sync_tables:
        print(table)
        x = API()
        x.table = table
        x.check_for_missing_records()


def schedule_pid(interval, table, pid):
    ltime = arrow.get().shift(hours=-int(24)).format('YYYY-MM-DD[T]HH:mm:ss.SSS[Z]')
    if interval:
        ltime = arrow.get().shift(hours=-int(interval)).format('YYYY-MM-DD[T]HH:mm:ss.SSS[Z]')
    v = API()
    v.pids = [pid]
    v.load_sync_files(table, start=ltime)
    return


if __name__ == '__main__':
    os.chdir('../../')
    scheduled('24')
