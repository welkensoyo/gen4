from gevent import sleep
import subprocess as sp
import os, arrow
from pathlib import Path
import traceback
import pysftp
import API.sftp as sftp
from API.container import API as docker
import time
from API.config import master_pass, denticonSftp, working_folder as folder, bpk
import API.dbms as ms

import sys


class DenticonException(Exception):
    pass

folder = folder + '\\denticon'

class API:
    def __init__(self):
        self.folder = folder
        self.database = 'denticon'
        self.clinicid = ''
        self.tables = ['ALERTMEDICAL', 'APPTD', 'APPTDINSD', 'APPTH', 'APPTQUICKFILL', 'AUDITTRAIL', 'CARRIERS', 'CARRIERSELIG', 'CHARTACTIVITY', 'CHARTCOLORS', 'CHARTDEFFORCODES', 'CHARTMATERIALS', 'CHARTNOTESMACROS', 'CHARTPERIOACTIVITY', 'CHARTPERIOACTIVITYD', 'CHARTPERIOACTIVITYH', 'CHARTPERIOSETUP', 'CLAIM835CLP', 'CLAIM835H', 'CLAIM835SVC', 'CLAIMD', 'CLAIMELIG', 'CLAIMELIGD', 'CLAIMELIGH', 'CLAIMFILLOUTDENTAL', 'CLAIMFILLOUTMEDICAL', 'CLAIMH', 'CODES', 'CODESEXPLOSIOND', 'CODESEXPLOSIONH', 'CODESICD', 'CODESMAPCDTCPT', 'CODESMAPCPTICD', 'CODESMODIFIER', 'CODESPOS', 'CODESTOS', 'CODESVIEW', 'COLAGENCY', 'DEFINITIONS', 'DEFINITIONSH', 'EMPLOYERS', 'EMPSALESREP', 'FEESCHEDA', 'FEESCHEDD', 'FEESCHEDH', 'FEESMART', 'HISTDENTALMEDICAL', 'HOLIDAYSOFFICE', 'HOLIDAYSPROVIDER', 'IMAGEDETAIL', 'IMAGEGROUP', 'INSCOVERAGE', 'INSCUSTCOVERAGE', 'INSGROUP', 'INSPLANS', 'INTERNALREFERRAL', 'INTERNALREFERRALTREATPLAN', 'INTERNALREFERRALXRAY', 'LABS', 'LEDGER', 'LEDGERINSD', 'LEDGERPAYALLOC', 'LEDGERPAYD', 'OFFICE', 'OGROUP', 'ONLINEMEDICALHISTORY', 'ONLINEPATALERTMEDICAL', 'ONLINEPATIENT', 'OPERATORY', 'ORTHOQUESTIONNAIRED', 'ORTHOQUESTIONNAIREH', 'ORTHOSETUPD', 'ORTHOSETUPH', 'ORTHOSETUPVIEW', 'ORTHOTXHISTORYD', 'ORTHOTXHISTORYH', 'ORTHOTXPLAND', 'ORTHOTXPLANH', 'PATALERTMEDICAL', 'PATBASICMEASURES', 'PATCARIESRISK', 'PATCARIESRISK1340', 'PATCONTRACTBILLING', 'PATFLASHALERTS', 'PATHISTDENTALMEDICAL', 'PATIENT', 'PATIENTRECENT', 'PATINSCONTRACTBILLING', 'PATINSPLANS', 'PATMEDICALHISTORYD', 'PATMEDICALHISTORYH', 'PATNOTES', 'PATNOTES_ARCHIVE', 'PATORTHOPLAN', 'PATPICTURE', 'PATRECALL', 'PATREGPLAN', 'PATRX', 'PATSECINSCONTRACTBILLING', 'PATSTATUSTRACK', 'PAYADJUST', 'PAYCLASS', 'PGROUP', 'PGRX', 'POSTCARDS', 'PROGRESSNOTES', 'PROGRESSNOTES_ARCHIVE', 'PROVIDER', 'PROVIDERCATSCHED', 'PROVIDERDAILYGOALS', 'PROVIDEREFFCATSCHED', 'PROVIDEREFFSCHED', 'PROVIDERINSID', 'PROVIDEROFFICE', 'PROVIDEROPERATORY', 'PROVIDERROUTESLIP', 'QALISTD', 'QALISTH', 'REFERRALDEMOGD', 'REFERRALDEMOGH', 'REFERRALS', 'RESPINSPLAN', 'RESPNOTES', 'RESPPARTY', 'ROUTESERIAL', 'SCHEDULERVIEWSD', 'SCHEDULERVIEWSH', 'SECUFUNCTIONS', 'SECUGROUPFUNCS', 'SECUGROUPS', 'SECUGROUPUSERS', 'SECUSERLOG', 'SECUUSER', 'SECUUSEROFFICE', 'SMARTASSIST', 'STATUSTRACK', 'TASKMANAGERAUDIT', 'TASKMANAGERD', 'TASKMANAGERDELETELOG', 'TASKMANAGERH', 'TCLOCK', 'TICKLER', 'TREATPLAN', 'TREATPLANINSD']
        # self.d = docker()

    def run(self):
        for file in self.get():
            print(file)
            if not self.decompress(file):
                continue
            self.create_db()
            self.load_db()
            self.save_db()
            self.drop_db()
            ms.close()
        return True

    def bpk_run(self):
        for file in self.get():
            if '4129' in file:
                continue
            if not self.decompress(file):
                continue
            print(file)
            self.folders()
            self.create_bpk_db()
            self.load_bpk_db()
            self.rename()
        return True

    def decompress(self, file):
        fullpath = self.folder+'\\'+file
        filename, file_extension = os.path.splitext(file)
        self.clinicid = filename.split('_')[0]
        print(self.clinicid)
        try:
            int(self.clinicid)
        except:
            return False
        # sp.run(["C:\\Program Files\\7-Zip\\7z.exe", "x", fullpath, f"-o{self.folder}\\{filename}", f"-p{denticonSftp.zip_pass}", "-y"])
        return self.clinicid

    def folders(self):
        self.dat_folder = folder.format('clinicid') + f'\\{self.clinicid}_DenticonDB\ExportBCP\\RestoreData\\dat{self.clinicid}\\'
        self.format_folder = folder + f'\\{self.clinicid}_DenticonDB\\ExportBCP\\RestoreFormat\\'
        self.script_folder = folder + f'\\{self.clinicid}_DenticonDB\\ExportBCP\\RestoreScripts\\'

    def get(self):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        url = denticonSftp.url
        user = denticonSftp.user
        pwd = denticonSftp.pwd
        start = time.perf_counter()
        files = []
        try:
            with pysftp.Connection(url, username=user, password=pwd, cnopts=cnopts) as connection:
                connection.chdir('\Out')
                files = connection.listdir()
        except:
            traceback.print_exc()
        for file in files:
            if not os.path.exists(self.folder+f'\\{file}'):
                sftp.get_file(url, user, pwd, file, self.folder+f'\\{file}', ftp_folder='\Out')
        end = time.perf_counter()
        print(f'{end - start:0.4f}')
        return files

    def build_docker(self):
        if 'SQL19' not in self.d.containers():
            self.d.create('SQL19')
            sleep(5)
        return self.d.pick_container('SQL19')

    def create_db(self):
        if not self.build_docker():
            raise DenticonException('Docker is not running...')
        print(f'{self.script_folder}createexportdb.sql')
        sleep(5)
        schema = f'sqlcmd -U sa -P {master_pass} -Q  "DROP DATABASE IF EXISTS {self.database}; CREATE DATABASE {self.database};" '
        os.system(schema)
        # sp.Popen(schema, universal_newlines=True, shell=False).communicate()
        schema = f'sqlcmd -U sa -P {master_pass} -v dbName="denticon" varDBPath="/var/opt/mssql/data" -i {self.script_folder}createexportdb.sql'
        os.system(schema)
        # sp.Popen(schema, universal_newlines=True, shell=True).communicate()
        print('Tables Created')

    def create_bpk_db(self):
        print('Creating DB')
        from API.toolkits.createdenticon import script
        schema = f"DROP DATABASE IF EXISTS {self.database}_{self.clinicid};"
        ms.bpkexecute(schema)
        ms.close()
        ms.bpkexecute(f"CREATE DATABASE {self.database};")
        ms.close()
        ms.denticonexecute(script)
        ms.close()
        # sp.Popen(schema, universal_newlines=True, shell=False).communicate()
        # schema = f'sqlcmd -S {bpk.server} -U {bpk.user} -P {bpk.password} -i c:\\backup\\exportdenticon.txt '
        # print(schema)
        # os.system(schema)
        # sp.Popen(schema, universal_newlines=True, shell=True).communicate()
        print('Tables Created')

    def load_bpk_db(self):
        start = time.perf_counter()
        for t in self.tables:
            print(f'{self.format_folder}{t}fmt.xml')
            bcp = f'bcp [{self.database}].[dbo].[{t}] in "{self.dat_folder}{t}.dat" -S {bpk.server}  -U {bpk.user} -P {bpk.password} -h TABLOCK -a 16384 -b 50000 -t ~$~ -r ~@~ -f "{self.format_folder}{t}fmt.xml"'
            os.system(bcp)
            # sp.Popen(bcp, universal_newlines=True, shell=True).communicate()
        end = time.perf_counter()
        print(f'{end - start:0.4f}')
        return

    def load_db(self):
        start = time.perf_counter()
        for t in self.tables:
            bcp = f'bcp [{self.database}].[dbo].[{t}] in "{self.dat_folder}{t}.dat" -U {bpk.user} -P {bpk.password} -h TABLOCK -a 16384 -b 50000 -t ~$~ -r ~@~ -f "{self.format_folder}{t}fmt.xml"'
            os.system(bcp)
            # sp.Popen(bcp, universal_newlines=True, shell=True).communicate()
        end = time.perf_counter()
        print(f'{end - start:0.4f}')
        return

    def save_db(self):
        return ms.execute(f'''USE [{self.database}];
                        BACKUP DATABASE [{self.database}] TO DISK = N'/mnt/backup/denticon_{self.clinicid}_{arrow.now().format("YYYYMMDDHHmmss")}.bak' WITH NOFORMAT, NOINIT,  NAME = N'denticon-Full Database Backup', SKIP, NOREWIND, NOUNLOAD, COMPRESSION, STATS = 10''')
    def drop_db(self):
        ms.close()
        return ms.denticonexecute(f''' ALTER DATABASE [{self.database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{self.database}];  ''')

    def rename(self):
        SQL = f''' ALTER DATABASE {self.database} MODIFY NAME = {self.database}_{self.clinicid};'''
        print(SQL)
        ms.bpkexecute(SQL)
        return self

    def load_bpk(self):
        SQL = ''' RESTORE DATABASE [{}] FROM DISK = N'{}' WITH FILE = 1, NOUNLOAD, REPLACE, STATS = 5;'''
        for f in os.listdir('C:\\backup'):
            x = Path(f)
            name = '_'.join(x.stem.split('_')[0:2])
            print(name)
            if 'bak' in x.suffix:
                print(ms.bpkexecute(SQL.format(name, name)))

    def cleanup(self):
        for f in os.listdir(self.folder):
            if 'denticon' in f.lower():
                command = f'powershell -Command rm -r {self.folder}/{f}'
                os.system(command)


if __name__ == '__main__':
    from gevent import monkey
    monkey.patch_all()
    try:
        d = API()
        d.cleanup()
    except:
        traceback.print_exc()
        sys.exit(1)
    sys.exit(0)
