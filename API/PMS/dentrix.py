import API.dbms as ms
from API.ledgeritem import Load
from API.container import API as docker
import subprocess as sp
import os

qry = {
    "get": ''' WITH FilteredClinics AS (SELECT rsc.URSCID, rsc.RSCID FROM DENTRIX.dbo.DDB_RSC_BASE rsc)
SELECT
		pat.LASTNAME AS PatLastName,
		pat.FIRSTNAME AS PatFirstName,
		pat.MI AS PatMI,
		pat.CHART AS PatChart,
		pat.PATID as PatID,
		pat.HOMEPHONE AS PatHomePhone,
		pat.PrivacyFlags AS PatPrivacyFlags,
		aging.BILLINGTYPE AS AgingBillingType,
		plProvider.RSCID AS ProcedureProvider,
		paymentClassDef.description AS PmtClassDescription,
		credit.creditid AS CreditID,
		creditProvider.RSCID AS CreditProvider,
		pc.FOLLOWUP AS ProcCodeFollowUp,
		pc.ADACODE AS ProcCodeADACode,
		pc.DESCRIPTION AS ProcCodeDescription,
		clinic.RSCID AS Clinic,
		pl.CHART_STATUS AS ProcedureChartStatus,
		pl.CLASS AS ProcedureClass,
		pl.FAMILYFLAG AS ProcedureFamilyFlag,
		pl.PLDATE AS ProcedureDate,
		pl.CREATEDATE AS ProcedureCreateDate,
		pl.CHECKNUM AS ProcedureCheckNum,
		pl.TOOTH_RANGE_START AS ProcedureToothRangeStart,
		pl.TOOTH_RANGE_END AS ProcedureToothRangeEnd,
		pl.SURF_STRING AS ProcedureSurfaceString,
		pl.AMOUNT AS ProcedureAmount,
		pl.PROC_LOGID AS ProcedureLogID,
		pl.PROVID AS ProcedureProvID,
		0 AS CRFIXIT
FROM DENTRIX.dbo.DDB_PAT_BASE pat
JOIN DENTRIX.dbo.DDB_AGING_BASE aging ON pat.GUARID = aging.GUARID AND pat.GUARDB = aging.GUARDB
JOIN DENTRIX.dbo.DDB_PROC_LOG_BASE pl ON pat.PATID = pl.PATID AND pat.PATDB = pl.PATDB
JOIN FilteredClinics clinic ON pl.ClinicAppliedTo = clinic.URSCID
LEFT JOIN DENTRIX.dbo.DDB_RSC_BASE plProvider ON pl.PROVID = plProvider.URSCID AND pl.PROVDB = plProvider.RSCDB
LEFT JOIN DENTRIX.dbo.DDB_PROC_CODE_BASE pc ON pl.PROC_CODEID = pc.PROC_CODEID
LEFT JOIN DENTRIX.dbo.dx1rep_CrToSingleProv credit ON pl.PROC_LOGID = credit.creditid AND pl.PROC_LOGDB = credit.creditdb
LEFT JOIN DENTRIX.dbo.DDB_RSC creditProvider ON credit.provid = creditProvider.URSCID AND credit.provdb = creditProvider.RSCDB
LEFT JOIN DENTRIX.dbo.dx1rep_DEF_ADJ_PMT paymentClassDef ON pl.CLASS = paymentClassDef.Class AND pl.ORD = paymentClassDef.Ord
WHERE
(pl.CHART_STATUS = 102 OR (pl.CHART_STATUS = 90 AND pl.CLASS IN (1, 2 ,3)))
AND CAST(pl.PLDATE as date)  >=  '2021-12-01' '''
}

class API:
    def __init__(self):
        self.d = docker()

    def run(self, file):
        self.build_docker()
        self.load_db(file)
        self.load_pg()

    def load_db(self, file):
        rmdir = 'docker exec -it SQL19 rm /var/opt/mssql/backup -f'
        sp.Popen(rmdir, universal_newlines=True, shell=True, stdout=sp.PIPE, stderr=sp.PIPE).communicate()
        mkdir = 'docker exec -it SQL19 mkdir /var/opt/mssql/backup'
        sp.Popen(mkdir, universal_newlines=True, shell=True, stdout=sp.PIPE, stderr=sp.PIPE).communicate()
        restore = f'docker cp {file} SQL19:/var/opt/mssql/backup'
        sp.Popen(restore, universal_newlines=True, shell=True, stdout=sp.PIPE, stderr=sp.PIPE).communicate()
        self.drop_db()
        ms.execute(f''' RESTORE DATABASE [DENTRIX] FROM  DISK = N'/var/opt/mssql/backup/{os.path.basename(file)}' 
                        WITH  FILE = 1,  
                        MOVE N'Dentrix_Data' TO N'/var/opt/mssql/data/Dentrix.mdf',  
                        MOVE N'Dentrix_Log' TO N'/var/opt/mssql/data/Dentrix_log.ldf' ''')


    def drop_db(self):
        database = 'dentrix'
        return ms.execute(f''' EXEC msdb.dbo.sp_delete_database_backuphistory @database_name = N'{database}';
                        USE [master];
                        ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                        DROP DATABASE [{database}];  ''')

    def build_docker(self):
        if 'SQL19' not in self.d.containers():
            self.d.create('SQL19')
            mkdir = 'docker exec -it SQL19 mkdir /var/opt/mssql/backup'
            sp.Popen(mkdir, universal_newlines=True, shell=True, stdout=sp.PIPE, stderr=sp.PIPE).communicate()
        return self.d.pick_container('SQL19')

    def load_pg(self):
        rows = ms.fetchall(qry['get'])
        print(len(rows))
        Load().dentrix(rows)

