import API.dbpg as pg
from API.njson import jc
import arrow, time


qry = {
    'insert': ''' INSERT INTO sdb.ledger (batchid, meta) VALUES {} '''
}


class LedgerItem:
    def __init__(self):
        self.patientid = ''
        self.provider = {'id':'','credit':''}
        self.payment = {'class':''}
        self.procedure = {'code':'','description':''}
        self.location = ''
        self.date = ''
        self.amount = ''
        self.clinicid = ''

    @classmethod
    def row(cls, patientid, providerid, cl, amount, code, description, location, date, clinicid):
         c = cls()
         c.provider = {
             'clinicid': clinicid,
             'id': providerid if providerid not in ('No_Prov',) else False,
             'location': location,
         }
         c.procedure = {
             'patientid':patientid,
             'class': cl,
             'code': code,
             'description': description.replace("'","`"),
             'date': date,
             'amount': amount
         }
         return c


class Load:
    def __init__(self):
        pass

    def bulk(self, meta):
        start = time.perf_counter()
        pg.execute(qry['insert'].format(meta))
        end = time.perf_counter()
        print(f'{end - start:0.4f}')

    def dentrix(self, rows, clientid='0010'):
        meta=''
        batchid = f"{clientid}:{+arrow.now().format('YYYY-MM-DD_hh_mm_ss')}"
        for r in rows:
            a = str(r[25])
            cl = r[9] or ''
            _class = r[11]
            l = LedgerItem.row(r[4], r[8], cl.replace('=', '').strip(), f'{a[:-2]}.{a[-2:]:}', r[13], r[14], r[15], arrow.get(r[19]).format('YYYY-MM-DD'), clientid)
            meta += f"('{batchid}','" + jc(vars(l)) + "'),"
        self.bulk(meta[:-1])

    def cloud9(self):
        pass

    def curve(self, rows):
        meta = ''
        batchid = 'curve:' + arrow.now().format('YYYY-MM-DD_hh_mm_ss')
        for r in rows:
            l = LedgerItem.row(r)
            meta += f"('{batchid}','" + jc(vars(l)) + "'),"



