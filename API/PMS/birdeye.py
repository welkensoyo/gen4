import traceback
import API.njson as j
import arrow
from API.comms import upool
from urllib.parse import urlencode
import API.dbms as db
from gevent import sleep
from API.config import sqlserver as ss
import os, csv
from pathlib import Path


qry = {
    'customer' : ''' INSERT INTO birdeye.customers (customerId, name, email, phone, lastActivityWithTime, sentiment) VALUES (%s, %s, %s, %s, %s, %s); ''',
    'customers' : ''' SELECT customerId FROM birdeye.customers ''',
    'surveys' : ''' INSERT INTO birdeye.surveys VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ''',
    'responses' : ''' INSERT INTO birdeye.survey_responses VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ''',
       }

class API:
    """
    sindex: is the start index
    count: No. of records, you want to fetch.
    """

    def __init__(self):
        self.root = str(Path.home()) + '/dataload/'
        self.id = '162880365742128'
        self.key =  '3U8cPHsVV7h1bvAt0mtr8TQpAa3adeIF'
        self.preurl = 'https://api.birdeye.com/resources/v1'
        self.preurl2 = 'https://api.birdeye.com/resources/v2'
        self.headers = {
            "content-type": "application/json",
            'Accept': "application/json",
        }

    def bulk(self, table, filename):
        bcp = f'/opt/mssql-tools/bin/bcp gen4_dw.birdeye.{table} in "{self.root}{filename}" -b 5000 -S {ss.server} -U {ss.user} -P {ss.password} -e "{self.root}error.txt" -h TABLOCK -a 16384 -q -c -t "|" '
        # print(bcp)
        os.system(bcp)
        return self

    def search_business(self, meta=None):
        url = f'{self.preurl}/business/search'
        if not meta:
            meta={}
            url = f'{self.preurl}/business/{self.id}'
        return self.transmit(url, meta, mode='GET')

    def business_review(self, meta):
        url = f'{self.preurl}/review/businessId/{meta.pop("id")}/summary'
        return self.transmit(url, meta, mode='GET')

    def employees(self):
        url = f'{self.preurl}/employee/{self.id}'
        return self.transmit(url, {}, mode='GET')

    def survey(self, meta):
        url = f'{self.preurl}/survey/{self.id}'
        return self.transmit(url, meta, mode='GET')

    def survey_response(self, surveyid, start=None , end=None):
        if not start:
            start = arrow.now().shift(months=3).format('MM/DD/YYYY HH:mm:ss')
        if not end:
            end = arrow.now().format('MM/DD/YYYY HH:mm:ss')
        meta = {}
        url = f'{self.preurl}/survey/ext/list/responses/{surveyid}'
        meta['size'] = 10000
        meta['page'] = 0
        meta['sortby'] = 'date'
        meta['businessNumber']  = self.id
        return self.transmit(url, meta, mode='POST', body={'startDate':start, 'endDate': end})

    def all_surveys(self, meta):
        url = f'{self.preurl}/survey/business/{self.id}/all'
        columns = ['surveyId', 'name', 'status', 'created', 'lastModified', 'responses', 'questionCount', 'pages',
                   'supportedLocales', 'surveyLink', 'strCreatedDate', 'strLastModifiedDate', 'showSurveyTitle',
                   'showBusinessName', 'showQuestionNumber', 'showProgressBar', 'hideBirdeyeLogo', 'hideLocaleMenu',
                   'cloneable', 'userPermissions', 'ownerName', 'ownerEmail', 'showLocationQues', 'surveyType',
                   'customParam', 'insightsEnabled', 'sent']
        for each in j.dc(self.transmit(url, meta, mode='GET')):
            row = []
            for c in columns:
                if c == 'surveyId':
                    row.append(int(each[c]))
                elif c == 'created':
                    row.append(arrow.get(each[c]).format('YYYY-MM-DD hh:mm:ss'))
                elif c == 'lastModified':
                    row.append(arrow.get(each[c]).format('YYYY-MM-DD hh:mm:ss'))
                else:
                    row.append(str(each[c]))
            db.execute(qry['surveys'], *row)
        return self

    def conversations(self):
        url = f'{self.preurl}/messenger/export'
        meta = {'businessNumber':self.id, 'apiKey':self.key}
        return self.transmit(url, {}, mode='POST', body=meta)

    def reviews(self, meta):
        url = f'{self.preurl}/review/businessId/{meta.get("id")}'
        if 'sindex' not in meta:
            meta['sindex'] = 0
            meta['count'] = 10000
        return self.transmit(url, meta, mode='POST')

    def competitor(self, meta):
        url = f'{self.preurl}/business/{meta.get("id")}/child'
        if not 'isCompetitor' in meta:
            meta['isCompetitor'] = False
        return self.transmit(url, meta, mode='GET')

    def search_child_business(self, meta):
        # pid = Reseller/Sub-reseller/Enterprise Id.
        url = f'{self.preurl}/business/child/all'
        return self.transmit(url, meta, mode='GET')

    def user(self, meta):
        if 'email' in meta:
            url = f'{self.preurl}/user/details'
            return self.transmit(url, meta, mode='GET')

    def customers(self, page=0, body=None):
        if not body:
            body = {"startDate": "01/01/2023", "endDate": arrow.now().format('MM/DD/YYYY')}
        def create_file(x):
            # fmt = 'Jul 07, 2023 07:53 AM PDT'
            fmt = 'MMM DD[, ]YYYY hh:mm A'
            a = []
            aa = a.append
            if 'customers' in x:
                for c in x['customers']:
                    try:
                        aa([c['customerId'], c.get('name',''), c.get('email', ''), c.get('phone',''), arrow.get(c['lastActivityWithTime'], fmt).format('YYYY-MM-DD hh:mm:ss'), c.get('sentiment','')])
                    except:
                        continue
                with open(self.root + 'customers.csv', 'w') as f:
                    cw = csv.writer(f, delimiter='|')
                    cw.writerows(a)
                self.bulk('customers', 'customers.csv')
        url = f'{self.preurl2}/customer/list'
        meta={}
        meta['size'] = 10000
        meta['page'] = page
        meta['sortby'] = 'lastActivityOn'

        x = j.dc(self.transmit(url, meta, mode='POST', body=body))
        create_file(x)
        tp = x['totalPages']
        start = page+1
        for page in range(start,tp):
            print(f'PAGE {page}')
            meta['page'] = page
            x = j.dc(self.transmit(url, meta, mode='POST', body=body))
            if 'code' in x and x['code']==500:
                sleep(10)
                x = j.dc(self.transmit(url, meta, mode='POST', body=body))
            create_file(x)
        return self

    def campaign(self):
        url = f'{self.preurl}/campaign/external/campaign-request-url'
        customers = [x[0] for x in db.fetchall(qry['customers'])]
        for customer in customers:
            print(customer)
            body={'businessId':self.id, 'customerId':customer}
            print(self.transmit(url, {}, mode='POST', body=body))

    def aggregation(self):
        url = self.preurl+f'/aggregation/business/{self.id}'
        return self.transmit(url, {}, mode='GET')

    def category(self):
        url = self.preurl + f'/category/all'
        return self.transmit(url, {}, mode='GET')

    def competitors(self):
        url = 'https://api.birdeye.com/resources/v1/business/162880365742128/competitors?api_key=3U8cPHsVV7h1bvAt0mtr8TQpAa3adeIF'
        r = upool.request('GET', url, headers=self.headers, retries=3)
        return r.data.decode()

    def transmit(self, url, meta, mode='POST', body=None):
        meta['api_key'] = self.key
        if mode.upper() == 'POST':
            if body:
                if meta:
                    url = url+'?'+urlencode(meta)
                r = upool.request(mode, url, body=j.jc(body), headers=self.headers, retries=3)
            else:
                r = upool.request(mode, url, headers=self.headers, retries=3)
        elif meta:
            if 'businessId' not in meta:
                meta['businessId'] = self.id
            print(meta)
            r = upool.request(mode, url, fields=meta, headers=self.headers, retries=3)
        else:
            if 'businessId' not in meta:
                meta['businessId'] = self.id
            r = upool.request(mode, url, headers=self.headers, retries=3)
        return r.data.decode()


def check(x):
    try:
        return int(x)

    except:
        pass
    try:
        fmt = 'MM/DD/YYYY'
        return arrow.get(x, fmt).format('YYYY-MM-DD hh:mm:ss')
    except:
        pass
    if isinstance(x, (list, tuple, dict)):
        return j.jc(x)
    return str(x)

def create_table(d):
    print(d)
    def detect(x):
        try:
            int(x)
            return 'BIGINT'
        except:
            pass
        try:
            fmt='MM/DD/YYYY'
            arrow.get(x,fmt).format('YYYY-MM-DD hh:mm:ss')
            return 'DATETIME2'
        except:
            pass
        return 'VARCHAR(max)'
    keys = list(d[0].keys())
    total = len(keys)
    inputs = '%s,'*total
    text = ''
    for k in keys:
        text+=f'{k} {detect(d[k])}, '
    return {'table' : text[0:-2], 'inputs':inputs, 'keys':keys}


if __name__ == "__main__":
    b = API()
    print(b.competitors())
    # for s in (21182,21905,25372,34718):
    #     x = j.dc(b.survey_response(s, '01/01/2001'))
    #     if r:= x.get('responseList'):
    #         print(len(r))
    #         print(len(r[0]))
    #         t = create_table(r[0])
    #         keys = ['responseId', 'requestDate', 'responseDate', 'completed', 'questionCount', 'locale', 'surveyId', 'surveyName', 'surveyType', 'businessId', 'locationName', 'businessNumber', 'businessLocationName', 'customerId', 'customerName', 'customerEmail', 'customerPhone', 'assistedByName,', 'assistedByEmail', 'assistedByPhone', 'ticketed', 'answers', 'overallScore']
    #         for each in r:
    #             response = [check(each.get(k,'')) for k in keys]
    #             db.execute(qry['responses'], *response)




