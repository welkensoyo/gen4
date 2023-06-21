from gevent import monkey
monkey.patch_all()
from API.config import cloud9 as c9
import API.njson as json
from API.comms import upool
import API.dbpg as db

qry = {
    'update' : ''' INSERT INTO sdb.curve (name, meta) VALUES (%s, %s::jsonb)
                    ON CONFLICT (name) DO UPDATE SET meta = %s::jsonb RETURNING name ''' ,
    'get' : ''' SELECT meta FROM sdb.curve WHERE name = %s '''
}

class API:
    def __init__(self, clientId):
        self.clientId = clientId
        self.url = ''
        self.body = '''<?xml version="1.0" encoding="utf-8" ?>
<GetDataRequest xmlns="http://schemas.practica.ws/cloud9/partners/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
<ClientID>{clientId}</ClientID>
<UserName>{user}</UserName>
<Password>{pwd}</Password>
<Procedure>{method}</Procedure>
<Parameters>{parameters}</Parameters>
</GetDataRequest>'''
        self.result = {}

    def atl_get(self, method, params):
        url = c9.url_atl
        user = c9.user
        pwd = c9.pwd_atl
        parameters = ''
        for k, v in params.items():
            if k == 'self':
                continue
            if v:
                parameters += f'<{k}>{v}</{k}>'
            else:
                parameters += f'''<{k} xsi:nil="true" ></{k}>'''
        body = self.body.format(clientId=self.clientId, user=user, pwd=pwd, method=method, parameters=parameters)
        return self.transmit(url, body, method='POST')

    def aws_get(self, method, params):
        url = c9.url_aws
        user = c9.user
        pwd = c9.pwd_aws
        parameters = ''
        for k, v in params.items():
            if k == 'self':
                continue
            if v:
                parameters += f'<{k}>{v}</{k}>'
            else:
                parameters += f'''<{k} xsi:nil="true" ></{k}>'''
        body = self.body.format(clientId=self.clientId, user=user, pwd=pwd, method=method, parameters=parameters)
        self.result[method] = self.transmit(url, body, method='POST')
        return self

    def adjustments(self, StartDateParam='', EndDateParam='', BreakByParam=''):
        return self.aws_get('GetAdjustments', locals())

    def insuranceContracts(self, fromDate='', toDate=''):
        return self.aws_get('GetInsuranceContracts', locals())

    def insurancePoliciesByPatient(self, patGUID=''):
        return self.aws_get('GetInsurancePoliciesByPatient', locals())

    def aging(self, AgeOfBalance='', AmountOfBalance='', IncludeInsuranceBalances='',IncludeResponsiblePartyBalances='', CreditsOnly='', AsOfDate='',LocationGuids='', ShowZeroAccounts='',UseCurrent='',InsuranceBillingCenter='', IncludeInsuranceBalance='' ):
        return self.aws_get('GetAgingReportData', locals())

    def patientlist(self, patEntryDate='', ModifiedSince='', LocGUIDS=''):
        return self.aws_get('GetPatientList', locals())

    def contracts(self, fromDate='', toDate='', isActive='',locGUIDAssigned=''):
        return self.aws_get('GetContracts', locals())

    def contractByStart(self, Locations='', StartDate='', EndDate=''):
        return self.aws_get('GetContractsByStartDate', locals())

    def getLedger(self, fromDate='', toDate='', patGUIDString='', ppGUIDString='', pipGUIDString='', showComments='', loginGUIDString='', persGUIDRelatedString='', providerGUIDs=''):
        return self.aws_get('GetLedger', locals())

    def transmit(self, url, body, method='POST'):
        r = upool.request(method.upper(), url, body=body, headers={'Content-Type': 'application/xml'})
        # print(r.data.decode())
        return json.xml(r.data)


if __name__ == '__main__':
    save_folder = 'C:\Production\Data_API\data\\'
    from pprint import pprint
    c = API('f1943667-c6a2-45c8-9cce-842c4187966e')
    x = {
        # 'PatientList': c.patientlist('f1943667-c6a2-45c8-9cce-842c4187966e'),
        'Ledger': c.getLedger()
    }
    m = json.jc(x)
    with open(save_folder + 'cloud9' + '.json', 'w') as f:
        f.write(m)

