import API.timetools as zt

states = ''.join(f'''<option value="{k}">{v},'''  for k,v in zt.states.items())

class Render:
    def __init__(self, route, payload, size='-sm-3'):
        self.route = route
        self.pl = payload
        self.size = size
        self.ignore = ('enroll','token','whitelabel','fee','pci')
        self.body = ''

    def container(self, size='-fluid'):
        self.body = f'''<div class="container{size}"><br><div class="row g-1">{self.body}</div><br><br></div>'''
        return self

    def reserved(self, key, value):
        body = ''
        if key == 'state':
            body += f'''<div class="col{self.size}"><div class="form-floating"><select class="form-select" id="state" name="state" aria-label="State">'''
            for k,v in zt.states.items():
                if value == k:
                    body += f'''<option value="{k}" selected>{v}</option>,'''
                else:
                    body += f'''<option value="{k}">{v}</option>'''
            body += '''<label for="floatingSelect">State</label></div></div>'''
            return body
        return False

    def detect(self, meta):
        last={}
        for k, v in meta.items():
            if x := self.reserved(k, v):
                self.body += x
            elif not v:
                self.body += f'''<div class="col{self.size}"><div class="form-floating"><input type="text" class="form-control" id="{k}" name="{k}" placeholder="{k}" value=""><label for="{k}">{k}</label></div></div>'''
            elif isinstance(v, (str,int,float)):
                v = str(v)
                if '@' in v:
                    self.body += f'''<div class="col{self.size}"><div class="form-floating"><input type="email" class="form-control" id="{k}" name="{k}" placeholder="{k}" value="{v or ''}"><label for="{k}">{k}</label></div></div>'''
                else:
                    self.body += f'''<div class="col{self.size}"><div class="form-floating"><input type="text" class="form-control" id="{k}" name="{k}" placeholder="{k}" value="{v or ''}"><label for="{k}">{k}</label></div></div>'''
            elif isinstance(v, (list,tuple)):
                pass
            elif isinstance(v, (dict,)):
                if k not in self.ignore:
                    last[k] = v
        for k,v in last.items():
            self.body+=f'<h5>{k.upper()}</h5>'
            self.detect(v)


    def config(self):
        cfg = {"status":True,"id":"7ec17b5b-72c4-48b9-a0ab-cf7f222031e3","message":{"hashcode":"TESTAPIKEY","name":"TEST API","logo":"\/static\/images\/LogoIcon.png","timezone":"US\/Central","email":"no.reply@tripleplaypay.com","callback":"http:\/\/localhost\/api\/callback","pricing":{"etf":"500","ach_p2c":True,"ach_rate":"1.00","card_p2c":False,"low_rate":"0+0","tax_rate":"0.00","bank_rate":"1.00+1","card_rate":"2.89+1","rev_share":50,"crypto_p2c":False,"crypto_rate":"1.99+5+1"},"config":{"ach":"fis","ccs":3,"crypto":"curpay","enroll":True,"payout":"split_settlment","timing":60,"devices":5,"enabled":True,"payment":"fis","velocity":100,"terminals":"fis","whitelabel":{"test_2":"Success"},"high_ticket":10000,"enroll_email":True,"transactions":5},"wallet":["TESTAPIKEY-BANK:*6789"],"fee":{"pci":{"amount":"15.00","end":"2999-01-01"}}},"method":"client"}
        self.id = cfg['id']
        self.detect(cfg['message'])
        return self



