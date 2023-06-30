import API.njson as j
from API.comms import upool


class API:
    """
    sindex: is the start index
    count: No. of records, you want to fetch.
    """

    def __init__(self):
        self.id = '162880365742128'
        self.key =  '3U8cPHsVV7h1bvAt0mtr8TQpAa3adeIF'
        self.preurl = 'https://api.birdeye.com/resources/v1'
        self.headers = {
            "content-type": "application/json",
            'Accept': "application/json",
        }


    def search_business(self, meta):
        url = f'{self.preurl}/business/search'
        if x := meta.pop('id', None):
            url = f'{self.preurl}/business/{x}'
        return self.transmit(url, meta, mode='GET')

    def business_review(self, meta):
        url = f'{self.preurl}/review/businessId/{meta.pop("id")}/summary'
        return self.transmit(url, meta, mode='GET')

    def employees(self, meta):
        url = f'{self.preurl}/employee/{meta.pop("id")}'
        return self.transmit(url, meta, mode='GET')

    def survey(self, meta):
        url = f'{self.preurl}/survey/{meta.pop("id")}'
        return self.transmit(url, meta, mode='GET')

    def conversations(self):
        url = f'{self.preurl}/messenger/export'
        return self.transmit(url, mode='POST')

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


    def transmit(self, url, meta=None, mode='POST'):
        meta['api_key'] = self.key
        print(url)
        if meta:
            r = upool.request(mode, url, fields=meta, headers=self.headers, retries=3)
        else:
            r = upool.request(mode, url, headers=self.headers, retries=3)
        return r.data.decode()


if __name__ == "__main__":
    b = API()
    print(b.search_business({'id':'162880365742128'}))
    print(b.search_business({'search_str':'Gaylord','loc':'37211'}))