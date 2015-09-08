import requests
import keys


class GetQuandlAPIData():
    def __init__(self, code, apiAsset, sdate=None, edate=None):
        self.code = code
        self.apiAsset = apiAsset
        self.sdate = sdate
        self.edate = edate

    def generateURL(self):
        urlBase="https://www.quandl.com/api/v1/datasets/{}/{}.json?".format(
            self.apiAsset, self.code)
        auth= "auth_token="+keys.QUANDL_AUTH_KEY
        url = urlBase+auth
        if self.sdate:
            url = url + '&trim_start={}'.format(self.sdate)
        if self.edate:
            url = url + '&trim_end={}'.format(self.edate)
        print url
        return url

    def getQuandlData(self):
        url = self.generateURL()
        page = requests.get(url)
        pageJson = page.json()
        return pageJson

