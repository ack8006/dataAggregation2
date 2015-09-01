import requests
import psycopg2
from contextlib import closing
import dbconnection
import keys
import datetime

#****should update currences table for to_date
#****occassionally check for differences between db and api
class CheckUpdate():
    def __init__(self, priority=2):
        self.priority = priority

    def getNewFXForUpdate(self):
        db = dbconnection.start()
        with closing(db.cursor()) as cur:
            newFX = self.checkForNewPriorityCurrencies(cur)
            newFX = [x[0] for x in newFX]
        db.close()
        return newFX

    def checkForNewPriorityCurrencies(self, cur):
        cur.execute('''SELECT A.code FROM (
                    SELECT code FROM currencies
                    WHERE priority<=%s) as A
                    LEFT JOIN (
                    SELECT code FROM dailyFXPrices) as B
                    ON A.code=B.code
                    WHERE B.code IS NULL''', (self.priority,))
        fxpairs = cur.fetchall()
        return fxpairs

    def getExistingFXForUpdate(self):
        db = dbconnection.start()
        with closing(db.cursor()) as cur:
            upFX = self.checkForCurrencyUpdates(cur)

        db.close()
        return upFX

    def checkForCurrencyUpdates(self, cur):
        cur.execute('''SELECT code, MAX(date) date
                    FROM dailyFXPrices
                    GROUP BY code''')
        fxDates = cur.fetchall()
        return fxDates


class UpdateCurrency():
    def __init__(self, code, sdate=None, edate=None):
        self.code = code
        self.sdate = sdate
        self.edate = edate

    def generateURL(self):
        urlBase="https://www.quandl.com/api/v1/datasets/CURRFX/{}.json?".format(self.code)
        auth= "auth_token="+keys.QUANDL_AUTH_KEY
        urlBase= urlBase+auth
        if self.sdate:
            urlBase=urlBase+"&trim_start="+self.sdate
        if self.edate:
            urlBase=urlBase+"&trim_end="+self.edate
        return urlBase

    def startUpdate(self):
        url = self.generateURL()
        pageJson = self.getCurrencyData(url)
        data = pageJson['data']
        self.uploadData(data)

    def uploadData(self, data):
        db = dbconnection.start()
        with closing(db.cursor()) as cur:
            for dp in data:
                date = datetime.datetime.strptime(dp[0], "%Y-%m-%d").date()
                high = None
                low = None
                if dp[2] != 0.0: high = dp[2]
                if dp[3] !=0.0: low = dp[3]
                cur.execute('''INSERT INTO dailyFXPrices
                            (code, rate, highprice, lowprice, date) VALUES
                            (%s, %s, %s, %s, %s)''',
                            (self.code, dp[1],high,low,date))
            db.commit()
        db.close()

    def getCurrencyData(self, url):
        page = requests.get(url)
        pageJson = page.json()
        return pageJson

#***can add more checks based on day of week and whatnot
def shouldCheckForUpdate(date):
    shouldCheck = True
    nextDate = date+datetime.timedelta(days=1)
    todayDate = datetime.datetime.now().date()
    if  nextDate >= todayDate :
        shouldCheck = False
    return shouldCheck


if __name__ == "__main__":
    cu = CheckUpdate()
    newFX=cu.getNewFXForUpdate()
    existingFX=cu.getExistingFXForUpdate()

    #*****at some point multiprocessing
    for x in newFX:
        uc=UpdateCurrency(x)
        uc.startUpdate()

    for ex in existingFX:
        fx = ex[0]
        startDate=ex[1]
        if shouldCheckForUpdate(startDate):
            uc=UpdateCurrency(fx, str(startDate))
            uc.startUpdate()
            print 'updating'


