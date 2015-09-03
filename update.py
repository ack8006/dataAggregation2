import requests
import psycopg2
from contextlib import closing
import dbconnection
import keys
import assetDBs as adb
import datetime
import downloadData as dd

#****should update currences table for to_date
#****occassionally check for differences between db and api
class CheckUpdate():
    def __init__(self, nameDB, priceDB, priority=2):
        self.priority = priority
        self.nameDB = nameDB
        self.priceDB = priceDB

    def getNewAssetForUpdate(self):
        db = dbconnection.start()
        with closing(db.cursor()) as cur:
            newFX = self.checkForNewPriorityCurrencies(cur)
            newFX = [[x[0],None] for x in newFX]
        db.close()
        return newFX

    def checkForNewPriorityCurrencies(self, cur):
        cur.execute('''SELECT A.code FROM (
                    SELECT code FROM '''+self.nameDB+
                    ''' WHERE priority<=%s) as A
                    LEFT JOIN (
                    SELECT code FROM '''+self.priceDB+''' ) as B
                    ON A.code=B.code
                    WHERE B.code IS NULL''', (self.priority,))
        fxpairs = cur.fetchall()
        return fxpairs

    def getExistingAssetsForUpdate(self):
        db = dbconnection.start()
        with closing(db.cursor()) as cur:
            upFX = self.checkForUpdates(cur)

        db.close()
        return upFX

    def checkForUpdates(self, cur):
        cur.execute('''SELECT code, MAX(date) date
                    FROM '''+self.priceDB+
                    ''' GROUP BY code''')
        fxDates = cur.fetchall()
        return fxDates

#***Should be a class for all assets with specific upload FX data
class UploadCurrencyData():
    def __init__(self, code, pageJson):
        self.pageJson = pageJson
        self.code = code

    def startUpdate(self):
        data = self.pageJson['data']
        self.uploadData(data)

    def uploadData(self, data):
        db = dbconnection.start()
        with closing(db.cursor()) as cur:
            for dp in data:
                date = datetime.datetime.strptime(dp[0], "%Y-%m-%d").date()
                high = None
                low = None
# insers None instead of 0.0 if missing
                if dp[2] != 0.0: high = dp[2]
                if dp[3] !=0.0: low = dp[3]
                cur.execute('''INSERT INTO dailyFXPrices
                            (code, rate, highprice, lowprice, date) VALUES
                            (%s, %s, %s, %s, %s)''',
                            (self.code, dp[1],high,low,date))
            db.commit()
        db.close()


#***can add more checks based on day of week and whatnot
def shouldCheckForUpdate(date):
    shouldCheck = True
    nextDate = date+datetime.timedelta(days=1)
    todayDate = datetime.datetime.now().date()
    if  nextDate >= todayDate :
        shouldCheck = False
    return shouldCheck

def updateFXData(dbs):
    def getAndPushData(assetDatePairs, apiAsset):
        for pair in assetDatePairs:
            code=pair[0]
            startDate=pair[1]
            if startDate:
                if not shouldCheckForUpdate(startDate):
                    continue
                startDate = str(startDate+datetime.timedelta(days=1))
            gqad = dd.GetQuandlAPIData(code, apiAsset, startDate)
            pageJson = gqad.getQuandlData()
            ucd = UploadCurrencyData(code, pageJson)
            ucd.startUpdate()


    newFX, existingFX = dataToUpdate(dbs)
    getAndPushData(newFX, 'CURRFX')
    getAndPushData(existingFX, 'CURRFX')


def dataToUpdate(dbs):
    cu = CheckUpdate(dbs[0],dbs[1])
    newAssets=cu.getNewAssetForUpdate()
    existingAssets=cu.getExistingAssetsForUpdate()
    return newAssets, existingAssets



if __name__ == "__main__":
    assetDBs = adb.assetDBs
    updateFXData(assetDBs['CURNCY'])




