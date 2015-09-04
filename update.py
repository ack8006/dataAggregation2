import requests
import psycopg2
from contextlib import closing
import dbconnection
import keys
import assetDBs as adb
import datetime
import downloadData as dd
import updateIndex

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
        upFX = [[x[0],x[1]] for x in upFX]
        return upFX

    def checkForUpdates(self, cur):
        cur.execute('''SELECT code, MAX(date) date
                    FROM '''+self.priceDB+
                    ''' GROUP BY code''')
        fxDates = cur.fetchall()
        return fxDates

#***Should be a class for all assets with specific upload FX data
class UploadAssetData():
    def __init__(self, code, pageJson, assetType, priceDB):
        self.pageJson = pageJson
        self.code = code
        constructorDict = {'CURNCY': self.FXCurConstructor,
                           'INDEX': self.IndCurConstructor,
                           'COMDTY': self.ComdCurConstructor
                           }
        self.curConstructor = constructorDict[assetType]
        self.priceDB = priceDB

    def startUpdate(self):
        try:
            data = self.pageJson['data']
            self.uploadData(data)
        except KeyError, e:
            print e
            print "CANNOT GET DATA FOR "+ self.code

    def uploadData(self, data):
        db = dbconnection.start()
        with closing(db.cursor()) as cur:
            cur = self.curConstructor(cur, data)
            db.commit()
        db.close()

    def FXCurConstructor(self, cur, data):
        for dp in data:
            date = datetime.datetime.strptime(dp[0], "%Y-%m-%d").date()
            high = None
            low = None
# insers None instead of 0.0 if missing
            if dp[2] != 0.0: high = dp[2]
            if dp[3] !=0.0: low = dp[3]
            cur.execute('''INSERT INTO '''+self.priceDB+
                        '''(code, rate, high, low, date) VALUES
                        (%s, %s, %s, %s, %s)''',
                        (self.code, dp[1],high,low,date))
        return cur

    def IndCurConstructor(self, cur, data):
        for dp in data:
            date = datetime.datetime.strptime(dp[0], "%Y-%m-%d").date()
            #date, open, high, low, close, volume, adj_close
            cur.execute('''INSERT INTO '''+self.priceDB+
                        ''' (code, open, high, low, close, volume, date) VALUES
                        (%s,%s,%s,%s,%s,%s,%s)''',
                        (self.code, dp[1],dp[2],dp[3],dp[4],dp[5], date,))
        return cur

    def ComdCurConstructor(self, cur, data):
        pass




#***can add more checks based on day of week and whatnot
def shouldCheckForUpdate(date):
    shouldCheck = True
    nextDate = date+datetime.timedelta(days=1)
    todayDate = datetime.datetime.now().date()
    if  nextDate >= todayDate :
        shouldCheck = False
    return shouldCheck

def updateData(assetType):
    apiAssetDict = adb.assetsAPIs

    def getAndPushData(assetDatePairs, apiAsset, assetType, priceDB):
        for pair in assetDatePairs:
            code=pair[0]
            startDate=pair[1]
            if startDate:
                if not shouldCheckForUpdate(startDate):
                    continue
                startDate = str(startDate+datetime.timedelta(days=1))
            #****SO JENKY
            if assetType == 'INDEX':
                code = 'INDEX_'+code
            gqad = dd.GetQuandlAPIData(code, apiAsset, startDate)
            pageJson = gqad.getQuandlData()
            if assetType == 'INDEX': code = code.replace('INDEX_','')
            ucd = UploadAssetData(code, pageJson, assetType, priceDB)
            ucd.startUpdate()

    dbs = adb.assetDBs[assetType]
    newAssets, existingAssets = dataToUpdate(dbs)
    getAndPushData(newAssets, apiAssetDict[assetType], assetType, dbs[1])
    getAndPushData(existingAssets, apiAssetDict[assetType], assetType, dbs[1])


def dataToUpdate(dbs):
    cu = CheckUpdate(dbs[0],dbs[1])
    newAssets=cu.getNewAssetForUpdate()
    existingAssets=cu.getExistingAssetsForUpdate()
    return newAssets, existingAssets


if __name__ == "__main__":
    updateData('CURNCY')

    #index updating
    updateData('INDEX')
    updateIndex.updateIndices()





