import pandas as pd
import pandas.io.data as web
import datetime
import assetDBs as adb
from update import CheckUpdate
import dbconnection as db


def loopAssetDates(assetsDates):
    for ad in assetsDates:
        code = ad[0]
        date = datetime.date(1900,1,1)
        if ad[1]: date = ad[1]+datetime.timedelta(days=1)

        priceData = getPriceData(code, date)
        if isinstance(priceData, pd.DataFrame):
            storePriceData(code, priceData)

def getPriceData(code, date):
    priceData = None
    try:
        priceData = web.DataReader("^"+code,'yahoo',date)
        priceData = cleanPriceData(priceData)
    except IOError as e:
        print e
        print 'New Data Unavailable ' + code
    return priceData


def storePriceData(code, priceData):
    engine = db.startEngine()
    priceData.to_sql(indexDBs[1], engine, if_exists='append')


def cleanPriceData(priceData):
    priceData = priceData.drop('Adj Close',1)
    priceData.columns = map(str.lower, priceData.columns)
    priceData.index.rename('date', inplace=True)
    priceData['code']=code


def updateIndices():
    indexDBs = adb.assetDBs['INDEX']
    cu = CheckUpdate(indexDBs[0],indexDBs[1])
    newAssets = cu.getNewAssetForUpdate()
    existingAssets = cu.getExistingAssetsForUpdate()

    assetsDates = newAssets+existingAssets
    loopAssetDates(assetsDates)

