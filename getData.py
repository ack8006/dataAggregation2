import requests
import psycopg2
from contextlib import closing
#import quandl
import numpy as np
import pandas as pd
import dbconnection
import datetime
from downloadQuandlAPIData import GetQuandlAPIData



class GetPriceData():
    def __init__(self, tickerInfo):
        #tickerInfo [ticker, assetClass, startDate, endDate]
        #asset types(Curncy, Equity, Comdty, Govt, Index,...
        self.tickerInfo = tickerInfo

    #*** check for proper asset class should occur before this
    #*** This must be update as other asset classes are added
    def tickerDictionary(self, assetClass):
        tickerDict = {'Curncy': {'db':('currencies','dailyFXPrices'),
                                 'api': 'CURRFX'}
                      }
        return tickerDict[assetClass]

    def getPriceData(self):
        assetDict = self.tickerDictionary(self.tickerInfo[1])
        dbPrices = self.getPricesFromDB(assetDict['db'][1])
        if dbPrices:
            return self.convertToDataFrame(dbPrices)
        else:
            gqpd = GetQuandlAPIData(self.tickerInfo[0], assetDict['api'],
                                    self.tickerInfo[2], self.tickerInfo[3])
            quandlData = gqpd.getQuandlData()['data']
            return self.convertToDataFrame(quandlData)

    def getPricesFromDB(self, dbName):
        db = dbconnection.start()
        with closing(db.cursor()) as cur:
            cur.execute('SELECT date, rate, highprice, lowprice FROM ' + dbName +
                            ''' WHERE date >= %s AND date <= %s AND code = %s''',
                        (self.tickerInfo[2], self.tickerInfo[3], self.tickerInfo[0], ))
            dbPrices = cur.fetchall()
        db.close()
        dbPrices = [[str(x[0]), x[1], x[2], x[3]] for x in dbPrices]
        return dbPrices

    def convertToDataFrame(self, pList):
        #date, rate, high, low
        d = {('Rate', self.tickerInfo[0]): pd.Series([x[1] for x in pList],
                        index = [x[0] for x in pList]),
             'High': pd.Series([x[2] for x in pList],
                        index = [x[0] for x in pList]),
             'Low': pd.Series([x[3] for x in pList],
                        index = [x[0] for x in pList])}
        df = pd.DataFrame(d)
        df.columns = [[self.tickerInfo[0], self.tickerInfo[0], self.tickerInfo[0]],
                      ['High','Low','Rate']]
        return df


if __name__ == "__main__":
    gpd = GetPriceData(['USDTHB', 'Curncy', '2015-08-01', '2015-08-30'])
    print gpd.getPriceData()
    gpd = GetPriceData(['USDJPY', 'Curncy', '2015-08-01', '2015-08-30'])
    print gpd.getPriceData()












