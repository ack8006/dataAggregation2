import pandas as pd
import dbconnection
import datetime
from downloadData import GetQuandlAPIData
import updateIndex
import assetDBs as adb



class GetPriceData():
    def __init__(self, tickerInfo):
        #tickerInfo [ticker, assetClass, startDate, endDate]
        self.code = tickerInfo[0]
        self.assetClass = tickerInfo[1]
        self.sDate = tickerInfo[2]
        self.eDate = tickerInfo[3]
        self.assetDBs = adb.assetDBs[tickerInfo[1]]
        self.assetAPI = adb.assetAPIs[tickerInfo[1]]


    def getPriceData(self):
        priceFrame = self.getPricesFromDB()
        if priceFrame.empty:
            priceFrame = self.getPricesFromExtern()
            return priceFrame
        else:
            priceFrame.drop('id',1,inplace=True)
            return priceFrame


    def getPricesFromDB(self):
        engine = dbconnection.startEngine()
        priceFrame = pd.read_sql(("SELECT * FROM " + self.assetDBs[1] +
                          " WHERE code = %(code)s AND "
                          "date <= %(eDate)s AND date >= %(sDate)s"),
                          engine,
                          params={"code":self.code,"eDate": self.eDate,
                                  "sDate":self.sDate})
        return priceFrame


    def getPricesFromExtern(self):
        prefix = adb.quandlPrefix[self.assetClass]

        gqpd = GetQuandlAPIData(prefix+self.code, self.assetAPI, self.sDate, self.eDate)
        quandlDict = gqpd.getQuandlData()
        quandlCols = quandlDict['column_names']
        quandlData = quandlDict['data']
        return self.convertToDataFrame(quandlCols, quandlData)


    def convertToDataFrame(self, cols, data):
        replacementDict={'High (est)':'high','Low (est)':'low'
                        }
        cols = [x.lower() if x not in replacementDict
                else replacementDict[x] for x in cols]
        df = pd.DataFrame(data, columns=cols)
        df['code']=self.code
        return df


if __name__ == "__main__":
    gpd = GetPriceData(['USDTHB', 'CURNCY', '2015-08-01', '2015-08-30'])
    print gpd.getPriceData()
    gpd = GetPriceData(['USDJPY', 'CURNCY', '2015-08-01', '2015-08-30'])
    print gpd.getPriceData()

    gpd = GetPriceData(['IXIC','INDEX','2015-08-01','2015-08-31'])
    print gpd.getPriceData()

    gpd = GetPriceData(['AEX','INDEX','2015-08-01','2015-08-31'])
    print gpd.getPriceData()




