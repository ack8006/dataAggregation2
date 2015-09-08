from pandas.io import data, wb
from pandas_datareader import data, wb
import pandas.io.data as web
import datetime



class PandasDataAggregator():
    #dates are strings
    def __init__(self, dataSources, dataTag, sDate, eDate):
        self.dataSources = dataSources
        self.dataTag = dataTag
        self.sDate = datetime.datetime.strptime(sDate,'%Y-%m-%d').date()
        self.eDate = datetime.datetime.strptime(eDate,'%Y-%m-%d').date()

    def getData(self):
        for source in self.dataSources:
            data = self.requestData(source)
            if not data.empty: return data

    def requestData(self, source):
        data = web.DataReader(self.dataTag, source, self.sDate, self.eDate)
        print data
        return data





class PandasWorldBankSearch():
    pass





def test():
    #yahoo, google, fred
    pda = PandasDataAggregator('yahoo'
                               'F',
                               '2015-05-01',
                               '2015-08-31')
    print pda.getData()

if __name__=="__main__":
    test()

