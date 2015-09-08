import dbconnection
from contextlib import closing
import csv

#code, name, datasource, geography, priority

#Indices = [['^DJI', 'Dow Jones Industrial Average', 'USA', 1],
#           ['^GSPC', 'S&P 500', 'USA', 1],
#           ['^IXIC', 'NASDAQ Composite', 'USA', 1],
#           ['^RUT', 'Russell 2000','USA',2],
#           ['^SSEC', 'Shanghai Composite', 'Asia',1],
#           ['^HSI', 'Hang Seng', 'Asia',1],
#           ['^N225', 'Nikkei 225', 'Asia',1],
#           ['^GSPTSE', 'S&P TSX Composite', 'Americas',2],
#           ['^FCHI','CAC 40','Europe',1],
#           ['^GDAXI','DAX','Europe',1],
#           ['^SSMI','Swiss Market','Europe',2],
#           ['^FTSE','FTSE 100','Europe',1],
#           ]


with open('Indicies.csv', 'rb') as csvfile:
    data = [row for row in csv.reader(csvfile.read().splitlines())][1:]


db = dbconnection.start()
with closing(db.cursor()) as cur:
    for index in data:
        cur.execute('''INSERT INTO indices
                    (code, name , datasource) VALUES
                    (%s,%s,'YahooQuandl')''',
                    (index[0],index[2],))

    db.commit()
db.close()
