#https://www.quandl.com/api/v2/datasets.json?query=*&source_code=CURRFX&per_page=300&page=1&auth_token=ZiSfvVWczC5Q3dGUvNM2

import requests
import dbconnection
from contextlib import closing
import datetime



def main():
    data = []
    for x in xrange(1,10):
    #for x in range(1,2):
        url ='''https://www.quandl.com/api/v2/datasets.json?query=*&source_code=CURRFX&per_page=300&page={}&auth_token=ZiSfvVWczC5Q3dGUvNM2'''.format(str(x))
        page = requests.get(url)
        page = page.json()
        newdata = [[pair['code'],pair['description'],pair['from_date'], pair['to_date']]
                    for pair in page['docs'] if pair['code'].startswith('USD')]

        for pair in newdata:
            check = False
            for pair2 in data:
                if pair[0]==pair2[0]:
                    check = True
            if not check:
                data.append(pair)
    print data

    db = dbconnection.start()
    with closing(db.cursor()) as cur:
        for pair in data:
            code = pair[0]
            name = pair[1].split('1 U.S. Dollar in ',1)[1].split('.',1)[0]
            fromdate = datetime.datetime.strptime(pair[2], "%Y-%m-%d").date()
            todate = datetime.datetime.strptime(pair[3], "%Y-%m-%d").date()
            cur.execute('''INSERT INTO currencies
                        (code, name, fromdate, todate) VALUES
                        (%s, %s, %s, %s)''',
                        (code, name, fromdate, todate,))
        db.commit()
    db.close()

if __name__ == '__main__':
    main()
