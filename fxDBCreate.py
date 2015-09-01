import psycopg2
import dbconnection
from contextlib import closing


db = dbconnection.start()

with closing(db.cursor()) as cur:
    cur.execute('''CREATE TABLE currencies (
                id SERIAL PRIMARY KEY,
                code VARCHAR(8) NOT NULL UNIQUE,
                name text NOT NULL,
                fromdate date,
                todate date
                priority int)''')

    cur.execute('''CREATE TABLE dailyFXprices (
                id SERIAL PRIMARY KEY,
                code VARCHAR(8) NOT NULL,
                rate numeric NOT NULL,
                highprice numeric,
                lowprice numeric,
                date date)''')

    db.commit()
db.close()


