import psycopg2
import dbconnection
from contextlib import closing


db = dbconnection.start()

with closing(db.cursor()) as cur:
    cur.execute('''CREATE TABLE indices(
                id SERIAL PRIMARY KEY,
                code VARCHAR(16) NOT NULL UNIQUE,
                name text NOT NULL,
                dataSource VARCHAR(16) NOT NULL,
                geography VARCHAR(16),
                priority int)''')

    cur.execute('''CREATE TABLE dailyIndexPrices (
                id SERIAL PRIMARY KEY,
                code VARCHAR(16) NOT NULL,
                open float,
                high float,
                low float,
                close float,
                volume float,
                date date)''')

    db.commit()
db.close()


