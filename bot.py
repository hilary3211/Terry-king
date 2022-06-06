from requests import Request, Session
import json
import pprint
import psycopg2

url = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest"

parameters = {"slug": "algorand", "convert": "USD"}

headers = {
    "Accepts": "application/json",
    "X-CMC_PRO_API_KEY": "977b8032-c8ff-4858-83c8-b2ba609aae13",
}

session = Session()
session.headers.update(headers)

response = session.get(url, params=parameters)

price = str(json.loads(response.text)["data"]["4030"]["quote"]["USD"]["price"])

cleaned_price = price.replace("None", "")
print(cleaned_price)
new_price = str(cleaned_price)

hostname = "ec2-3-218-171-44.compute-1.amazonaws.com"
database = "d8d58ci3of9cec"
username = "bubgqsyxbdddup"
pwd = "ef9ebaed10600d914d7ecfec9378d487c80ff05bc6615f0db0396b297c57dd8a"
port_id = "5432"
conn = None
cur = None

try:
    conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id,
    )

    cur = conn.cursor()
    # cur.execute("DROP TABLE IF EXISTS Datasets")
    create_script = """ CREATE TABLE IF NOT EXISTS Datasets (
                            id SERIAL PRIMARY KEY,
                            Filecoin varchar(50) NOT NULL)                        
    """

    cur.execute(create_script)

    insert_sc = "INSERT INTO Datasets (Filecoin) VALUES (%s) "
    insert_val = new_price

    cur.execute(insert_sc, [insert_val])
    conn.commit()

except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
