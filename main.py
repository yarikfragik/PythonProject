import pandas as pd
import json
import psycopg2
import os

with open('config.json') as config_file:
    config = json.load(config_file)

db_config = config['database']
table_name = config['table_name']
exel_file = config['exel_file']
csv_file = config['csv_file']


DB_NAME = db_config['db_name']
USER = db_config['user']
PASSWORD = db_config['password']
HOST = db_config['host']
PORT = db_config['port']



conn = psycopg2.connect( database=DB_NAME, user=USER, password=PASSWORD, host=HOST, port=PORT )
cursor = conn.cursor()

cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
exists = cursor.fetchone()

if not exists:
    cursor.execute(f'CREATE DATABASE {DB_NAME}')
    print(f"DB {DB_NAME} created.")
else:
    print(f"DB {DB_NAME} alredy exists.")

create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(255),
        second_name VARCHAR(255),
        age int,
        average_mark float,
        gender VARCHAR(255),
        phone VARCHAR(255)
    );
    """

cursor.execute(create_table_query)
conn.commit()


df = pd.read_excel(exel_file)
df = df.dropna(subset=['average mark'])
df[['first name','second name']]=df['student name'].str.split(' ',n=1,expand=True)
df = df.drop(columns='student name')
if not os.path.exists(csv_file):
    df.to_csv(csv_file, index=False,mode="w")
else: print("CSV file already exists")

for _,row in df.iterrows():
     query_insert = f"""INSERT INTO {table_name} (first_name, second_name, 
     age, average_mark,gender,phone)
 VALUES (%s, %s, %s, %s, %s, %s);
 """
     cursor.execute(query_insert,(row["first name"],row["second name"],row["age"],
                                row["average mark"],row["gender"],row["phone number"]))
conn.commit()

count_query = f"""
select gender,count(*)
from {table_name}
where average_mark > 5
group by gender
"""
cursor.execute(count_query)
result = cursor.fetchall()
df_result = pd.DataFrame(result,columns=["gender","count"])
print(df_result)


conn.close()
cursor.close()