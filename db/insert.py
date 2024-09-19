import psycopg2
import pandas as pd
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT
)
df = pd.read_csv('../transactions_1.csv')

cursor = conn.cursor()

# Inserindo os dados do DataFrame original (sem pivot)
try:
    for index, row in df.iterrows():
        cursor.execute('''
            INSERT INTO transactions (time, status, count)
            VALUES (%s, %s, %s)
            ON CONFLICT (time, status)
            DO UPDATE SET
                count = EXCLUDED.count;
        ''', (row['time'], row['status'], row['f0_']))
    print("Dados inseridos com sucesso!")
except Exception as e:
    print(e)

conn.commit()

# Fechar a conexão
cursor.close()
conn.close()

