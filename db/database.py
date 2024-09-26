import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT
)

conn.autocommit = True
cursor = conn.cursor()

cursor.execute("CREATE DATABASE transactions;")

conn.commit()
cursor.close()
conn.close()

print('Database created')
