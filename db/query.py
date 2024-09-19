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

cursor = conn.cursor()

# Realizar a consulta no banco de dados
# cursor.execute('SELECT time, status, count FROM transactions')
# data = cursor.fetchall()

query = '''
SELECT
    time,
    SUM(CASE WHEN status = 'approved' THEN count ELSE 0 END) AS approved,
    SUM(CASE WHEN status = 'denied' THEN count ELSE 0 END) AS denied,
    SUM(CASE WHEN status = 'refunded' THEN count ELSE 0 END) AS refunded,
    SUM(CASE WHEN status = 'failed' THEN count ELSE 0 END) AS failed,
    SUM(CASE WHEN status = 'processing' THEN count ELSE 0 END) AS processing,
    SUM(CASE WHEN status = 'reversed' THEN count ELSE 0 END) AS reversed,
    SUM(CASE WHEN status = 'backend_reversed' THEN count ELSE 0 END) AS backend_reversed
FROM
    transactions
GROUP BY
    time
ORDER BY
    time;
'''

# Executar a consulta e armazenar o resultado em um DataFrame
df_pivot = pd.read_sql(query, conn)

# Exibir o DataFrame resultante
print(df_pivot.head(50))

# Fechar a conexão
conn.close()