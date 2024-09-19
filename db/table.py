import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT
)

cursor = conn.cursor()

# Criação da tabela original no banco de dados
try:
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        time VARCHAR(10),
        status VARCHAR(50),
        count NUMERIC,
        PRIMARY KEY (time, status)
    );
    ''')
    print("Tabela criada com sucesso!")
except Exception as e:
    print(e)


conn.commit()

# Fechar a conexão
cursor.close()
conn.close()
