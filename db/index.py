import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT


def create_index():
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE INDEX IF NOT EXISTS idx_time ON transactions(time);
    ''')

    # Confirmar a criação do índice
    conn.commit()

    # Fechar a conexão
    cursor.close()
    conn.close()

# Chamar a função para criar o índice
create_index()
print("Índice criado com sucesso!")
