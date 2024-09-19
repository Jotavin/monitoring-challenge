from fastapi import FastAPI
import psycopg2
import pandas as pd
from db.config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
import time
from datetime import datetime, timedelta
from typing import AsyncGenerator
from fastapi.responses import StreamingResponse

app = FastAPI()

# Função para puxar os dados pivotados do banco de dados
def get_pivoted_data():
    # Conectar ao banco de dados
    conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT
)

    cursor = conn.cursor()

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
    
    cursor.execute(query)
    result = cursor.fetchall()

    # Obter os nomes das colunas
    columns = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    # Converter o resultado em um DataFrame pandas
    df = pd.DataFrame(result, columns=columns)

    # Converter o DataFrame em um dicionário para retorno na API
    return df.to_dict(orient="records")

# Rota para obter os dados pivotados
@app.get("/pivoted_data/")
def get_pivoted_transactions():
    data = get_pivoted_data()
    return {"data": data}