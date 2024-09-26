from fastapi import FastAPI
import psycopg2
import pandas as pd
import asyncio
from datetime import datetime, timedelta
from fastapi.responses import StreamingResponse
from typing import AsyncGenerator
from decimal import Decimal
from alert import check_alert_conditions
from db.config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT


app = FastAPI()

# Variável global para armazenar o último tempo buscado
last_time = None

# Função para coletar os dados de um minuto específico do banco de dados
def get_data_for_minute(minute):
    # Conectar ao banco de dados
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
)

    cursor = conn.cursor()

    # Consulta SQL para buscar os dados do minuto subsequente
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
    WHERE
        time = %s
    GROUP BY
        time;
    '''
    
    # Executar a consulta
    cursor.execute(query, (minute,))
    result = cursor.fetchall()

    # Obter os nomes das colunas
    columns = [desc[0] for desc in cursor.description]

    # Fechar a conexão
    cursor.close()
    conn.close()

    # Converter o resultado em um DataFrame pandas
    if result:
        df = pd.DataFrame(result, columns=columns)
        return df.to_dict(orient="records")
    else:
        return []

# Função para converter Decimal para float
def convert_decimal_to_float(data):
    for item in data:
        for key, value in item.items():
            if isinstance(value, Decimal):
                item[key] = float(value)
    return data

# Função geradora para streaming de dados
async def stream_real_time_data() -> AsyncGenerator[str, None]:
    global last_time

    # Se for a primeira execução, definir o último minuto como "00h 00" ou algum minuto inicial
    if last_time is None:
        last_time = "00h 00"

    while True:
        # Buscar o próximo minuto (subsequente) no formato "HHh MM"
        current_time = datetime.strptime(last_time, "%Hh %M") + timedelta(minutes=1)
        next_minute = current_time.strftime("%Hh %M")

        # Buscar os dados para o minuto subsequente
        data = get_data_for_minute(next_minute)

        # Converter Decimal para float
        data = convert_decimal_to_float(data)

        alerts = check_alert_conditions(data, next_minute)
        # Atualizar o último minuto consultado
        last_time = next_minute

        # Enviar os dados coletados como um pedaço do stream
        if data:
            yield f"Dados para {next_minute}: {data}\n"
        else:
            yield f"Sem dados para {next_minute}\n"

        if alerts:
            for alert in alerts:
                yield f"*** {alert} ***\n"

        # Pausar por 1 minuto de forma não bloqueante
        await asyncio.sleep(.01)

# Rota para fazer o streaming dos dados em tempo real
@app.get("/transactions/")
async def real_time_data():
    return StreamingResponse(stream_real_time_data(), media_type="text/plain")
