import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()


def conecta_primeiro():
    try:
        conn = psycopg2.connect(
            os.getenv("connstring_primeiro"),
        )
        return conn
    except Exception as e:
        raise ConnectionError(
            f"Ocorreu um erro ao tentar conectar ao banco de dados do primeiro: {e}"
        )


def conecta_segundo():
    try:
        conn = psycopg2.connect(
            os.getenv("connstring_segundo"),
        )
        return conn
    except Exception as e:
        raise ConnectionError(
            f"Ocorreu um erro ao tentar conectar ao banco de dados do segundo: {e}"
        )


def encerra_conexao(conn):
    if conn:
        conn.close()
