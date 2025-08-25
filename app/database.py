import psycopg2
import os
from dotenv import load_dotenv
from psycopg2 import Error

load_dotenv()


def conecta_primeiro():
    try:
        conn = psycopg2.connect(
            user=os.getenv("user_primeiro"),
            password=os.getenv("password_primeiro"),
            host=os.getenv("host_primeiro"),
            port=os.getenv("port_primeiro"),
            database=os.getenv("database_primeiro"),
        )
        return conn
    except Error as e:
        print(f"Ocorreu um erro ao tentar conectar ao banco de dados do primeiro: {e}")


def conecta_segundo():
    try:
        conn = psycopg2.connect(
            user=os.getenv("user_segundo"),
            password=os.getenv("password_segundo"),
            host=os.getenv("host_segundo"),
            port=os.getenv("port_segundo"),
            database=os.getenv("database_segundo"),
        )
        return conn
    except Error as e:
        print(f"Ocorreu um erro ao tentar conectar ao banco de dados do segundo: {e}")


def encerra_conexao(conn):
    if conn:
        conn.close()
