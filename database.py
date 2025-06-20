import psycopg2
import os
from dotenv import load_dotenv
from psycopg2 import Error

load_dotenv()

def conecta_primeiro():
    try:
        conn = psycopg2.connect(
            user=os.getenv('user'),
            password= os.getenv('password'),
            host=os.getenv('host'),
            port=os.getenv('port'),
            database=os.getenv('database_primeiro')
        )
        print("Conectado no postgres com sucesso!!")
        return conn
    except Error as e:
        print(f"Ocorreu um erro ao tentar conectar no banco de dados: {e}")

def conecta_segundo():
    try:
        conn = psycopg2.connect(
            user=os.getenv('user'),
            password= os.getenv('password'),
            host=os.getenv('host'),
            port=os.getenv('port'),
            database=os.getenv('database_segundo')
        )
        print("Conectado no postgres com sucesso!!")
        return conn
    except Error as e:
        print(f"Ocorreu um erro ao tentar conectar no banco de dados: {e}")

def encerra_conexao(conn):
    if conn:
        conn.close()
        print("Conexão encerrada com o banco de dados!")