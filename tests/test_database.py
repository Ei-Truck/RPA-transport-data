import pytest
from unittest.mock import patch, MagicMock
from app import database


def test_conecta_primeiro_sucesso():
    mock_conn = MagicMock()
    with patch("psycopg2.connect", return_value=mock_conn):
        conn = database.conecta_primeiro()
    assert conn == mock_conn


def test_conecta_primeiro_exception():
    with patch("psycopg2.connect", side_effect=Exception("Erro na conexão")):
        conn = database.conecta_primeiro()
    # Se der erro, retorna None
    assert conn is None


def test_conecta_segundo_sucesso():
    mock_conn = MagicMock()
    with patch("psycopg2.connect", return_value=mock_conn):
        conn = database.conecta_segundo()
    assert conn == mock_conn


def test_conecta_segundo_exception():
    with patch("psycopg2.connect", side_effect=Exception("Erro na conexão")):
        conn = database.conecta_segundo()
    assert conn is None


def test_encerra_conexao_fecha():
    mock_conn = MagicMock()
    database.encerra_conexao(mock_conn)
    mock_conn.close.assert_called_once()


def test_encerra_conexao_none():
    database.encerra_conexao(None) 
