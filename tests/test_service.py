import pytest
from unittest.mock import MagicMock, patch
import app.service as service


@pytest.fixture
def mock_conn_cursor():
    """Fixture que cria mock de conexão e cursor"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


def test_pegar_dados(mock_conn_cursor):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.fetchall.return_value = [(1,), (2,), (3,)]

    with patch("app.service.conecta_primeiro", return_value=mock_conn), patch(
        "app.service.encerra_conexao"
    ):
        resultado = service.pegar_dados("tabela_teste", ["coluna1"])

    mock_cursor.execute.assert_called_once_with("SELECT (coluna1) FROM tabela_teste;")
    assert resultado == [1, 2, 3]


def test_chamar_procedure(mock_conn_cursor):
    mock_conn, mock_cursor = mock_conn_cursor

    with patch("app.service.conecta_segundo", return_value=mock_conn), patch(
        "app.service.delete_tabela_temp"
    ) as mock_delete, patch("app.service.encerra_conexao"):
        service.chamar_procedure("usuarios")

    mock_cursor.execute.assert_called_once_with("CALL SP_AtualizaUsuarios();")
    mock_conn.commit.assert_called_once()
    mock_delete.assert_called_once_with("usuarios")


def test_criar_tabela_temp(mock_conn_cursor):
    mock_conn, mock_cursor = mock_conn_cursor

    with patch("app.service.conecta_segundo", return_value=mock_conn), patch(
        "app.service.encerra_conexao"
    ):
        service.criar_tabela_temp("clientes", [("nome", "varchar"), ("idade", "int")])

    mock_cursor.execute.assert_called_once_with(
        "create table clientes_temp(id serial, nome varchar, idade int);"
    )
    mock_conn.commit.assert_called_once()


def test_delete_tabela_temp(mock_conn_cursor):
    mock_conn, mock_cursor = mock_conn_cursor

    with patch("app.service.conecta_segundo", return_value=mock_conn), patch(
        "app.service.encerra_conexao"
    ):
        service.delete_tabela_temp("produtos")

    mock_cursor.execute.assert_called_once_with("DROP TABLE IF EXISTS produtos_temp;")
    mock_conn.commit.assert_called_once()


def test_pegar_colunas(mock_conn_cursor):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.fetchall.return_value = [
        ("id",),
        ("nome",),
        ("transaction_made",),
        ("idade",),
    ]

    with patch("app.service.conecta_primeiro", return_value=mock_conn), patch(
        "app.service.encerra_conexao"
    ):
        resultado = service.pegar_colunas("clientes")

    assert resultado == ["nome", "idade"]


def test_pegar_colunas_tipo(mock_conn_cursor):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.fetchall.return_value = [
        ("id", "serial"),
        ("nome", "varchar"),
        ("transaction_made", "timestamp"),
        ("idade", "int"),
    ]

    with patch("app.service.conecta_primeiro", return_value=mock_conn), patch(
        "app.service.encerra_conexao"
    ):
        resultado = service.pegar_colunas_tipo("clientes")

    assert resultado == [("nome", "varchar"), ("idade", "int")]


def test_pegar_tabelas(mock_conn_cursor):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.fetchall.return_value = [("usuarios",), ("pedidos",)]

    with patch("app.service.conecta_primeiro", return_value=mock_conn), patch(
        "app.service.encerra_conexao"
    ):
        resultado = service.pegar_tabelas("public")

    assert resultado == ["usuarios", "pedidos"]


def test_inserir_dados(mock_conn_cursor):
    mock_conn, mock_cursor = mock_conn_cursor
    dados = [(1,), (2,), (3,)]
    colunas = ["id"]

    with patch("app.service.conecta_segundo", return_value=mock_conn), patch(
        "app.service.encerra_conexao"
    ):
        service.inserir_dados(dados, "clientes", colunas)

    mock_cursor.execute.assert_called_once()
    query, params = mock_cursor.execute.call_args[0]
    assert query.startswith("INSERT INTO clientes_temp (id) VALUES")
    assert params == dados


def test_pegar_dados_exception(mock_conn_cursor):
    mock_conn, mock_cursor = mock_conn_cursor
    # Simula erro no execute
    mock_cursor.execute.side_effect = Exception("Erro no SELECT")

    with patch("app.service.conecta_primeiro", return_value=mock_conn), patch(
        "app.service.encerra_conexao"
    ):
        resultado = service.pegar_dados("tabela_teste", ["coluna1"])

    # Deve retornar None se houver exceção
    assert resultado is None


def test_chamar_procedure_exception(mock_conn_cursor):
    mock_conn, mock_cursor = mock_conn_cursor
    # Simula None retornando na conexão
    with patch("app.service.conecta_segundo", return_value=None), patch(
        "app.service.encerra_conexao"
    ):
        # Deve capturar AttributeError pois conn=None não tem cursor
        service.chamar_procedure("usuarios")


def test_criar_tabela_temp_exception(mock_conn_cursor):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.execute.side_effect = Exception("Erro no CREATE TABLE")

    with patch("app.service.conecta_segundo", return_value=mock_conn), patch(
        "app.service.encerra_conexao"
    ):
        service.criar_tabela_temp("clientes", [("nome", "varchar")])


def test_delete_tabela_temp_exception(mock_conn_cursor):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.execute.side_effect = Exception("Erro no DROP TABLE")

    with patch("app.service.conecta_segundo", return_value=mock_conn), patch(
        "app.service.encerra_conexao"
    ):
        service.delete_tabela_temp("clientes")


def test_pegar_colunas_exception(mock_conn_cursor):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.execute.side_effect = Exception("Erro no SELECT columns")

    with patch("app.service.conecta_primeiro", return_value=mock_conn), patch(
        "app.service.encerra_conexao"
    ):
        resultado = service.pegar_colunas("clientes")

    assert resultado is None


def test_pegar_colunas_tipo_exception(mock_conn_cursor):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.execute.side_effect = Exception("Erro no SELECT columns")

    with patch("app.service.conecta_primeiro", return_value=mock_conn), patch(
        "app.service.encerra_conexao"
    ):
        resultado = service.pegar_colunas_tipo("clientes")

    assert resultado is None


def test_pegar_tabelas_exception(mock_conn_cursor):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.execute.side_effect = Exception("Erro no SELECT tables")

    with patch("app.service.conecta_primeiro", return_value=mock_conn), patch(
        "app.service.encerra_conexao"
    ):
        resultado = service.pegar_tabelas("public")

    assert resultado is None


def test_inserir_dados_exception(mock_conn_cursor):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.execute.side_effect = Exception("Erro no INSERT")

    with patch("app.service.conecta_segundo", return_value=mock_conn), patch(
        "app.service.encerra_conexao"
    ):
        service.inserir_dados([(1,)], "clientes", ["id"])
