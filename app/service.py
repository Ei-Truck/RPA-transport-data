from app.database import conecta_primeiro, conecta_segundo, encerra_conexao


def pegar_dados(tabela, colunas):
    try:
        conn = conecta_primeiro()
        cursor = conn.cursor()
        nomes_colunas = ", ".join(colunas)
        comando = f"SELECT {nomes_colunas} FROM {tabela};"
        cursor.execute(comando)
        dados = cursor.fetchall()
        return dados
    except Exception as e:
        raise RuntimeError(f"Erro ao pegar os dados da tabela {tabela}: {e}")
    finally:
        encerra_conexao(conn)


def chamar_procedure(tabela):
    try:
        procedure = f"prc_atualiza_{tabela}"
        conn = conecta_segundo()
        cursor = conn.cursor()
        comando = f"CALL {procedure}();"
        cursor.execute(comando)
        conn.commit()
        delete_tabela_temp(tabela)
    except Exception as e:
        raise RuntimeError(f"Erro ao chamar a procedure {procedure}(): {e}")
    finally:
        encerra_conexao(conn)


def criar_tabela_temp(tabela, colunas_tipo):
    try:
        conn = conecta_segundo()
        cursor = conn.cursor()
        delete_tabela_temp(tabela)
        colunas_tipo_ajustadas = [
            (c, "text" if t == "character varying" else t) for c, t in colunas_tipo
        ]
        campos = ", ".join(f"{c} {t}" for c, t in colunas_tipo_ajustadas)
        comando = f"CREATE TABLE {tabela}_temp(id serial, {campos});"
        cursor.execute(comando)
        conn.commit()
    except Exception as e:
        raise RuntimeError(f"Erro ao criar tabela temporária {tabela}_temp: {e}")
    finally:
        encerra_conexao(conn)


def delete_tabela_temp(tabela):
    try:
        conn = conecta_segundo()
        cursor = conn.cursor()
        comando = f"DROP TABLE IF EXISTS {tabela}_temp;"
        cursor.execute(comando)
        conn.commit()
    except Exception as e:
        raise RuntimeError(f"Erro ao deletar a tabela temporária {tabela}_temp: {e}")
    finally:
        encerra_conexao(conn)


def pegar_colunas(tabela):
    try:
        conn = conecta_primeiro()
        cursor = conn.cursor()
        comando = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{tabela}';"
        cursor.execute(comando)
        colunas = cursor.fetchall()
        return [
            coluna[0]
            for coluna in colunas
            if coluna[0] not in ["id", "transaction_made", "updated_at", "is_inactive"]
        ]
    except Exception as e:
        raise RuntimeError(
            f"Erro ao pegar os nomes das colunas da tabela {tabela}: {e}"
        )
    finally:
        encerra_conexao(conn)


def pegar_colunas_tipo(tabela):
    try:
        conn = conecta_primeiro()
        cursor = conn.cursor()
        comando = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{tabela}';"
        cursor.execute(comando)
        colunas = cursor.fetchall()
        return [
            coluna
            for coluna in colunas
            if coluna[0]
            not in ["id", "transaction_made", "isupdated", "isinactive", "isdeleted"]
        ]
    except Exception as e:
        raise RuntimeError(
            f"Erro ao pegar os nomes das colunas da tabela {tabela}: {e}"
        )
    finally:
        encerra_conexao(conn)


def pegar_tabelas(schema):
    try:
        conn = conecta_primeiro()
        cursor = conn.cursor()
        comando = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}';"
        cursor.execute(comando)
        tabelas = cursor.fetchall()
        return [tabela[0] for tabela in tabelas]
    except Exception as e:
        raise RuntimeError(f"Erro ao pegar tabelas do schema {schema}: {e}")
    finally:
        encerra_conexao(conn)


def inserir_dados(dados, tabela, colunas):
    try:
        conn = conecta_segundo()
        cursor = conn.cursor()
        quantidade_parametros = ", ".join(
            ["(" + ", ".join(["%s"] * len(colunas)) + ")" for _ in dados]
        )
        nomes_colunas = f"({', '.join(colunas)})"
        comando = (
            f"INSERT INTO {tabela}_temp {nomes_colunas} VALUES {quantidade_parametros};"
        )
        valores = [item for tupla in dados for item in tupla]
        cursor.execute(comando, valores)
        conn.commit()
    except Exception as e:
        raise RuntimeError(f"Erro ao inserir dados: {e}")
    finally:
        encerra_conexao(conn)
