from database import conecta_primeiro, conecta_segundo, encerra_conexao
        
def pegar_dados(tabela, colunas):
    try:
        conn = conecta_primeiro()
        cursor = conn.cursor()
        nomes_colunas = f"({', '.join(colunas)})"
        comando = f"SELECT {nomes_colunas} FROM {tabela};"
        cursor.execute(comando)
        dados = cursor.fetchall()
        return [dado[0] for dado in dados]
    except Exception as e:
        print(f"Erro ao pegar os dados para transferir da tabela {tabela}: {e}")
        return None
    finally:
        encerra_conexao(conn)

def chamar_procedure(tabela):
    try:
        procedure = f"SP_Atualiza{tabela.capitalize()}"
        conn = conecta_segundo()
        cursor = conn.cursor()
        comando = f"CALL {procedure}();"
        cursor.execute(comando)
        conn.commit()
        delete_tabela_temp(tabela)
    except Exception as e:
        print(f"Erro ao chamar {procedure}(): {e}")
    finally:
        encerra_conexao(conn)

def criar_tabela_temp(tabela, colunas_tipo):
    try:
        conn = conecta_segundo()
        cursor = conn.cursor()
        campos = f"{', '.join(f'{c} {t}' for c, t in colunas_tipo)}"
        comando = f"create table {tabela}_temp(id serial, {campos});"
        cursor.execute(comando)
        conn.commit()
    except Exception as e:
        print(f"Erro ao criar {tabela}_temp: {e}")
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
        print(f"Erro ao deletar a tabela temporária {tabela}_temp: {e}")
    finally:
        encerra_conexao(conn)

def pegar_colunas(tabela):
    try:
        conn = conecta_primeiro()
        cursor = conn.cursor()
        comando = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{tabela}';"
        cursor.execute(comando)
        colunas = cursor.fetchall()
        return [coluna[0] for coluna in colunas if coluna[0] != 'id' and coluna[0] != 'transaction_made' and coluna[0] != 'isupdated' and coluna[0] != 'isinactive' and coluna[0] != 'isdeleted']
    except Exception as e:
        print(f"Erro ao pegar os nomes das colunas da tabela {tabela}: {e}")
        return None
    finally:
        encerra_conexao(conn)

def pegar_colunas_tipo(tabela):
    try:
        conn = conecta_primeiro()
        cursor = conn.cursor()
        comando = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{tabela}';"
        cursor.execute(comando)
        colunas = cursor.fetchall()
        return [coluna for coluna in colunas if coluna[0] != 'id' and coluna[0] != 'transaction_made' and coluna[0] != 'isupdated' and coluna[0] != 'isinactive' and coluna[0] != 'isdeleted']
    except Exception as e:
        print(f"Erro ao pegar os nomes das colunas da tabela {tabela}: {e}")
        return None
    finally:
        encerra_conexao(conn)

def pegar_tabelas(schema):
    try:
        conn = conecta_primeiro()
        cursor = conn.cursor()
        comando = f"select table_name from information_schema.tables where table_schema = '{schema}';"
        cursor.execute(comando)
        tabelas = cursor.fetchall()
        return [tabela[0] for tabela in tabelas]
    except Exception as e:
        print(f"Erro ao pegar tabelas do shema {schema}: {e}")
        return None
    finally:
        encerra_conexao(conn)

def inserir_dados(dados, tabela, colunas):
    try:
        conn = conecta_segundo()
        cursor = conn.cursor()
        quantidade_parametros = f"{', '.join(['(%s)'] * len(dados))}"
        nomes_colunas = f"({', '.join(colunas)})"
        comando = f"INSERT INTO {tabela}_temp {nomes_colunas} VALUES {quantidade_parametros};"
        cursor.execute(comando, dados)
        conn.commit()
    except Exception as e:
        print(f"Erro ao inserir dados: {e}")
    finally:
        encerra_conexao(conn)