from database import conecta_primeiro, conecta_segundo, encerra_conexao
        
def pegar_dados_para_trasferir(tabela, colunas):
    try:
        conn = conecta_primeiro()
        cursor = conn.cursor()
        for i in range(len(colunas)):
            nomes_colunas = ", "
            nomes_colunas = f"({nomes_colunas.join([colunas[i]])})"
        comando = f"SELECT {nomes_colunas} FROM {tabela} where transaction_made = false;"
        cursor.execute(comando)
        dados = cursor.fetchall()
        return [dado[0] for dado in dados]
    except Exception as e:
        print(f"Erro ao pegar os dados da tabela {tabela}: {e}")
        return None
    finally:
        encerra_conexao(conn)

def pegar_id_transferir(tabela):
    try:
        conn = conecta_primeiro()
        cursor = conn.cursor()
        comando = f"SELECT id FROM {tabela} where transaction_made = false;"
        cursor.execute(comando)
        dados = cursor.fetchall()
        return [dado[0] for dado in dados]
    except Exception as e:
        print(f"Erro ao pegar os dados da tabela {tabela}: {e}")
        return None
    finally:
        encerra_conexao(conn)

def pegar_id_inativos(tabela):
    try:
        conn = conecta_primeiro()
        cursor = conn.cursor()
        comando = f"SELECT id FROM {tabela} where isinactive = true;"
        cursor.execute(comando)
        dados = cursor.fetchall()
        return [dado[0] for dado in dados]
    except Exception as e:
        print(f"Erro ao pegar os dados da tabela {tabela}: {e}")
        return None
    finally:
        encerra_conexao(conn)

def pegar_dados_atualizados(tabela, colunas):
    try:
        conn = conecta_primeiro()
        cursor = conn.cursor()
        for i in range(len(colunas)):
            nomes_colunas = ", "
            nomes_colunas = f"({nomes_colunas.join([colunas[i]])})"
        comando = f"SELECT {nomes_colunas} FROM {tabela} where isupdated = true;"
        cursor.execute(comando)
        dados = cursor.fetchall()
        return [dado[0] for dado in dados]
    except Exception as e:
        print(f"Erro ao pegar os dados da tabela {tabela}: {e}")
        return None
    finally:
        encerra_conexao(conn)

def pegar_id_atualizados(tabela):
    try:
        conn = conecta_primeiro()
        cursor = conn.cursor()
        comando = f"SELECT id FROM {tabela} where isupdated = true;"
        cursor.execute(comando)
        dados = cursor.fetchall()
        return [dado[0] for dado in dados]
    except Exception as e:
        print(f"Erro ao pegar os dados da tabela {tabela}: {e}")
        return None
    finally:
        encerra_conexao(conn)

def pegar_colunas(tabela):
    try:
        conn = conecta_primeiro()
        cursor = conn.cursor()
        comando = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{tabela}';"
        cursor.execute(comando)
        colunas = cursor.fetchall()
        return [coluna[0] for coluna in colunas]
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
    finally:
        encerra_conexao(conn)