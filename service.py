from database import conecta_primeiro, conecta_segundo, encerra_conexao
        
def pegar_dados_para_trasferir(tabela, colunas):
    try:
        conn = conecta_primeiro()
        cursor = conn.cursor()
        nomes_colunas = f"({', '.join(colunas)})"
        comando = f"SELECT {nomes_colunas} FROM {tabela} where transaction_made = false;"
        cursor.execute(comando)
        dados = cursor.fetchall()
        return [dado[0] for dado in dados]
    except Exception as e:
        print(f"Erro ao pegar os dados para transferir da tabela {tabela}: {e}")
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
        return [coluna[0] for coluna in colunas if coluna[0] != 'id' and coluna[0] != 'transaction_made' and coluna[0] != 'isupdated' and coluna[0] != 'isinactive' and coluna[0] != 'isdeleted']
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
        comando = f"INSERT INTO {tabela} {nomes_colunas} VALUES {quantidade_parametros};"
        cursor.execute(comando, dados)
        conn.commit()
        print('Dados inseridos com sucesso!')
    except Exception as e:
        print(f"Erro ao inserir dados: {e}")
    finally:
        encerra_conexao(conn)
