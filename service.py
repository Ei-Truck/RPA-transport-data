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

def inserir_dados(dados, tabela, colunas):
    try:
        conn = conecta_segundo()
        cursor = conn.cursor()
        quantidade_parametros = f"{', '.join(['(%s)'] * len(dados))}"
        for i in range(len(colunas)):
            nomes_colunas = ", "
            nomes_colunas = f"({nomes_colunas.join([colunas[i]])})"
        comando = f"INSERT INTO {tabela} {nomes_colunas} VALUES {quantidade_parametros};"
        cursor.execute(comando, dados)
        conn.commit()
        print("Dados inseridos com sucesso!")
        print("Atualizando campo no banco do primeiro...")
        ids = pegar_id_transferir(tabela)
        atualizar_campos('transaction_made', tabela, 'true', ids)
        print("Dados atualizados com sucesso!")
    except Exception as e:
        print(f"Erro ao inserir dados: {e}")
    finally:
        encerra_conexao(conn)

def atualizar_dados(dados, tabela, colunas, id):
    try:
        conn = conecta_segundo()
        cursor = conn.cursor()
        dados.append(id)
        for i in range(len(colunas)):
            nomes_colunas = ", "
            nomes_colunas = nomes_colunas.join([f'{colunas[i]} = %s'])
        comando = f"UPDATE {tabela} SET {nomes_colunas} WHERE id = %s;"
        print(comando)
        cursor.execute(comando, dados)
        conn.commit()
        print("Dados atualizados com sucesso!")
        atualizar_campo('isupdated', tabela, 'false', id)
    except Exception as e:
        print(f"Erro ao atualizar dados: {e}")
    finally:
        encerra_conexao(conn)

def inativar_dados(ids, tabela):
    try:
        conn = conecta_segundo()
        cursor = conn.cursor()
        quantidade_parametros = f"{', '.join(['%s'] * len(ids))}"
        comando = f"update {tabela} SET isdeleted = true WHERE id in ({quantidade_parametros});"
        cursor.execute(comando, ids)
        conn.commit()
        print("Dados inativados com sucesso!")
        atualizar_campos('isinactive', 'teste', 'false', ids)
        atualizar_campos('isdeleted', 'teste', 'true', ids)
    except Exception as e:
        print(f"Erro ao inativar dados: {e}")
    finally:
        encerra_conexao(conn)

def atualizar_campos(campo, tabela, valor, ids):
    try:
        conn = conecta_primeiro()
        cursor = conn.cursor()
        quantidade_parametros = f"{', '.join(['%s'] * len(ids))}"
        comando = f"UPDATE {tabela} SET {campo} = {valor} WHERE id in ({quantidade_parametros});"
        cursor.execute(comando, ids)
        conn.commit()
    except Exception as e:
        print(f"Erro ao atualizar campo: {e}")
    finally:
        encerra_conexao(conn)

def atualizar_campo(campo, tabela, valor, id):
    try:
        conn = conecta_primeiro()
        cursor = conn.cursor()
        comando = f"UPDATE {tabela} SET {campo} = {valor} WHERE id = {id}"
        print(comando)
        cursor.execute(comando)
        conn.commit()
    except Exception as e:
        print(f"Erro ao atualizar campo: {e}")
    finally:
        encerra_conexao(conn)