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
        
def pegar_dados(banco, tabela):
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
    try:
        if banco == "primeiro":
            conn = conecta_primeiro()
        elif banco == "segundo":
            conn = conecta_segundo()
        else:
            raise ValueError("Banco de dados inválido. Use 'primeiro' ou 'segundo'.")
        cursor = conn.cursor()
        comando = f"SELECT * FROM {tabela};"
        cursor.execute(comando)
        dados = cursor.fetchall()
        return dados
    except Exception as e:
        print(f"Erro ao pegar os dados da tabela {tabela} no banco do {banco}: {e}")
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