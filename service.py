from database import conecta_primeiro, conecta_segundo, encerra_conexao

def inserir_dados(dados, tabela, values):
    try:
        conn = conecta_segundo()
        cursor = conn.cursor()
        quantidade_parametros = f"({', '.join(['%s'] * len(values))})"
        for i in range(len(values)):
            nomes_colunas = ", "
            nomes_colunas = f"({nomes_colunas.join([values[i]])})"
        comando = f"INSERT INTO {tabela} {nomes_colunas} VALUES {quantidade_parametros};"
        cursor.execute(comando, dados)
        conn.commit()
        print("Dados inseridos com sucesso!")
    except Exception as e:
        print(f"Erro ao inserir dados: {e}")
    finally:
        encerra_conexao(conn)
        
def pegar_dados(banco, tabela):
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