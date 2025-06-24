from service import pegar_dados, pegar_colunas, inserir_dados

def atualizar_banco(tabela):
    dados_primeiro = pegar_dados("primeiro", tabela)
    dados_segundo = pegar_dados("segundo", tabela)

    quantidade_primeiro = len(dados_primeiro)
    quantidade_segundo = len(dados_segundo)

    print(f"Quantidade de dados na tabela '{tabela}' do primeiro banco: {quantidade_primeiro}")
    print(f"Quantidade de dados na tabela '{tabela}' do segundo banco: {quantidade_segundo}")

    if quantidade_primeiro > quantidade_segundo:
        print("Inserindo novos dados no segundo banco...")
        nomes_colunas = pegar_colunas(tabela)
        nomes_colunas_sem_id = [coluna for coluna in nomes_colunas if nomes_colunas.index(coluna) != 0]
        print(f"Nomes das colunas: {nomes_colunas}")
        dados_primeiro_sem_id = [dado[1:] for dado in dados_primeiro] 
        dados_a_inserir = [dado for dado in dados_primeiro_sem_id if dado not in dados_segundo]
        inserir_dados(dados_a_inserir, tabela, nomes_colunas_sem_id)
    else:
        print("O banco já está atualizado.")

atualizar_banco("teste")