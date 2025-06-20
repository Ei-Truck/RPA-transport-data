from service import pegar_dados_tabela, pegar_nome_colunas, inserir_dados

def atualizar_banco(tabela):
    dados_primeiro = pegar_dados_tabela("primeiro", tabela)
    dados_segundo = pegar_dados_tabela("segundo", tabela)

    quantidade_primeiro = len(dados_primeiro)
    quantidade_segundo = len(dados_segundo)

    print(f"Quantidade de dados na tabela '{tabela}' do primeiro banco: {quantidade_primeiro}")
    print(f"Quantidade de dados na tabela '{tabela}' do segundo banco: {quantidade_segundo}")

    if quantidade_primeiro > quantidade_segundo:
        print("O primeiro banco tem mais dados. Inserindo novos dados no segundo banco...")
        nomes_colunas = pegar_nome_colunas(tabela)
        nomes_colunas = [coluna for coluna in nomes_colunas if nomes_colunas.index(coluna) != 0]
        print(f"Nomes das colunas: {nomes_colunas}")
        dados_primeiro = [dado[1:] for dado in dados_primeiro] 
        novos_dados = [dado for dado in dados_primeiro if dado not in dados_segundo]
        inserir_dados(novos_dados, tabela, nomes_colunas)
    else:
        print("O banco já está atualizado.")

atualizar_banco("teste")