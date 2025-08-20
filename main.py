from service import pegar_dados_para_trasferir, pegar_id_inativos, pegar_id_atualizados, pegar_colunas, pegar_dados_atualizados, inativar_dados, inserir_dados, atualizar_dados, pegar_tabelas

def atualizar_banco():
    lista_tabelas = pegar_tabelas('public')
    string_tabelas = ', '.join(lista_tabelas)
    print(f'Começando o procedimento nas tabelas: {string_tabelas}')
    for tabela in lista_tabelas:
        print(f'Tabela: {tabela}')
        # Pegando colunas da tabela
        colunas = pegar_colunas(tabela)
        #Inserindo dados
        print('Inserindo dados...')
        transferir = pegar_dados_para_trasferir(tabela, colunas)
        if len(transferir) > 0:
            inserir_dados(transferir, tabela, colunas)
        else:
            print('Nenhum dado a ser transferido')
        #Deletando dados
        print('Inativando dados...')
        inativos = pegar_id_inativos(tabela)
        if len(inativos) > 0:
            inativar_dados(inativos, tabela)
        else:
            print('Nenhum dado inativo')
        #Atualizando dados um por um
        print('Atualizando dados...')
        ids = pegar_id_atualizados(tabela)
        if len(ids) > 0:
            for id in ids:
                print(f'Id: {id}')
                atualizar_dados(pegar_dados_atualizados(tabela, colunas), tabela, colunas, id)
        else:
             print('Nenhum dado a ser atualizado')
    print('Banco atualizado!')
   

atualizar_banco()