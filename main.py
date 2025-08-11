from service import pegar_dados_para_trasferir, pegar_id_inativos, pegar_id_atualizados, pegar_id_transferir, pegar_colunas, pegar_dados_atualizados, inativar_dados, inserir_dados, atualizar_dados, pegar_tabelas

def atualizar_banco():
    lista_tabelas = pegar_tabelas('public')
    for i in range(len(lista_tabelas)):
            string_tabelas = ", "
            string_tabelas = f"{string_tabelas.join([lista_tabelas[i]])}"
    print(f'Começando o procedimento nas tabelas: {string_tabelas}')
    for tabela in lista_tabelas:
        print(f'Tabela: {tabela}')
        #Inserindo dados
        print('Inserindo dados...')
        transferir = pegar_dados_para_trasferir(tabela, pegar_colunas(tabela))
        if len(transferir) > 0:
            inserir_dados(transferir, tabela, pegar_colunas(tabela))
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
                atualizar_dados(pegar_dados_atualizados(tabela, pegar_colunas(tabela)), tabela, pegar_colunas(tabela), id)
        else:
             print('Nenhum dado a ser atualizado')
    print('Banco atualizado!')
   
atualizar_banco()