from service import pegar_dados, pegar_tabelas, pegar_colunas, criar_tabela_temp, inserir_dados, chamar_procedure, pegar_colunas_tipo

def atualizar_banco():
    lista_tabelas = pegar_tabelas('public')
    string_tabelas = ', '.join(lista_tabelas)
    print(f'Começando o procedimento nas tabelas: {string_tabelas}')
    for tabela in lista_tabelas:
        print(f'Tabela: {tabela}')
        # Pegando colunas da tabela
        colunas = pegar_colunas(tabela)
        # Criar tabela temp
        criar_tabela_temp(tabela, pegar_colunas_tipo(tabela))
        # Inserir na tabela temp
        inserir_dados(pegar_dados(tabela, colunas), tabela, colunas)
        # Puxar proc do banco
        chamar_procedure(tabela)
    print('Banco atualizado!')
   
atualizar_banco()