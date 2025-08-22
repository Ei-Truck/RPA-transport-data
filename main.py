from service import pegar_dados_para_trasferir, pegar_id_inativos, pegar_id_atualizados, pegar_colunas, pegar_dados_atualizados, inativar_dados, inserir_dados, atualizar_dados, pegar_tabelas

def atualizar_banco():
    lista_tabelas = pegar_tabelas('public')
    string_tabelas = ', '.join(lista_tabelas)
    print(f'Começando o procedimento nas tabelas: {string_tabelas}')
    for tabela in lista_tabelas:
        print(f'Tabela: {tabela}')
        # Pegando colunas da tabela
        colunas = pegar_colunas(tabela)
        # Criar tabela temp

        # Inserir na tabela temp

        # Puxar proc do banco
    print('Banco atualizado!')
   

atualizar_banco()