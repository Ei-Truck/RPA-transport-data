from service import pegar_dados_para_trasferir, pegar_id_inativos, pegar_id_atualizados, pegar_id_transferir, pegar_colunas, pegar_dados_atualizados, inativar_dados, inserir_dados, atualizar_dados, pegar_tabelas

# def atualizar_banco(tabela):
#     dados_primeiro = pegar_dados("primeiro", tabela)
#     dados_segundo = pegar_dados("segundo", tabela)

#     quantidade_primeiro = len(dados_primeiro)
#     quantidade_segundo = len(dados_segundo)

#     print(f"Quantidade de dados na tabela '{tabela}' do primeiro banco: {quantidade_primeiro}")
#     print(f"Quantidade de dados na tabela '{tabela}' do segundo banco: {quantidade_segundo}")

#     if quantidade_primeiro > quantidade_segundo:
#         print("Inserindo novos dados no segundo banco...")
#         nomes_colunas = pegar_colunas(tabela)
#         nomes_colunas_sem_id = [coluna for coluna in nomes_colunas if nomes_colunas.index(coluna) != 0]
#         print(f"Nomes das colunas: {nomes_colunas}")
#         dados_primeiro_sem_id = [dado[1:] for dado in dados_primeiro] 
#         dados_a_inserir = [dado for dado in dados_primeiro_sem_id if dado not in dados_segundo]
#         inserir_dados(dados_a_inserir, tabela, nomes_colunas_sem_id)
#     else:
#         print("O banco já está atualizado.")

# pegar lista de tabelas:
# print(pegar_tabelas('public'))

# transferir
# print(pegar_dados_para_trasferir('teste', pegar_colunas('teste')))
# print(pegar_id_transferir('teste'))
# inserir_dados(pegar_dados_para_trasferir('teste', pegar_colunas('teste')), 'teste', pegar_colunas('teste'))

# inativar
# print(pegar_id_inativos('teste'))
# inativar_dados(pegar_id_inativos('teste'), 'teste')

# print(pegar_id_atualizados('teste'))
# ids = pegar_id_atualizados('teste')
# print(pegar_dados_atualizados('teste', pegar_colunas('teste')))
# for id in ids:
#     atualizar_dados(pegar_dados_atualizados('teste', pegar_colunas('teste')), 'teste', pegar_colunas('teste'), id)