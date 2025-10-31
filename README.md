# 🤖 RPA Transport Data

O **RPA Transport Data** é um **robô de automação em Python** projetado para **transferir, sincronizar e atualizar dados entre dois bancos de dados PostgreSQL**.  
Ele executa todo o processo de **extração, criação de tabelas temporárias, inserção e atualização via procedures** de forma automatizada.

Ideal para **rotinas ETL internas**, **integrações entre sistemas** ou **migrações de dados** em pipelines corporativos.

---

## 🚀 Tecnologias Utilizadas

| Tecnologia | Função |
|-------------|--------|
| **Python 3** | Linguagem principal |
| **psycopg2** | Conexão e manipulação de bancos PostgreSQL |
| **dotenv** | Gerenciamento de variáveis de ambiente |
| **Docker** | Containerização do robô |
| **Pytest** | Testes automatizados |

---

## 📦 Estrutura do Projeto

```
RPA-transport-data/
│
├── app/
│ ├── main.py # Script principal: executa a rotina completa
│ ├── database.py # Conexões e gerenciamento de bancos (origem e destino)
│ ├── service.py # Funções de extração, criação de tabelas e execução de procedures
│ └── .env # Variáveis de ambiente (não versionadas)
│
├── tests/ # Testes automatizados
├── requirements.txt # Dependências do projeto
├── .env.example # Exemplo de variáveis de ambiente
├── .gitignore
└── pytest.ini # Configuração de testes
```

## ⚙️ Funcionamento

| Ao executar main.py, o sistema realiza automaticamente: |
|---------------------------------------------------------|
| Conecta-se ao banco de origem (conecta_primeiro). |
| Extrai os dados de cada tabela listada. |
| Cria tabelas temporárias equivalentes no banco de destino (conecta_segundo). |
| Insere os dados extraídos nas tabelas temporárias. |
| Executa a procedure associada a cada tabela (ex: prc_atualiza_segmento()). |
| Remove as tabelas temporárias e finaliza a rotina. |

## 🔧 Variáveis de Ambiente

**Crie um arquivo .env baseado em .env.example:**

```env
user_primeiro=usuario_origem
password_primeiro=senha_origem
host_primeiro=host_origem
port_primeiro=5432
database_primeiro=nome_banco_origem

user_segundo=usuario_destino
password_segundo=senha_destino
host_segundo=host_destino
port_segundo=5432
database_segundo=nome_banco_destino
```

## 🏃 Como Executar o Projeto

### 1️⃣ Na raiz do projeto, execute o comando:

```bash
python -m app.main
````

## 🧪 Testes

- **Para executar os testes automatizados:** `python -m pytest`

## 📝 Observações

- As **procedures devem existir previamente** no banco de destino, com o nome padrão `prc_atualiza_<tabela>()`.
- O script é **idempotente**, podendo ser executado diariamente sem duplicar dados.
- Caso alguma tabela falhe, o processo continua para as demais.
- Ideal para execução via **cron**, **Airflow** ou **serviços RPA corporativos**.
- O projeto pode ser expandido para incluir **logs detalhados**, **monitoramento** e **notificações automáticas**.
