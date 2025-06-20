# 🚀 Pull Request


### 🏷️ Commits Semânticos

#### AJUDINHA PRA QUEM NÃO SABE

| **Tipo**   | **O que significa**                                           | **Exemplo de commit**                             |
| ---------- | ------------------------------------------------------------- | ------------------------------------------------- |
| `feat`     | Nova funcionalidade (feature)                                 | `feat(auth): add JWT authentication`              |
| `fix`      | Correção de bug                                               | `fix(api): handle null pointer exception`         |
| `docs`     | Alterações na documentação                                    | `docs(readme): update installation instructions`  |
| `style`    | Formatação, espaços, ponto e vírgula, sem alteração lógica    | `style(button): adjust padding and colors`        |
| `refactor` | Refatoração de código sem alterar comportamento               | `refactor(db): optimize query for user lookup`    |
| `perf`     | Mudança que melhora performance                               | `perf(api): cache user data for faster response`  |
| `test`     | Adicionar ou corrigir testes                                  | `test(auth): add tests for login failure cases`   |
| `chore`    | Atualizações que não modificam src ou testes (build, configs) | `chore(ci): add GitHub Actions workflow`          |
| `build`    | Mudanças relacionadas a build ou dependências                 | `build: update webpack to version 5`              |
| `ci`       | Alterações em arquivos de integração contínua                 | `ci: fix TravisCI build script`                   |
| `revert`   | Reversão de commit anterior                                   | `revert: fix(api): handle null pointer exception` |



**EM INGLÊS, ESCREVA ASSIM**
<br>
tipo(escopo):pequena descrição

✅ Exemplos:
| Tipo       | Exemplo de mensagem                                       | Proposito                             |
| ---------- | --------------------------------------------------------- | ------------------------------------- |
| `feat`     | `feat(auth): add JWT authentication to login system`      | Introduces a new feature              |
| `fix`      | `fix(api): correct 500 error when saving user`            | Fixes a bug                           |
| `docs`     | `docs(readme): update usage section with new command`     | Changes only in documentation         |
| `style`    | `style(button): adjust padding and color for consistency` | Formatting changes (no code logic)    |
| `refactor` | `refactor(db): optimize query logic for performance`      | Code refactoring (no behavior change) |
| `chore`    | `chore(ci): add GitHub Actions for PR validation`         | Tooling/config/build process updates  |
| `test`     | `test(api): add unit tests for user controller`           | Adding or updating tests              |


## 🏷️ Nome da PR

**Use o seguinte padrão no título da PR:**
(Tipo)-(Versão)/o_que_foi_implementado_ou_corrigido

Exemplos:
- `Feature-01/adicao_login_com_jwt`
- `Fix-04/corrige_bug_ao_salvar_cliente`
- `Chore-12/atualiza_dependencias`

> 🔔 **Tipos comuns**: `Feature`, `Fix`, `Chore`, `Refactor`, `Docs`, `Test`, etc.

---

## 🧠 Descrição

Explique de forma objetiva o que foi feito neste PR. Seja direto, mas inclua o contexto necessário para que qualquer pessoa da equipe entenda.

## Sua Descrição: 



Exemplo:
> Este PR adiciona a funcionalidade de login com autenticação via JWT, além de corrigir um bug que impedia o redirecionamento correto após o login.

---

## ✅ Checklist

- [ ] Os commits estão em inglês e com mensagens descritivas
- [ ] O título da PR segue o padrão definido
- [ ] A funcionalidade foi testada localmente com sucesso
- [ ] Não foram observados efeitos colaterais ou bugs após as alterações
- [ ] O código foi revisado rapidamente antes de abrir a PR
- [ ] Comentários desnecessários ou código morto foram removidos
- [ ] A branch está atualizada com a `main`
- [ ] **Os labels apropriados foram adicionados**
- [ ] **Um code reviewer foi designado**
- [ ] **O code reviewer comentou o PR confirmando que está tudo certo**