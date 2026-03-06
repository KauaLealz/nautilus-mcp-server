# Nautilus MCP Server

**\[BETA]** — Servidor MCP (Model Context Protocol) para **acesso seguro** a bancos de dados SQL e NoSQL. Pensado para uso por agentes de IA com proteções contra execução indevida e alucinações (queries destrutivas ou inválidas).

## Bancos suportados

| Tipo  | Bancos        |
|-------|----------------|
| **SQL**   | PostgreSQL, MySQL, SQL Server, Oracle |
| **NoSQL** | MongoDB, Redis               |

## Arquitetura

- **Tools** (`src/tools/`): pontos de entrada MCP (common, sql, nosql).
- **Use cases** (`src/use_cases/`): orquestração e regras de negócio.
- **Domain** (`src/domain/`): ports (interfaces), modelos e validação de segurança (SQL).
- **Adapters** (`src/adapters/sql/`, `src/adapters/nosql/`): uma implementação por banco.
- **Bootstrap** (`src/bootstrap.py`): monta adapters e use cases por configuração.

## Instalação

```bash
cd nautilus-mcp-server
pip install -r requirements.txt
```

## Configuração

1. Copie o arquivo de exemplo e edite com suas conexões:

```powershell
copy .env.example .env
```

2. No `.env`, defina as conexões com o padrão:

- `DATABASES__<connection_id>__type`: tipo do banco (`postgresql`, `mysql`, `sqlserver`, `oracle`, `mongodb`, `redis`).
- `DATABASES__<connection_id>__url`: URL de conexão (sem expor em logs).
- **read_only é padrão (somente leitura)**; informe `DATABASES__<id>__read_only=false` apenas se quiser desabilitar.

Exemplo para uma conexão PostgreSQL:

```env
DATABASES__pg_main__type=postgresql
DATABASES__pg_main__url=postgresql://user:password@localhost:5432/mydb
```

**Usuário ou senha com caracteres especiais (@, :, /):**  
Se usar `url`, codifique na URL (ex.: `@` → `%40`, `:` → `%3A`). Ou use variáveis separadas para que a URL seja montada automaticamente com encoding correto:

```env
DATABASES__pg_main__type=postgresql
DATABASES__pg_main__host=localhost
DATABASES__pg_main__port=5432
DATABASES__pg_main__user=meu_usuario
DATABASES__pg_main__password=senha@com:especiais
DATABASES__pg_main__database=mydb
```

3. (Opcional) Limites de segurança: **timeout** vale para todas as consultas (evita travar a base); **max_rows** é o teto de linhas. O agente pode pedir menos linhas via parâmetro `max_rows` nas tools; se a query tiver `LIMIT`/`TOP`/`FETCH FIRST` acima do cap, é barrada na validação (dry).

```env
NAUTILUS_QUERY_MAX_LENGTH=2000
NAUTILUS_QUERY_TIMEOUT_SECONDS=30
NAUTILUS_MAX_ROWS=500
NAUTILUS_ALLOW_WRITE=false
NAUTILUS_CONFIRM_WRITE_TOKEN=   # Opcional; usado por execute_confirmed_write
```
Para queries salvas, crie `saved_queries.json` na raiz (ou defina `NAUTILUS_SAVED_QUERIES_JSON`). Veja `saved_queries.json.example`.

## Execução

```bash
python server.py
```

O servidor usa transporte stdio para integração com Cursor/IDE (MCP).

## Teste com todos os bancos (Docker)

Para subir PostgreSQL, MySQL, SQL Server, Oracle, MongoDB e Redis em containers e configurar o MCP no Cursor para conectar em todos:

1. Suba os containers: `docker compose up -d`
2. Configure o MCP: use `.env.docker` (copie para `.env`) e/ou `.cursor/mcp.json.example` (copie para `.cursor/mcp.json`).

Detalhes, credenciais e passos no Cursor em **[docs/DOCKER_TEST.md](docs/DOCKER_TEST.md)**.

## Tools disponíveis

### Comuns

- **list_connections**: Lista todas as conexões configuradas (connection_id, tipo, somente leitura).
- **test_connection**: Testa se uma conexão está acessível.

### SQL

- **list_tables**: Lista tabelas de um banco SQL (connection_id, schema opcional).
- **describe_table**: Descreve colunas de uma tabela (nome, tipo, nullable).
- **execute_query_sql**: Executa uma query de leitura (SELECT ou WITH ... SELECT). Só leitura por padrão; INSERT/UPDATE/DELETE/DDL são rejeitados.

### NoSQL

- **list_collections**: Lista coleções de um database MongoDB.
- **find_documents**: Busca documentos em uma coleção MongoDB (filtro JSON, limite).
- **mongodb_aggregate**: Pipeline de agregação read-only ($match, $group, $sort, $limit, etc.; $out/$merge proibidos).
- **redis_get**: Obtém o valor de uma chave Redis.
- **redis_keys**: Lista chaves Redis por pattern (ex.: `user:*`), com limite de chaves.
- **redis_key_type**: Retorna o tipo da chave (string, list, hash, etc.).
- **redis_key_ttl**: Retorna o TTL da chave em segundos.
- **redis_mget**: Retorna valores de várias chaves de uma vez (máx. 50).

### Descoberta e schema

- **list_databases**: Lista databases disponíveis na conexão SQL.
- **get_table_sample**: Retorna N linhas de amostra de uma tabela (sem montar SELECT).
- **get_schema_summary**: Resumo de todas as tabelas do schema (colunas e opcionalmente row count).
- **export_schema_json**: Exporta schema (tabelas + colunas) como JSON.
- **list_indexes**: Lista índices de uma tabela.
- **list_views**: Lista views e definição (quando disponível).
- **get_foreign_keys**: Chaves estrangeiras de uma tabela.
- **get_table_relationships**: Grafo tabela A → tabela B (por FK).
- **get_row_count**: Contagem de linhas (com WHERE opcional).
- **get_column_stats**: Estatísticas de colunas (min, max, avg, count, nulls, distinct).

### Query e export

- **explain_query_sql**: Plano de execução (EXPLAIN) sem executar a query.
- **validate_query_sql**: Valida só a sintaxe da query.
- **execute_query_sql_as_csv**: Executa query e retorna resultado em CSV.
- **execute_query_sql_as_json**: Executa query e retorna resultado como JSON.

### Auditoria e ajuda

- **query_history**: Últimas N queries executadas (connection_id, query, timestamp, row_count).
- **get_connection_capabilities**: Lista as capacidades suportadas pela conexão.
- **suggest_tables**: Sugere tabelas/colunas cujo nome contém um termo.

### Comparação

- **compare_schemas**: Compara schemas de duas conexões SQL (diferenças em tabelas/colunas).
- **run_same_query**: Executa a mesma query em 2+ conexões e retorna resultados lado a lado.

### Queries salvas

- **list_saved_queries**: Lista queries salvas (arquivo `saved_queries.json`).
- **execute_saved_query**: Executa uma query salva com parâmetros (placeholders `{{param}}`). Configure `NAUTILUS_SAVED_QUERIES_JSON` ou use `saved_queries.json` na raiz.

### Write com confirmação (human-in-the-loop)

- **request_pending_write**: Registra um comando de escrita pendente; retorna `pending_id`.
- **execute_confirmed_write**: Executa o write pendente se o `token` coincidir com `NAUTILUS_CONFIRM_WRITE_TOKEN`. Requer `NAUTILUS_ALLOW_WRITE=true`.

## Segurança e proteção contra alucinações

- **Read-only por padrão**: Conexões e validação em código permitem apenas leitura, a menos que `NAUTILUS_ALLOW_WRITE=true` (e opt-in explícito no fluxo).
- **Validação de query SQL**: Allowlist (SELECT, WITH); blocklist de palavras-chave (DROP, ALTER, TRUNCATE, etc.); limite de tamanho, timeout e de linhas retornadas.
- **Introspectação de schema**: Use `list_tables` e `describe_table` antes de montar queries para usar o schema real e reduzir erros e “invenção” de nomes.
- **Identificação por connection_id**: O agente só escolhe entre conexões já configuradas; não há acesso a credenciais ou connection strings.
- **Respostas padronizadas**: Erros retornam mensagens genéricas ao agente; detalhes técnicos podem ser logados apenas no servidor.

## Como adicionar um novo banco

1. Crie um adapter em `src/adapters/sql/` ou `src/adapters/nosql/` que implemente pelo menos `list_connections`, `get_connection_info` e `test_connection`.
2. Para SQL: implemente também `execute_read_only`, `list_tables` e `describe_table`.
3. Registre o adapter em `src/bootstrap.py` (filtro por `config.type`) e adicione ao `connection_to_adapter`.
4. Se for NoSQL com operações específicas, adicione tools em `src/tools/nosql_tools.py` e use `get_adapter(connection_id)`.

## Ideias de novas funcionalidades

Há um documento com sugestões de ferramentas e comportamentos úteis (descoberta de schema, EXPLAIN, perfilamento, export CSV/JSON, NoSQL, auditoria, etc.): [docs/IDEAS_FUNCIONALIDADES.md](docs/IDEAS_FUNCIONALIDADES.md).

## Licença

Projeto de referência para uso com MCP/Cursor.
