# nautilus-mcp-server

> Servidor [MCP](https://modelcontextprotocol.io) de **consulta somente-leitura** a bancos de dados — PostgreSQL, MySQL/MariaDB, SQLite, SQL Server, Oracle, MongoDB e Redis — por um único arquivo de conexões, com senhas cifradas.

## O que faz

Expõe seus bancos para um assistente de IA de forma segura: listar estruturas, inspecionar metadados/índices e **executar apenas leituras** (`SELECT`/`WITH` no SQL, `find` no MongoDB, leitura por tipo no Redis), sempre com limite de linhas e timeout. Muitas respostas trazem `structuredContent` (JSON) além do texto.

| Tool | O que faz |
|------|-----------|
| `db_list_connections` | Lista os `connection_id` configurados (tipo e `read_only`). Use primeiro, para escolher o id. |
| `db_list_resources` | Tabelas (SQL), coleções (MongoDB, exige `database`) ou chaves (Redis; `key_pattern`/`cursor`). |
| `db_get_metadata` | Colunas da tabela (SQL), amostra de documentos (MongoDB) ou tipo+amostra da chave (Redis). |
| `db_describe_indexes` | Índices de uma tabela SQL ou coleção MongoDB. |
| `db_query_sql` | Executa `SELECT`/`WITH`. `output_format`: `table`/`json`/`csv`; `limit` opcional. |
| `db_fetch_documents` | MongoDB: `find` com `filter_json`, `limit`, `skip`. |
| `db_read_cache` | Redis: leitura conforme o tipo da chave (string, hash, list, set, zset, stream, RedisJSON). |
| `db_peek_sample` | Até três registros de amostra (linhas, documentos ou valores). |

Recurso MCP `nautilus://connections`: JSON com a lista de conexões.

## Requisitos

- **Python 3.11+**.
- Os drivers de todos os bancos suportados já vêm incluídos — não há extras a instalar.

## Instalação

```bash
# Executar direto (sem instalar), via uv:
uvx nautilus-mcp-server

# Ou instalar as ferramentas (servidor + CLI `nautilus`) no sistema:
uv tool install nautilus-mcp-server
# (alternativa: pip install nautilus-mcp-server)
```

## Configuração

### 1. Gere a chave de criptografia (uma vez)

```bash
nautilus keygen           # gera e salva a chave em ~/.nautilus/key
```

Servidor e CLI leem a chave desse arquivo — **não precisa setar nada no ambiente**. (Em CI/containers, `NAUTILUS_KEY` no ambiente tem precedência sobre o arquivo.)

### 2. Cadastre as conexões pelo CLI

Toda adição/alteração/remoção passa pelo CLI `nautilus`, que edita o YAML e **cifra a senha** ao salvar:

```bash
nautilus add pg_prod      # pergunta type/host/port/user/senha/database (senha oculta)
nautilus list             # lista as conexões (nunca mostra a senha)
nautilus edit pg_prod     # altera campos; senha em branco mantém a atual
nautilus remove pg_prod   # remove a conexão
# --config aponta outro arquivo (padrão: NAUTILUS_CONFIG ou ./nautilus.yaml)
```

O arquivo resultante (veja [`nautilus.yaml.example`](./nautilus.yaml.example)):

```yaml
defaults:                  # herdados por todas as conexões (opcionais)
  read_only: true
  query_timeout_ms: 30000
  max_rows: 200
connections:
  pg_prod:
    type: postgresql       # postgresql | mysql | mariadb | sqlite | sqlserver | oracle | mongodb | redis
    host: db.interno
    port: 5432
    user: leitor
    password: "enc:gAAAAAB..."   # gravado pelo CLI — cifrado, nunca à mão
    database: vendas
```

- Conexão por **componentes** (`host`/`port`/`user`/`password`/`database`) ou por **`url`** completa. SQLite usa `url: ":memory:"`/`"file:..."` ou `path:`.
- Overrides por conexão: `read_only`, `query_timeout_ms`, `connect_timeout_ms` (Redis: `socket_timeout_ms`).
- **Sem `${VAR}`**: todos os valores vão literais no YAML; só a senha é cifrada.

### 3. Aponte o cliente MCP para o servidor

**Nenhuma variável de ambiente é necessária.** Por padrão, o servidor lê o YAML de `~/.nautilus/nautilus.yaml` e a chave de `~/.nautilus/key` — ambos criados pelos passos acima:

```json
{
  "mcpServers": {
    "nautilus": {
      "command": "uvx",
      "args": ["nautilus-mcp-server"],
      "env": {}
    }
  }
}
```

O mesmo bloco vale para Cursor, Claude Desktop, Claude Code e outras IDEs com MCP por stdio.

> **Caminhos personalizados (opcional):** `NAUTILUS_CONFIG` aponta o YAML para outro lugar; `NAUTILUS_KEY` (ou `NAUTILUS_KEY_FILE`) define a chave para CI/containers. Sem eles, usa-se o padrão em `~/.nautilus/`.

## Segurança

- **Somente leitura** por padrão no SQL (validação bloqueia escrita); `read_only: false` libera por conexão.
- Senha **sempre cifrada** no YAML (`enc:`, Fernet); a chave vive em `~/.nautilus/key` (ou `NAUTILUS_KEY`), **fora do YAML** — não compartilhe/commite o arquivo de chave.
- Limite de linhas e timeout por consulta; decode seguro de binário no Redis.
