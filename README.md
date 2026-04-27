# Nautilus MCP Server

## O que é

É um servidor [Model Context Protocol (MCP)](https://modelcontextprotocol.io) que fala com o assistente pela entrada/saída padrão (**stdio**). Está publicado no npm como **`nautilus-mcp-server`** e precisa de **Node.js 18 ou superior**. Recomenda-se executar com **`npx -y nautilus-mcp-server@latest`** para o `npx` resolver sempre o dist-tag **latest** (evita reutilizar cache de instalação antiga). Se precisar travar em uma versão exata, use `nautilus-mcp-server@<versão>` no lugar de `@latest`.

## O que faz

- **Bancos SQL** (PostgreSQL, MySQL/MariaDB, SQLite, SQL Server, Oracle): listar tabelas, ver colunas e índices, executar apenas consultas de leitura (`SELECT` / `WITH`) com validação de segurança e limite de linhas, e pequenas amostras de dados.
- **MongoDB**: listar coleções, buscar documentos com filtro em JSON e paginação, ver índices da coleção, amostra rápida.
- **Redis**: listar chaves (com padrão e cursor), ler valores conforme o tipo da chave (string, hash, lista, set, sorted set), amostra rápida.

Em muitas respostas o cliente recebe também **`structuredContent`** (JSON estruturado) além do texto, para quem consome colunas, linhas, documentos ou chaves sem depender só de texto formatado.

Comportamento padrão: **somente leitura** no SQL; limites globais de tamanho de query, timeout e número de linhas/registros podem ser ajustados por variáveis de ambiente (veja abaixo).

## Tools

| Tool | Função |
|------|--------|
| **`db_list_connections`** | Lista os `connection_id` configurados, tipo de engine e `read_only`. Use antes das demais tools para escolher o id correto. |
| **`db_list_resources`** | SQL: tabelas. MongoDB: coleções (parâmetro `database` obrigatório). Redis: chaves (`key_pattern` e `cursor` opcionais). |
| **`db_get_metadata`** | SQL: metadados/colunas da tabela. MongoDB: amostra de documentos. Redis: tipo e amostra do valor da chave. |
| **`db_describe_indexes`** | Índices de uma tabela SQL ou de uma coleção MongoDB. |
| **`db_query_sql`** | Executa `SELECT`/`WITH` em conexões SQL. `output_format`: `table`, `json` ou `csv`; `limit` opcional (respeitando o teto global). |
| **`db_fetch_documents`** | MongoDB: `find` com `filter_json`, `limit` e `skip`. |
| **`db_read_cache`** | Redis: leitura conforme o tipo da chave. |
| **`db_peek_sample`** | Até três registros: linhas (SQL), documentos (MongoDB) ou amostra (Redis). |

## Resources (MCP)

| URI | Conteúdo |
|-----|----------|
| **`nautilus://connections`** | JSON com `{ "connections": [...] }` (mesmos campos que `db_list_connections`: `connection_id`, `type`, `read_only`). |

## Como configurar as conexões

Cada conexão tem um **identificador** (`connection_id`): letras, números e underscores (ex.: `pg_prod`, `mongo_app`).

Todas as variáveis seguem o padrão:

`DATABASES__<connection_id>__<atributo>`

### Obrigatório por conexão

- **`type`**: um entre `postgresql`, `mysql`, `mariadb` (tratado como MySQL), `sqlite`, `sqlserver`, `oracle`, `mongodb`, `redis`.
- **Conexão**, em um dos dois formatos:
  - **`url`**: URL completa de conexão (ex.: `postgresql://user:senha@host:5432/banco`, `mongodb://host:27017/`, `redis://host:6379/0`, arquivo/path para SQLite conforme documentação do driver).
  - **Ou campos separados** (útil se a senha tem `@`, `:`, `/`, etc.), sempre no formato `DATABASES__<connection_id>__…`: **`host`**, **`port`**, **`user`**, **`password`**, **`database`** ou **`db`**. Exemplo com id `pg`: `DATABASES__pg__host`, **`DATABASES__pg__port`** (ex.: `5432`), `DATABASES__pg__user`, `DATABASES__pg__password`, `DATABASES__pg__database`. Em vez de `port` separado, a porta pode ir junto em `host` (ex.: `db.exemplo.com:3306`). Se **`port`** for omitida e não estiver no `host`, usam-se portas padrão por tipo (PostgreSQL 5432, MySQL 3306, SQL Server 1433, Oracle 1521, MongoDB 27017, Redis 6379).

### Opcional por conexão

- **`read_only`**: padrão é somente leitura.

### Limites e opções globais (opcional)

| Variável | Efeito (padrão se omitida) |
|----------|----------------------------|
| `NAUTILUS_QUERY_MAX_LENGTH` | Tamanho máximo da query SQL em caracteres (2000). |
| `NAUTILUS_MAX_ROW_LIMIT` ou `NAUTILUS_MAX_ROWS` | Teto de linhas/registros e validação de `LIMIT` (200). |
| `NAUTILUS_DEFAULT_ROW_LIMIT` | Limite padrão quando a tool não fixa outro (50, até o máximo). |
| `NAUTILUS_QUERY_TIMEOUT_MS` | Timeout das operações em milissegundos (5000 se não usar a de segundos). |
| `NAUTILUS_QUERY_TIMEOUT_SECONDS` | Alternativa ao timeout em ms (só usada se `NAUTILUS_QUERY_TIMEOUT_MS` não estiver definida). |
| `NAUTILUS_READ_ONLY_MODE` | Modo somente leitura (`true` por padrão). |

### Exemplos (`.env` ou `env` do MCP)

PostgreSQL por URL:

```env
DATABASES__pg__type=postgresql
DATABASES__pg__url=postgresql://user:password@localhost:5432/mydb
```

PostgreSQL com host e senha com caracteres especiais:

```env
DATABASES__pg__type=postgresql
DATABASES__pg__host=localhost
DATABASES__pg__port=5432
DATABASES__pg__user=myuser
DATABASES__pg__password=pa@ss:word/with
DATABASES__pg__database=mydb
```

MySQL com host e porta no mesmo campo:

```env
DATABASES__app__type=mysql
DATABASES__app__host=db.exemplo.com:3306
DATABASES__app__user=app
DATABASES__app__password=secret
DATABASES__app__database=appdb
```

## Como usar nos clientes

Em qualquer cliente MCP com transporte **stdio**, configure:

- **Comando**: `npx` (no Windows, se necessário, use o caminho completo de `npx.cmd`).
- **Argumentos**: use `["-y", "nautilus-mcp-server@latest"]`. Opcionalmente fixe versão com `["-y", "nautilus-mcp-server@<versão>"]`.
- **Variáveis de ambiente**: todas as `DATABASES__…` e, se quiser, as `NAUTILUS__…`.

Exemplo de trecho JSON (Cursor, VS Code com MCP, etc.):

```json
{
  "mcpServers": {
    "nautilus": {
      "command": "npx",
      "args": ["-y", "nautilus-mcp-server@latest"],
      "env": {
        "DATABASES__pg__type": "postgresql",
        "DATABASES__pg__host": "localhost",
        "DATABASES__pg__port": "5432",
        "DATABASES__pg__user": "app",
        "DATABASES__pg__password": "secret",
        "DATABASES__pg__database": "appdb"
      }
    }
  }
}
```

### Cursor

**Configurações → MCP** (ou JSON de MCP do projeto): adicione o servidor com `command`, `args` e `env` como acima.

### Claude Desktop

Use o mesmo esquema de `command`, `args` e `env` no arquivo de configuração MCP do aplicativo. Caminho e formato exato variam por sistema; veja a [documentação da Anthropic sobre MCP](https://docs.anthropic.com/en/docs/agents-and-tools/mcp).

### Claude Code e outras IDEs

Mesma ideia: processo `npx` (ou Node apontando para o pacote instalado), argumentos `-y` e `nautilus-mcp-server@latest`, e o mesmo bloco de variáveis de ambiente.

### Problemas comuns no Windows com `npx`

Se aparecer **`EPERM`**, **`TAR_ENTRY_ERROR`** ou **`MODULE_NOT_FOUND`** (por exemplo em dependências sob o cache do npm), feche o cliente MCP, apague a pasta **`%LocalAppData%\npm-cache\_npx`** e tente de novo. Outra opção estável é instalar o pacote globalmente (`npm install -g nautilus-mcp-server`) e configurar o MCP para executar o **`node`** com o caminho do `cli.js` dessa instalação global, em vez de `npx`.
