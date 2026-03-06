# Teste com todos os bancos em Docker

Este guia sobe todos os bancos suportados (PostgreSQL, MySQL, SQL Server, Oracle, MongoDB, Redis) em containers e configura o MCP no Cursor para conectar neles.

## Pré-requisitos

- Docker e Docker Compose instalados
- Cursor com o workspace aberto na pasta **nautilus-mcp-server** (raiz do projeto)
- Python com dependências instaladas (`pip install -r requirements.txt`)
- **SQL Server**: ODBC Driver 17 ou 18 instalado (para Windows: [Microsoft ODBC Driver for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server))
- **Oracle**: Aguardar ~2 minutos após `docker compose up` para o Oracle XE ficar pronto

## 1. Subir os containers

Na raiz do projeto:

```powershell
docker compose up -d
```

Verifique se todos estão rodando:

```powershell
docker compose ps
```

(Oracle pode levar até ~2 min para passar no healthcheck.) Os serviços `sqlserver-init`, `mongo-init` e `redis-init` populam dados após os bancos subirem.

### Dados de exemplo (read-only)

Todos os bancos sobem já populados para você testar o MCP em modo somente leitura:

| Banco      | Tabelas/coleções/chaves |
|-----------|--------------------------|
| **SQL** (PG, MySQL, SQL Server, Oracle) | `departments` (3 linhas), `employees` (5 linhas), `products` (5 linhas) |
| **MongoDB** | DB `nautilus`: coleções `departments`, `employees`, `products` com os mesmos dados |
| **Redis**   | `app:name`, `user:1`, `user:2` (strings), `recent_queries` (list), `config:app` (hash) |

Exemplos de teste no MCP: `list_tables` (connection_id do SQL), `execute_query_sql` com `SELECT * FROM employees`, `find_documents` no MongoDB, `redis_get` / `redis_keys` no Redis.

## 2. Configurar o MCP no Cursor

### Opção A: Usar arquivo `.env` (recomendado para desenvolvimento)

1. Copie o arquivo de ambiente para `.env`:
   ```powershell
   copy .env.docker .env
   ```
2. No Cursor, abra **Settings** → **MCP** (ou edite `.cursor/mcp.json` na raiz do projeto).
3. Adicione o servidor Nautilus apontando para o projeto e deixe o servidor carregar o `.env` sozinho:

   **Criar ou editar `.cursor/mcp.json`** na raiz do projeto:

   ```json
   {
     "mcpServers": {
       "nautilus": {
         "command": "python",
         "args": ["server.py"]
       }
     }
   }
   ```

   Com isso, ao rodar `python server.py`, o Pydantic/Settings pode carregar o `.env` da raiz se o processo for iniciado com cwd na raiz. **Importante**: o Cursor inicia o MCP com o **cwd = pasta do workspace**. Abra o Cursor com a pasta `nautilus-mcp-server` como raiz do projeto para que `server.py` e o `.env` sejam encontrados.

### Opção B: Colar as variáveis no `mcp.json`

Se o Cursor não carregar o `.env` automaticamente ao iniciar o MCP, passe todas as variáveis no próprio JSON:

1. Copie o conteúdo de **`.cursor/mcp.json.example`** para **`.cursor/mcp.json`** (crie o arquivo se não existir).
2. Ajuste se precisar (por exemplo, driver do SQL Server no Windows: `ODBC Driver 17 for SQL Server` ou `ODBC Driver 18 for SQL Server`).
3. Reinicie o Cursor para o MCP recarregar.

Estrutura esperada do `.cursor/mcp.json` (exemplo com env inline):

```json
{
  "mcpServers": {
    "nautilus": {
      "command": "python",
      "args": ["server.py"],
      "env": {
        "DATABASES__pg_docker__type": "postgresql",
        "DATABASES__pg_docker__url": "postgresql://nautilus:nautilus@localhost:5432/nautilus",
        ...
      }
    }
  }
}
```

O arquivo **`.cursor/mcp.json.example`** já traz todas as variáveis preenchidas para os containers deste compose.

## 3. Verificar conexões

No chat do Cursor, peça ao agente para usar a tool **`list_connections`** do servidor **nautilus**. Deve aparecer algo como:

- `pg_docker` (postgresql)
- `mysql_docker` (mysql)
- `sqlserver_docker` (sqlserver)
- `oracle_docker` (oracle)
- `mongo_docker` (mongodb)
- `redis_docker` (redis)

Em seguida use **`test_connection`** com cada `connection_id` para garantir que todos respondem.

## 4. Portas e credenciais (Docker)

| Banco      | Porta | Usuário  | Senha           | Database/Service   |
|-----------|-------|----------|-----------------|--------------------|
| PostgreSQL| 5432  | nautilus | nautilus        | nautilus           |
| MySQL     | 3306  | nautilus | nautilus        | nautilus           |
| SQL Server| 1433  | sa       | NautilusTest123!| master             |
| Oracle XE | 1521  | system   | oracle          | XEPDB1             |
| MongoDB   | 27017 | -        | -               | -                  |
| Redis     | 6379  | -        | -               | -                  |

## 5. Parar os containers

```powershell
docker compose down
```

Para remover também os volumes (dados):

```powershell
docker compose down -v
```

## Observações

- **SQL Server**: No Windows, a connection string no `.env.docker` e no `mcp.json.example` usa `ODBC Driver 17 for SQL Server`. Se você tiver só o driver 18, altere para `ODBC Driver 18 for SQL Server` e adicione `TrustServerCertificate=yes` se for ambiente de teste.
- **Oracle**: O container `gvenzl/oracle-xe` demora para subir. Use `docker compose logs -f oracle` para acompanhar. O usuário `nautilus` (APP_USER) também é criado; para usá-lo: `oracle://nautilus:nautilus@localhost:1521/XEPDB1`.
- **Cursor**: O MCP é iniciado pelo Cursor com o diretório de trabalho igual à pasta do workspace. Mantenha a raiz do workspace em `nautilus-mcp-server` para que `python server.py` e o `.env` funcionem.
