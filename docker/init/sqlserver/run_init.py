"""
Executa init.sql no SQL Server (container de seed).
Aguarda o servidor ficar disponível e roda o script.
"""
import os
import sys
import time

def main():
    try:
        import pymssql
    except ImportError:
        print("Instalando pymssql...", flush=True)
        os.system(f"{sys.executable} -m pip install -q pymssql")
        import pymssql

    host = os.environ.get("MSSQL_HOST", "sqlserver")
    user = os.environ.get("MSSQL_SA_USER", "sa")
    password = os.environ.get("MSSQL_SA_PASSWORD", "NautilusTest123!")
    database = os.environ.get("MSSQL_DATABASE", "master")
    init_sql_path = os.environ.get("INIT_SQL_PATH", "/init/init.sql")

    if not os.path.isfile(init_sql_path):
        print(f"Arquivo não encontrado: {init_sql_path}", flush=True)
        sys.exit(1)

    sql = open(init_sql_path, "r", encoding="utf-8").read()
    # divide por GO (batch separator do SQL Server)
    batches = [b.strip() for b in sql.split("GO") if b.strip()]

    for attempt in range(30):
        try:
            conn = pymssql.connect(
                server=host,
                user=user,
                password=password,
                database=database,
            )
            conn.close()
            break
        except Exception as e:
            print(f"Tentativa {attempt + 1}/30: aguardando SQL Server... ({e})", flush=True)
            time.sleep(2)
    else:
        print("SQL Server não ficou disponível a tempo.", flush=True)
        sys.exit(1)

    print("Conectado. Executando init.sql...", flush=True)
    conn = pymssql.connect(
        server=host,
        user=user,
        password=password,
        database=database,
    )
    try:
        cursor = conn.cursor()
        for batch in batches:
            if batch:
                cursor.execute(batch)
        conn.commit()
        print("Init concluído com sucesso.", flush=True)
    except Exception as e:
        print(f"Erro ao executar init: {e}", flush=True)
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
