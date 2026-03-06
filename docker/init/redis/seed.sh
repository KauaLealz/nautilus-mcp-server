#!/bin/sh
# Nautilus: dados de exemplo para teste read-only (Redis)
set -e
echo "Aguardando Redis..."
sleep 2
redis-cli -h redis ping
redis-cli -h redis SET "app:name" "Nautilus MCP"
redis-cli -h redis SET "user:1" '{"id":1,"name":"Ana Silva","role":"admin"}'
redis-cli -h redis SET "user:2" '{"id":2,"name":"Bruno Santos","role":"user"}'
redis-cli -h redis LPUSH "recent_queries" "SELECT * FROM employees" "SELECT * FROM products"
redis-cli -h redis HSET "config:app" "version" "1.0" "env" "docker"
echo "Redis seed concluído."
