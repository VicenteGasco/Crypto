#!/bin/bash
# init.sh — Inicializa topics de Kafka y directorios HDFS
# Ejecutar una vez el clúster esté arriba: bash scripts/init.sh

set -e

echo "=============================================="
echo "  CryptoExchange — Inicialización del clúster"
echo "=============================================="

# ── Esperar a que Kafka esté listo ──────────────────────────
echo ""
echo "⏳ Esperando a Kafka..."
until docker exec kafka kafka-broker-api-versions \
    --bootstrap-server localhost:9092 > /dev/null 2>&1; do
  sleep 3
  echo "   ... todavía esperando Kafka"
done
echo "✅ Kafka operativo"

# ── Crear topics ─────────────────────────────────────────────
echo ""
echo "📦 Creando topics de Kafka..."

# Topic tickers: 2 particiones (una por par de trading)
# Justificación: particionar por par permite que Spark lea cada
# par en paralelo. Más particiones = más paralelismo, pero
# en entorno local con 1 broker, 2 es suficiente.
docker exec kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic crypto.ticker \
    --partitions 2 \
    --replication-factor 1 \
    --config retention.ms=86400000   # 24h

docker exec kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic crypto.kline \
    --partitions 2 \
    --replication-factor 1 \
    --config retention.ms=86400000

echo "✅ Topics creados:"
docker exec kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --list

# ── Esperar a que HDFS esté listo ───────────────────────────
echo ""
echo "⏳ Esperando a HDFS NameNode..."
until docker exec hdfs-namenode hdfs dfsadmin -report > /dev/null 2>&1; do
  sleep 5
  echo "   ... todavía esperando HDFS"
done
echo "✅ HDFS operativo"

# ── Crear estructura de directorios HDFS ────────────────────
echo ""
echo "📁 Creando directorios HDFS..."

docker exec hdfs-namenode hdfs dfs -mkdir -p /data/cripto/ticker
docker exec hdfs-namenode hdfs dfs -mkdir -p /data/cripto/kline
docker exec hdfs-namenode hdfs dfs -mkdir -p /checkpoints/ticker
docker exec hdfs-namenode hdfs dfs -mkdir -p /checkpoints/kline
docker exec hdfs-namenode hdfs dfs -chmod -R 777 /data
docker exec hdfs-namenode hdfs dfs -chmod -R 777 /checkpoints

echo "✅ Directorios HDFS:"
docker exec hdfs-namenode hdfs dfs -ls -R /data

echo ""
echo "=============================================="
echo "  ✅ Inicialización completa"
echo ""
echo "  Accesos:"
echo "  • Kafka UI:       http://localhost:8090"
echo "  • HDFS UI:        http://localhost:9870"
echo "  • Spark UI:       http://localhost:8080"
echo "  • Prometheus:     http://localhost:9090"
echo "  • Grafana:        http://localhost:3000  (admin/crypto123)"
echo "=============================================="
