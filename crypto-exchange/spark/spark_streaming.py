#!/usr/bin/env python3
"""
spark_streaming.py — Spark Structured Streaming
Lee de Kafka → Calcula indicadores técnicos → Escribe en HDFS (Parquet)
                                             → Expone métricas en Prometheus
"""

import os
import sys
import time
import logging
import threading
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, BooleanType, TimestampType
)
from prometheus_client import Gauge, Counter, Histogram, start_http_server

# ─── Configuración ────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "localhost:9092")
HDFS_NAMENODE    = os.getenv("HDFS_NAMENODE",    "hdfs://hdfs-namenode:9000")
PROMETHEUS_PORT  = int(os.getenv("PROMETHEUS_PORT", "8001"))
TOPIC_TICKER     = "crypto.ticker"
TOPIC_KLINE      = "crypto.kline"
HDFS_OUTPUT_PATH = f"{HDFS_NAMENODE}/data/cripto"
CHECKPOINT_PATH  = f"{HDFS_NAMENODE}/checkpoints"

# ─── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SPARK] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
log = logging.getLogger("spark-streaming")

# ─── Métricas Prometheus ──────────────────────────────────────
micro_batches_total  = Counter("spark_microbatches_total",   "Total micro-batches procesados")
registros_procesados = Counter("spark_registros_total",      "Total registros procesados", ["par"])
precio_spark         = Gauge("spark_precio_actual",          "Precio calculado por Spark",  ["par"])
sma_20_gauge         = Gauge("spark_sma20",                  "Media móvil 20 periodos",     ["par"])
rsi_gauge            = Gauge("spark_rsi",                    "RSI calculado",               ["par"])
pump_dump_alertas    = Counter("spark_pump_dump_alertas_total", "Alertas pump/dump detectadas", ["par", "tipo"])
batch_duration       = Histogram("spark_batch_duration_seconds", "Duración micro-batch Spark")

# ─── Schemas ──────────────────────────────────────────────────
SCHEMA_TICKER = StructType([
    StructField("tipo",       StringType(),  True),
    StructField("par",        StringType(),  True),
    StructField("precio",     DoubleType(),  True),
    StructField("open",       DoubleType(),  True),
    StructField("high",       DoubleType(),  True),
    StructField("low",        DoubleType(),  True),
    StructField("volumen",    DoubleType(),  True),
    StructField("volumen_q",  DoubleType(),  True),
    StructField("cambio_pct", DoubleType(),  True),
    StructField("timestamp",  LongType(),    True),
    StructField("ts_iso",     StringType(),  True),
])

SCHEMA_KLINE = StructType([
    StructField("tipo",       StringType(),  True),
    StructField("par",        StringType(),  True),
    StructField("intervalo",  StringType(),  True),
    StructField("open_time",  LongType(),    True),
    StructField("close_time", LongType(),    True),
    StructField("open",       DoubleType(),  True),
    StructField("high",       DoubleType(),  True),
    StructField("low",        DoubleType(),  True),
    StructField("close",      DoubleType(),  True),
    StructField("volumen",    DoubleType(),  True),
    StructField("cerrada",    BooleanType(), True),
    StructField("num_trades", LongType(),    True),
    StructField("timestamp",  LongType(),    True),
    StructField("ts_iso",     StringType(),  True),
])


# ─── Spark Session ────────────────────────────────────────────
def crear_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CryptoExchangeStreaming")
        .master("spark://spark-master:7077")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
        # Paquetes Kafka + Hadoop
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-client:3.3.4")
        # Escritura en HDFS
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        # Tamaño micro-batch
        .config("spark.sql.streaming.minBatchesToRetain", "5")
        .getOrCreate()
    )


# ─── Indicadores Técnicos ─────────────────────────────────────
def calcular_indicadores(df):
    """
    Calcula sobre el batch:
    - SMA (Simple Moving Average) sobre ventana de 5 min
    - Variación porcentual respecto al tick anterior
    - Detección de pump/dump: cambio > 3% en la ventana
    """
    # Añadir columna de fecha y hora a partir del timestamp Unix (ms → timestamp)
    df = df.withColumn(
        "event_time",
        F.to_timestamp(F.col("timestamp") / 1000)
    ).withColumn(
        "fecha", F.to_date("event_time")
    )

    # Variación % calculada sobre el campo que viene de Binance
    # (cambio_pct = variación respecto apertura 24h)
    df = df.withColumn(
        "alerta_pump_dump",
        F.when(F.abs(F.col("cambio_pct")) > 3.0, True).otherwise(False)
    )

    return df


def calcular_sma_ventana(spark, df_ticker):
    """
    SMA con ventana deslizante de 5 minutos, deslizamiento 30 segundos.
    Justificación: BTC puede moverse 1-2% en minutos; ventana corta captura
    cambios sin demasiado ruido. Deslizamiento 30s = actualización frecuente.
    """
    return (
        df_ticker
        .withColumn("event_time", F.to_timestamp(F.col("timestamp") / 1000))
        .groupBy(
            "par",
            F.window("event_time", "5 minutes", "30 seconds")
        )
        .agg(
            F.avg("precio").alias("sma_5m"),
            F.max("precio").alias("max_5m"),
            F.min("precio").alias("min_5m"),
            F.avg("volumen").alias("avg_vol_5m"),
            F.count("*").alias("num_ticks"),
            F.stddev("precio").alias("volatilidad_5m"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )


def calcular_rsi(df_kline, periodo: int = 14):
    """
    RSI (Relative Strength Index) con período 14 sobre velas de 1m.
    Fórmula simplificada adaptada a batch Spark.
    Solo usa velas cerradas (cerrada=True) para no contaminar con velas abiertas.
    """
    df_cerradas = df_kline.filter(F.col("cerrada") == True)

    df_con_cambio = df_cerradas.withColumn(
        "cambio", F.col("close") - F.col("open")
    ).withColumn(
        "ganancia", F.when(F.col("cambio") > 0, F.col("cambio")).otherwise(0.0)
    ).withColumn(
        "perdida",  F.when(F.col("cambio") < 0, F.abs(F.col("cambio"))).otherwise(0.0)
    )

    # Promedio de ganancias y pérdidas sobre las últimas N velas por par
    # (en Spark Streaming usamos la ventana como proxy del período)
    resumen = df_con_cambio.groupBy(
        "par",
        F.window(
            F.to_timestamp(F.col("timestamp") / 1000),
            f"{periodo} minutes",
            "1 minute"
        )
    ).agg(
        F.avg("ganancia").alias("avg_ganancia"),
        F.avg("perdida").alias("avg_perdida"),
        F.avg("close").alias("precio_medio"),
    ).withColumn(
        "rs",  F.col("avg_ganancia") / (F.col("avg_perdida") + 1e-10)
    ).withColumn(
        "rsi", 100.0 - (100.0 / (1.0 + F.col("rs")))
    ).withColumn(
        "sobrecompra", F.when(F.col("rsi") > 70, True).otherwise(False)
    ).withColumn(
        "sobreventa",  F.when(F.col("rsi") < 30, True).otherwise(False)
    ).withColumn("window_start", F.col("window.start")).drop("window")

    return resumen


# ─── Callbacks de escritura ───────────────────────────────────
def procesar_batch_ticker(batch_df, batch_id):
    """Procesado de cada micro-batch de tickers."""
    t0 = time.monotonic()
    count = batch_df.count()
    if count == 0:
        return

    log.info("Batch %d: %d registros ticker", batch_id, count)
    micro_batches_total.inc()

    df = calcular_indicadores(batch_df)

    # Actualizar métricas Prometheus
    for row in df.select("par", "precio", "cambio_pct", "alerta_pump_dump").collect():
        precio_spark.labels(par=row["par"]).set(row["precio"])
        registros_procesados.labels(par=row["par"]).inc()
        if row["alerta_pump_dump"]:
            tipo = "pump" if row["cambio_pct"] > 0 else "dump"
            pump_dump_alertas.labels(par=row["par"], tipo=tipo).inc()
            log.warning("⚠️  ALERTA %s en %s: %.2f%%", tipo.upper(), row["par"], row["cambio_pct"])

    # Escribir en HDFS particionado por par y fecha
    # Justificación: consultas habituales filtran por par (BTCUSDT) y rango de fechas.
    # Particionar por ambos evita full-scan del histórico.
    (
        df
        .withColumn("fecha", F.to_date(F.to_timestamp(F.col("timestamp") / 1000)))
        .write
        .mode("append")
        .partitionBy("par", "fecha")
        .parquet(f"{HDFS_OUTPUT_PATH}/ticker")
    )

    batch_duration.observe(time.monotonic() - t0)


def procesar_batch_kline(batch_df, batch_id):
    """Procesado de cada micro-batch de velas."""
    count = batch_df.count()
    if count == 0:
        return

    log.info("Batch %d: %d registros kline", batch_id, count)

    df = batch_df.withColumn(
        "fecha", F.to_date(F.to_timestamp(F.col("timestamp") / 1000))
    )

    (
        df
        .write
        .mode("append")
        .partitionBy("par", "fecha")
        .parquet(f"{HDFS_OUTPUT_PATH}/kline")
    )


# ─── Main ─────────────────────────────────────────────────────
def main():
    log.info("🚀 Iniciando Spark Streaming job")

    # Prometheus
    start_http_server(PROMETHEUS_PORT)
    log.info("📊 Métricas Prometheus en puerto %d", PROMETHEUS_PORT)

    spark = crear_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── Stream TICKER desde Kafka ──────────────────────────────
    df_kafka_ticker = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_TICKER)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    df_ticker = (
        df_kafka_ticker
        .select(
            F.from_json(
                F.col("value").cast("string"),
                SCHEMA_TICKER
            ).alias("data")
        )
        .select("data.*")
        .filter(F.col("par").isNotNull())
    )

    # ── Stream KLINE desde Kafka ───────────────────────────────
    df_kafka_kline = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_KLINE)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    df_kline = (
        df_kafka_kline
        .select(
            F.from_json(
                F.col("value").cast("string"),
                SCHEMA_KLINE
            ).alias("data")
        )
        .select("data.*")
        .filter(F.col("par").isNotNull())
    )

    # ── Queries de escritura ───────────────────────────────────
    query_ticker = (
        df_ticker.writeStream
        .foreachBatch(procesar_batch_ticker)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/ticker")
        .trigger(processingTime="10 seconds")   # micro-batch cada 10s
        .start()
    )

    query_kline = (
        df_kline.writeStream
        .foreachBatch(procesar_batch_kline)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/kline")
        .trigger(processingTime="30 seconds")   # velas se actualizan cada segundo, no hace falta tan frecuente
        .start()
    )

    log.info("✅ Queries Spark activas. Esperando datos...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
