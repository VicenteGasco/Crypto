#!/usr/bin/env python3
"""
producer.py — Binance WebSocket → Apache Kafka
Suscribe a @miniTicker y @kline_1m para los pares configurados.
Publica métricas en Prometheus (puerto 8000).
"""

import json
import os
import time
import logging
import threading
import signal
import sys
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from prometheus_client import Gauge, Counter, Histogram, start_http_server

# ─── Configuración ────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
PAIRS_RAW       = os.getenv("PAIRS", "BTCUSDT,ETHUSDT")
PAIRS           = [p.strip().lower() for p in PAIRS_RAW.split(",")]
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "8000"))

# Topics: un topic por tipo de dato (justificación en memoria)
# Separar por tipo permite que Spark consuma solo lo que necesita
# sin deserializar mensajes irrelevantes.
TOPIC_TICKER = "crypto.ticker"   # @miniTicker  — precio actual
TOPIC_KLINE  = "crypto.kline"    # @kline_1m    — velas OHLCV

# ─── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
log = logging.getLogger("producer")

# ─── Métricas Prometheus ──────────────────────────────────────
precio_actual      = Gauge("crypto_precio_actual", "Precio actual del par", ["par"])
volumen_24h        = Gauge("crypto_volumen_24h",   "Volumen en 24h del par",  ["par"])
mensajes_enviados  = Counter("producer_mensajes_total", "Total mensajes enviados a Kafka", ["topic", "par"])
errores_ws         = Counter("producer_errores_ws_total", "Errores WebSocket", ["par"])
latencia_kafka     = Histogram(
    "producer_kafka_latency_seconds",
    "Latencia de envío a Kafka",
    ["topic"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
)

# ─── Kafka Producer ───────────────────────────────────────────
def crear_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    """Crea el producer con reintentos, necesario porque Kafka tarda en arrancar."""
    for intento in range(retries):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",          # espera confirmación del broker
                retries=3,
                compression_type="gzip",
            )
            log.info("✅ Kafka conectado en %s", KAFKA_BOOTSTRAP)
            return p
        except NoBrokersAvailable:
            log.warning("⏳ Kafka no disponible, reintento %d/%d en %ds...", intento+1, retries, delay)
            time.sleep(delay)
    log.error("❌ No se pudo conectar a Kafka tras %d intentos", retries)
    sys.exit(1)


kafka_producer = crear_producer()


# ─── Envío a Kafka ────────────────────────────────────────────
def enviar_kafka(topic: str, key: str, mensaje: dict):
    """Envía un mensaje a Kafka midiendo la latencia."""
    t0 = time.monotonic()
    future = kafka_producer.send(topic, key=key, value=mensaje)
    try:
        future.get(timeout=5)
        latencia_kafka.labels(topic=topic).observe(time.monotonic() - t0)
        mensajes_enviados.labels(topic=topic, par=key).inc()
    except Exception as exc:
        log.error("Error enviando a Kafka [%s]: %s", topic, exc)


# ─── Procesamiento de mensajes WebSocket ─────────────────────
def procesar_ticker(data: dict):
    """Procesa un evento @miniTicker y lo publica en Kafka."""
    par = data.get("s", "").upper()   # ej. BTCUSDT
    mensaje = {
        "tipo":       "ticker",
        "par":        par,
        "precio":     float(data.get("c", 0)),    # precio cierre (actual)
        "open":       float(data.get("o", 0)),
        "high":       float(data.get("h", 0)),
        "low":        float(data.get("l", 0)),
        "volumen":    float(data.get("v", 0)),    # volumen base 24h
        "volumen_q":  float(data.get("q", 0)),    # volumen cotización 24h
        "cambio_pct": float(data.get("P", 0)),    # % cambio 24h
        "timestamp":  int(data.get("E", time.time() * 1000)),
        "ts_iso":     datetime.now(timezone.utc).isoformat(),
    }
    # Actualizar métricas Prometheus
    precio_actual.labels(par=par).set(mensaje["precio"])
    volumen_24h.labels(par=par).set(mensaje["volumen"])

    enviar_kafka(TOPIC_TICKER, par, mensaje)


def procesar_kline(data: dict):
    """Procesa un evento @kline_1m y lo publica en Kafka."""
    k   = data.get("k", {})
    par = data.get("s", "").upper()
    mensaje = {
        "tipo":         "kline",
        "par":          par,
        "intervalo":    k.get("i"),
        "open_time":    k.get("t"),
        "close_time":   k.get("T"),
        "open":         float(k.get("o", 0)),
        "high":         float(k.get("h", 0)),
        "low":          float(k.get("l", 0)),
        "close":        float(k.get("c", 0)),
        "volumen":      float(k.get("v", 0)),
        "cerrada":      k.get("x", False),   # True si la vela está cerrada
        "num_trades":   k.get("n", 0),
        "timestamp":    int(data.get("E", time.time() * 1000)),
        "ts_iso":       datetime.now(timezone.utc).isoformat(),
    }
    enviar_kafka(TOPIC_KLINE, par, mensaje)


# ─── WebSocket ────────────────────────────────────────────────
class BinanceStream:
    """Gestiona la conexión WebSocket a Binance con reconexión automática."""

    BINANCE_WS = "wss://stream.binance.com:9443/stream?streams="

    def __init__(self, pares: list[str]):
        self.pares = pares
        self._ws  = None
        self._running = True
        self._construir_url()

    def _construir_url(self):
        streams = []
        for par in self.pares:
            streams.append(f"{par}@miniTicker")
            streams.append(f"{par}@kline_1m")
        self.url = self.BINANCE_WS + "/".join(streams)
        log.info("🔗 URL WebSocket: %s", self.url)

    def _on_message(self, ws, raw):
        try:
            outer = json.loads(raw)
            data  = outer.get("data", outer)
            tipo  = data.get("e")

            if tipo == "24hrMiniTicker":
                procesar_ticker(data)
            elif tipo == "kline":
                procesar_kline(data)
            else:
                log.debug("Evento desconocido: %s", tipo)
        except json.JSONDecodeError as exc:
            log.warning("JSON inválido: %s", exc)
        except Exception as exc:
            log.error("Error procesando mensaje: %s", exc)

    def _on_error(self, ws, error):
        par_label = ",".join(self.pares)
        errores_ws.labels(par=par_label).inc()
        log.error("❌ WebSocket error: %s", error)

    def _on_close(self, ws, code, msg):
        log.warning("🔌 WebSocket cerrado (code=%s). Reconectando en 5s...", code)

    def _on_open(self, ws):
        log.info("✅ WebSocket conectado para pares: %s", self.pares)

    def ejecutar(self):
        """Bucle principal con reconexión automática."""
        while self._running:
            self._ws = websocket.WebSocketApp(
                self.url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self._ws.run_forever(ping_interval=30, ping_timeout=10)
            if self._running:
                log.info("⏳ Reconectando en 5 segundos...")
                time.sleep(5)

    def parar(self):
        self._running = False
        if self._ws:
            self._ws.close()


# ─── Main ─────────────────────────────────────────────────────
def main():
    log.info("🚀 Iniciando productor para pares: %s", PAIRS)

    # Arrancar servidor de métricas Prometheus
    start_http_server(PROMETHEUS_PORT)
    log.info("📊 Métricas Prometheus en puerto %d", PROMETHEUS_PORT)

    stream = BinanceStream(PAIRS)

    # Manejo de señales para shutdown limpio
    def handle_shutdown(sig, frame):
        log.info("🛑 Señal %s recibida. Parando...", sig)
        stream.parar()
        kafka_producer.flush()
        kafka_producer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT,  handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    stream.ejecutar()


if __name__ == "__main__":
    main()
