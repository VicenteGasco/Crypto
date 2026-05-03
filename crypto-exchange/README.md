# CryptoExchange Pipeline v1.0

Pipeline de análisis de criptomonedas en tiempo real usando tecnologías Big Data.

```
Binance WebSocket → Kafka → Spark Streaming → HDFS (Parquet)
                                            → Prometheus → Grafana
```

## Requisitos previos

- Docker Desktop (con al menos **6 GB de RAM** asignados)
- Docker Compose v2
- Git

> ⚠️ **Importante**: Antes de arrancar, asegúrate de tener al menos 6 GB libres para Docker.
> En Docker Desktop: Settings → Resources → Memory → 6 GB mínimo.

## Arrancar el proyecto

```bash
# 1. Clonar el repositorio
git clone <tu-repo>
cd crypto-exchange

# 2. Levantar toda la infraestructura
docker compose up --build -d

# 3. Esperar 2-3 minutos y luego inicializar Kafka + HDFS
bash scripts/init.sh
```

Eso es todo. El pipeline empieza a recibir datos de Binance automáticamente.

## Accesos

| Servicio       | URL                        | Credenciales       |
|----------------|----------------------------|--------------------|
| Grafana        | http://localhost:3000      | admin / crypto123  |
| Kafka UI       | http://localhost:8090      | —                  |
| HDFS Web UI    | http://localhost:9870      | —                  |
| Spark UI       | http://localhost:8080      | —                  |
| Prometheus     | http://localhost:9090      | —                  |
| Productor métr.| http://localhost:8000/metrics | —               |
| Spark métricas | http://localhost:8001/metrics | —               |

## Estructura del proyecto

```
crypto-exchange/
├── docker-compose.yml          # Infraestructura completa
├── hadoop.env                  # Variables de entorno Hadoop/HDFS
│
├── producer/                   # Productor Python (Binance → Kafka)
│   ├── producer.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── spark/                      # Job Spark Streaming
│   ├── spark_streaming.py
│   └── Dockerfile
│
├── prometheus/                 # Configuración de monitorización
│   ├── prometheus.yml
│   └── alerts.yml
│
├── grafana/
│   ├── provisioning/           # Configuración automática de Grafana
│   └── dashboards/             # Dashboards JSON (infra + negocio)
│
├── hive/
│   └── hive_queries.hql        # Consultas históricas HQL
│
└── scripts/
    └── init.sh                 # Inicializa topics Kafka y dirs HDFS
```

## Topics de Kafka

| Topic          | Contenido            | Particiones | Retención |
|----------------|----------------------|-------------|-----------|
| crypto.ticker  | Precio actual (1/s)  | 2           | 24h       |
| crypto.kline   | Velas OHLCV (1/min)  | 2           | 24h       |

**Justificación del diseño**: Se usan dos topics separados por tipo de dato (ticker vs kline)
en lugar de uno por par. Esto permite que los consumidores de Spark se suscriban solo
al tipo de dato que necesitan, reduciendo la deserialización innecesaria. Con 2 particiones
por topic Spark puede leer cada par en un executor diferente.

## Estructura HDFS

```
/data/
  cripto/
    ticker/
      par=BTCUSDT/
        fecha=2025-01-15/
          part-00000.parquet
      par=ETHUSDT/
        fecha=2025-01-15/
          part-00000.parquet
    kline/
      par=BTCUSDT/
        ...
/checkpoints/
  ticker/
  kline/
```

**Justificación del particionado**: Particionar por `par` y luego por `fecha` optimiza las
consultas más habituales: "Dame todos los datos de BTC del día de ayer". Solo se leen
los ficheros de esa partición, sin tocar el histórico completo.

## Factor de replicación HDFS

Configurado a **1** (en lugar del valor por defecto de 3).

**Justificación**: En un clúster Docker de desarrollo con un único DataNode, una
replicación de 3 es imposible (no hay 3 nodos donde distribuir las réplicas). HDFS
la ignoraría o lanzaría avisos continuos. En producción con 3+ DataNodes se
recuperaría a replicación 3.

## Indicadores técnicos implementados

| Indicador   | Ventana            | Deslizamiento | Justificación                                      |
|-------------|--------------------|---------------|----------------------------------------------------|
| SMA 5m      | 5 minutos          | 30 segundos   | Detecta tendencia sin exceso de ruido para BTC     |
| RSI 14      | 14 minutos (klines)| 1 minuto      | Período estándar del mercado para detectar extremos|
| Pump & dump | por batch          | —             | Alerta inmediata si cambio_pct > 3%                |

## Consultas HQL históricas

```bash
# Conectar a Hive desde el contenedor
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000

# Luego ejecutar las queries del fichero hive/hive_queries.hql
```

## Parar el clúster

```bash
docker compose down

# Para borrar también los datos almacenados:
docker compose down -v
```

## Solución de problemas

**Kafka no arranca**: Espera 60-90 segundos tras `docker compose up`. Kafka tarda
en sincronizar con Zookeeper.

**Spark no conecta a HDFS**: Asegúrate de haber ejecutado `scripts/init.sh` para
que los directorios `/data` y `/checkpoints` existan en HDFS.

**Sin datos en Grafana**: Verifica que el productor está corriendo:
```bash
docker logs crypto-producer -f
```

**Memoria insuficiente**: Reduce `SPARK_WORKER_MEMORY` en el `docker-compose.yml`
de 1G a 512m si tu máquina tiene menos de 8GB de RAM.
