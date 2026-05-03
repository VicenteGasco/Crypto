-- hive_queries.hql
-- Consultas HQL sobre los datos históricos en HDFS
-- Ejecutar desde: docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000

-- ============================================================
-- 1. Crear tablas externas sobre los datos en HDFS
-- ============================================================

-- Tabla para tickers (precio, volumen, cambio %)
CREATE EXTERNAL TABLE IF NOT EXISTS crypto_ticker (
    tipo        STRING,
    precio      DOUBLE,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    volumen     DOUBLE,
    volumen_q   DOUBLE,
    cambio_pct  DOUBLE,
    timestamp   BIGINT,
    ts_iso      STRING
)
PARTITIONED BY (par STRING, fecha DATE)
STORED AS PARQUET
LOCATION 'hdfs://hdfs-namenode:9000/data/cripto/ticker';

-- Recuperar las particiones existentes automáticamente
MSCK REPAIR TABLE crypto_ticker;


-- Tabla para klines (velas OHLCV)
CREATE EXTERNAL TABLE IF NOT EXISTS crypto_kline (
    tipo        STRING,
    intervalo   STRING,
    open_time   BIGINT,
    close_time  BIGINT,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volumen     DOUBLE,
    cerrada     BOOLEAN,
    num_trades  BIGINT,
    timestamp   BIGINT,
    ts_iso      STRING
)
PARTITIONED BY (par STRING, fecha DATE)
STORED AS PARQUET
LOCATION 'hdfs://hdfs-namenode:9000/data/cripto/kline';

MSCK REPAIR TABLE crypto_kline;


-- ============================================================
-- 2. Consultas analíticas
-- ============================================================

-- Q1: Precio máximo, mínimo y medio por par y día
SELECT
    par,
    fecha,
    MIN(precio)        AS precio_min,
    MAX(precio)        AS precio_max,
    AVG(precio)        AS precio_medio,
    STDDEV(precio)     AS volatilidad,
    COUNT(*)           AS num_ticks
FROM crypto_ticker
GROUP BY par, fecha
ORDER BY par, fecha DESC;


-- Q2: Variación % promedio por hora
SELECT
    par,
    fecha,
    HOUR(FROM_UNIXTIME(timestamp / 1000)) AS hora,
    AVG(cambio_pct)    AS cambio_pct_medio,
    MAX(ABS(cambio_pct)) AS max_variacion_abs
FROM crypto_ticker
GROUP BY par, fecha, HOUR(FROM_UNIXTIME(timestamp / 1000))
ORDER BY par, fecha, hora;


-- Q3: Detección de pump & dump (variación > 3%)
SELECT
    par,
    ts_iso,
    precio,
    cambio_pct,
    CASE WHEN cambio_pct > 3  THEN 'PUMP'
         WHEN cambio_pct < -3 THEN 'DUMP'
    END AS evento
FROM crypto_ticker
WHERE ABS(cambio_pct) > 3
ORDER BY timestamp DESC
LIMIT 50;


-- Q4: Volumen total por par y día
SELECT
    par,
    fecha,
    SUM(volumen)   AS volumen_total,
    SUM(volumen_q) AS volumen_usdt_total
FROM crypto_ticker
GROUP BY par, fecha
ORDER BY fecha DESC;


-- Q5: Correlación precio-volumen (análisis de mercado)
-- Un aumento de precio con aumento de volumen indica tendencia real (no manipulación)
SELECT
    par,
    fecha,
    CORR(precio, volumen) AS correlacion_precio_volumen
FROM crypto_ticker
GROUP BY par, fecha
ORDER BY fecha DESC;


-- Q6: Velas con mayor número de operaciones (períodos de alta actividad)
SELECT
    par,
    fecha,
    FROM_UNIXTIME(open_time / 1000) AS apertura,
    open, high, low, close,
    volumen,
    num_trades
FROM crypto_kline
WHERE cerrada = true
ORDER BY num_trades DESC
LIMIT 20;


-- Q7: Media móvil simple de 20 velas (SMA20) sobre klines
SELECT
    par,
    fecha,
    FROM_UNIXTIME(open_time / 1000) AS tiempo,
    close,
    AVG(close) OVER (
        PARTITION BY par
        ORDER BY open_time
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS sma_20
FROM crypto_kline
WHERE cerrada = true
ORDER BY par, open_time;
