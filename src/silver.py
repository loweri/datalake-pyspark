import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

log = logging.getLogger(__name__)


def transform_silver(spark: SparkSession, path_bronze: str, path_silver: str) -> int:
    """Lê a camada Bronze, limpa, calcula médias móveis e salva na Silver."""
    log.info("🧹 Lendo Camada Bronze de %s...", path_bronze)
    df_bronze = spark.read.format("delta").load(path_bronze)

    # 1. Limpeza e Padronização de Tipos com .select() explícito
    df_clean = df_bronze \
        .withColumn("date",        F.to_date(F.col("Date"))) \
        .withColumn("open_price",  F.round(F.col("Open").cast("double"), 4)) \
        .withColumn("high_price",  F.round(F.col("High").cast("double"), 4)) \
        .withColumn("low_price",   F.round(F.col("Low").cast("double"), 4)) \
        .withColumn("close_price", F.round(F.col("Close").cast("double"), 4)) \
        .withColumn("volume",      F.col("Volume").cast("long")) \
        .select("ticker_code", "date", "open_price", "high_price", "low_price", "close_price", "volume", "ingestion_timestamp") \
        .filter(F.col("close_price").isNotNull()) \
        .dropDuplicates(["ticker_code", "date"])

    # 2. Window Specs
    w21  = Window.partitionBy("ticker_code").orderBy("date").rowsBetween(-20, 0)
    w200 = Window.partitionBy("ticker_code").orderBy("date").rowsBetween(-199, 0)

    # 3. Engenharia de Recursos (Window Functions em PySpark)
    df_silver = df_clean \
        .withColumn("daily_return_pct", F.round(((F.col("close_price") - F.col("open_price")) / F.col("open_price")) * 100, 4)) \
        .withColumn("sma_21",  F.round(F.avg("close_price").over(w21), 4)) \
        .withColumn("sma_200", F.round(F.avg("close_price").over(w200), 4)) \
        .withColumn("year",    F.year("date")) \
        .withColumn("month",   F.month("date"))

    # 4. Salvar na Camada Silver como Delta Table (Particionado por Ticker)
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("ticker_code") \
        .save(path_silver)

    total = df_silver.count()
    log.info("✨ Camada Silver concluída: %d registros enriquecidos.", total)
    return total
