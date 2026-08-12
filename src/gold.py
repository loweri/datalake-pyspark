import logging
from pyspark.sql import SparkSession, functions as F

log = logging.getLogger(__name__)


def load_gold(spark: SparkSession, path_silver: str, path_gold_fact: str) -> int:
    """Lê a camada Silver, aplica o Data Quality Gate e grava a Tabela Fato Gold."""
    log.info("🥇 Lendo Camada Silver de %s...", path_silver)
    df_silver = spark.read.format("delta").load(path_silver)

    # 1. Selecionar colunas OLAP e Data Quality Gate (Remover NaNs/Nulos)
    df_gold_fact = df_silver \
        .filter(~F.isnan(F.col("close_price"))) \
        .filter(F.col("close_price").isNotNull()) \
        .select(
            "ticker_code", "date", "year", "month",
            "open_price", "high_price", "low_price", "close_price",
            "volume", "daily_return_pct", "sma_21", "sma_200"
        )

    # 2. Salvar na Camada Gold como Delta Table (Particionado por Ano e Mês)
    df_gold_fact.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .save(path_gold_fact)

    total = df_gold_fact.count()
    log.info("🎉 Camada Gold concluída: %d registros na Fato Gold.", total)
    return total
