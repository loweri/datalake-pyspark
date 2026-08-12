import logging
from datetime import datetime
import pandas as pd
import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType, TimestampType,
)

log = logging.getLogger(__name__)

# Schema explícito — Schema Enforcement na Bronze
BRONZE_SCHEMA = StructType([
    StructField("Date",                StringType(),    True),
    StructField("Open",                DoubleType(),    True),
    StructField("High",                DoubleType(),    True),
    StructField("Low",                 DoubleType(),    True),
    StructField("Close",               DoubleType(),    True),
    StructField("Volume",              LongType(),      True),
    StructField("ticker_code",         StringType(),    False),
    StructField("ingestion_timestamp", TimestampType(), True),
])


def ingest_bronze(spark: SparkSession, tickers: list[str], path_bronze: str) -> int:
    """Extrai dados do yfinance e grava na camada Bronze como Delta Table."""
    log.info("📦 Iniciando ingestão Bronze para %d tickers...", len(tickers))
    records = []
    for ticker in tickers:
        log.info("Extraindo API: %s", ticker)
        df = yf.Ticker(ticker).history(period="1y").reset_index()
        if df.empty:
            log.warning("Sem dados para: %s", ticker)
            continue
        df["ticker_code"] = ticker
        df["ingestion_timestamp"] = datetime.now()
        df["Date"] = df["Date"].astype(str)
        
        # Filtro estrito das 8 colunas do Schema
        df_filtered = df[["Date", "Open", "High", "Low", "Close", "Volume", "ticker_code", "ingestion_timestamp"]]
        records.append(df_filtered)

    if not records:
        raise ValueError("❌ Nenhum dado extraído da API!")

    df_all = pd.concat(records, ignore_index=True)
    df_spark = spark.createDataFrame(df_all, schema=BRONZE_SCHEMA)

    # Escrever como Delta Table (ACID + Time Travel + Partition Pruning)
    df_spark.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("ticker_code") \
        .save(path_bronze)

    total = df_spark.count()
    log.info("✅ Bronze concluída: %d registros gravados.", total)
    return total
