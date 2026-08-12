import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from datetime import datetime
from delta import configure_spark_with_delta_pip
from src.silver import transform_silver

@pytest.fixture(scope="session")
def spark():
    """Fixture que cria uma SparkSession local leve configurada com Delta Lake."""
    builder = SparkSession.builder \
        .appName("PyTestSpark") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "1") \
        .master("local[1]")
    return configure_spark_with_delta_pip(builder).getOrCreate()

def test_transform_silver_pipeline(spark, tmp_path):
    """Testa a função transform_silver verificando gravação na Silver e cálculos."""
    path_bronze = str(tmp_path / "bronze")
    path_silver = str(tmp_path / "silver")

    # 1. Criar dados sintéticos de teste para a Bronze
    schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", LongType(), True),
        StructField("ticker_code", StringType(), False),
        StructField("ingestion_timestamp", TimestampType(), True),
    ])

    data = [
        ("2025-08-11", 10.0, 11.0, 9.5, 10.5, 1000, "PETR4.SA", datetime.now()),
        ("2025-08-12", 10.5, 12.0, 10.0, 11.0, 1500, "PETR4.SA", datetime.now()),
    ]

    df_test_bronze = spark.createDataFrame(data, schema=schema)
    df_test_bronze.write.format("delta").save(path_bronze)

    # 2. Executar a função transform_silver da pasta src/
    total_silver = transform_silver(spark, path_bronze, path_silver)

    # 3. Asserções do Teste Unitário
    assert total_silver == 2, "Deveria ter processado exatamente 2 registros na Silver"

    # 4. Validar se a Delta Table Silver foi gravada corretamente
    df_result = spark.read.format("delta").load(path_silver)
    row_day1 = df_result.filter(df_result.date == "2025-08-11").collect()[0]
    
    # Valida se o retorno percentual de 10.0 para 10.5 foi +5.0%
    assert row_day1.daily_return_pct == 5.0, "O retorno percentual do dia 1 deveria ser 5.0%"
