from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Caminho absoluto do projeto Data Lakehouse
PROJECT_DIR = "/home/ericl/projetos/datalake-pyspark"
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

# Configurações Globais dos Caminhos do Data Lake
PATH_BRONZE    = os.path.join(PROJECT_DIR, "storage", "bronze")
PATH_SILVER    = os.path.join(PROJECT_DIR, "storage", "silver")
PATH_GOLD_FACT = os.path.join(PROJECT_DIR, "storage", "gold", "fact_stock_prices")

TICKERS = [
    "PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA", "WEGE3.SA", "ABEV3.SA",
    "AAPL", "NVDA", "MSFT", "AMZN", "GOOGL", "TSLA"
]


def get_spark_session():
    """Cria a SparkSession com suporte ao Delta Lake (Lazy Import)."""
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = SparkSession.builder \
        .appName("AirflowFinancialDataLake") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "2") \
        .master("local[*]")
    return configure_spark_with_delta_pip(builder).getOrCreate()


def task_ingest_bronze():
    """Task 1: Ingestão de dados brutos e escrita em Delta Table Bronze."""
    if PROJECT_DIR not in sys.path:
        sys.path.insert(0, PROJECT_DIR)
    from src.bronze import ingest_bronze

    spark = get_spark_session()
    try:
        total = ingest_bronze(spark, TICKERS, PATH_BRONZE)
        print(f"✅ Ingestão Bronze concluída via Airflow: {total} registros.")
    finally:
        spark.stop()


def task_transform_silver():
    """Task 2: Limpeza, deduplicação e cálculo de indicadores na Silver."""
    if PROJECT_DIR not in sys.path:
        sys.path.insert(0, PROJECT_DIR)
    from src.silver import transform_silver

    spark = get_spark_session()
    try:
        total = transform_silver(spark, PATH_BRONZE, PATH_SILVER)
        print(f"✨ Transformação Silver concluída via Airflow: {total} registros.")
    finally:
        spark.stop()


def task_load_gold():
    """Task 3: Carga analítica e agregação de Tabela Fato na Gold."""
    if PROJECT_DIR not in sys.path:
        sys.path.insert(0, PROJECT_DIR)
    from src.gold import load_gold

    spark = get_spark_session()
    try:
        total = load_gold(spark, PATH_SILVER, PATH_GOLD_FACT)
        print(f"🎉 Carga Gold concluída via Airflow: {total} registros na Tabela Fato.")
    finally:
        spark.stop()


default_args = {
    "owner": "Ericles Fernandes Oliveira",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="financial_datalake_pyspark_pipeline",
    default_args=default_args,
    description="Pipeline Data Lakehouse Medallion (Bronze -> Silver -> Gold) com PySpark e Delta Lake",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["pyspark", "delta-lake", "datalakehouse", "b3", "nasdaq"],
) as dag:

    t1 = PythonOperator(
        task_id="ingest_bronze_task",
        python_callable=task_ingest_bronze,
    )

    t2 = PythonOperator(
        task_id="transform_silver_task",
        python_callable=task_transform_silver,
    )

    t3 = PythonOperator(
        task_id="load_gold_task",
        python_callable=task_load_gold,
    )

    # Encadeamento de Dependências da Arquitetura Medalhão
    t1 >> t2 >> t3
