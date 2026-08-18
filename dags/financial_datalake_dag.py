"""
financial_datalake_dag.py — Orquestração de Pipeline Data Lakehouse no Airflow 3
================================================================================
DAG responsável por agendar, orquestrar e monitorar a esteira
Medalhão (Bronze -> Silver -> Gold) com PySpark e Delta Lake.

Padrões de Produção:
  - Lazy Imports para isolamento de dependências e otimização do Scheduler.
  - Resolução dinâmica de PROJECT_DIR via pathlib e variáveis de ambiente.
  - Injeção de JAVA_HOME e PYSPARK_PYTHON nos workers do Airflow.
  - Idempotência em todas as tarefas via Delta Lake Overwrite particionado.
"""

from datetime import datetime, timedelta
import os
import sys
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def _resolve_project_dir() -> str:
    """Resolve o PROJECT_DIR usando múltiplas estratégias de fallback."""
    env_dir = os.environ.get("DATALAKE_PROJECT_DIR", "")
    if env_dir and Path(env_dir, "src").is_dir():
        return env_dir

    candidate = str(Path(__file__).resolve().parent.parent)
    if Path(candidate, "src").is_dir():
        return candidate

    return str(Path.cwd())


# Caminho do projeto Data Lakehouse resolvido dinamicamente
PROJECT_DIR = _resolve_project_dir()
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


def _resolve_java_home() -> str:
    """Detecta automaticamente o JAVA_HOME do sistema."""
    java_home = os.environ.get("JAVA_HOME", "")
    if java_home and Path(java_home, "bin", "java").exists():
        return java_home

    fallback = Path.home() / ".jdk17"
    if (fallback / "bin" / "java").exists():
        return str(fallback)

    return ""


def get_spark_session(app_name: str = "AirflowFinancialDataLake"):
    """Cria a SparkSession com suporte ao Delta Lake e injeção de JVM (Lazy Import)."""
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    java_home = _resolve_java_home()
    if java_home:
        os.environ["JAVA_HOME"] = java_home
        os.environ["PATH"] = os.path.join(java_home, "bin") + os.pathsep + os.environ.get("PATH", "")

    venv_python = Path(PROJECT_DIR) / ".venv" / "bin" / "python3"
    if venv_python.exists():
        os.environ.setdefault("PYSPARK_PYTHON", str(venv_python))

    builder = SparkSession.builder \
        .appName(app_name) \
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

    spark = get_spark_session("Airflow_Market_Bronze")
    try:
        total = ingest_bronze(spark, TICKERS, PATH_BRONZE)
        print(f"✅ Ingestão Bronze concluída via Airflow: {total} registros.")
        return total
    finally:
        spark.stop()


def task_transform_silver():
    """Task 2: Limpeza, deduplicação e cálculo de indicadores na Silver."""
    if PROJECT_DIR not in sys.path:
        sys.path.insert(0, PROJECT_DIR)
    from src.silver import transform_silver

    spark = get_spark_session("Airflow_Market_Silver")
    try:
        total = transform_silver(spark, PATH_BRONZE, PATH_SILVER)
        print(f"✨ Transformação Silver concluída via Airflow: {total} registros.")
        return total
    finally:
        spark.stop()


def task_load_gold():
    """Task 3: Carga analítica e agregação de Tabela Fato na Gold."""
    if PROJECT_DIR not in sys.path:
        sys.path.insert(0, PROJECT_DIR)
    from src.gold import load_gold

    spark = get_spark_session("Airflow_Market_Gold")
    try:
        total = load_gold(spark, PATH_SILVER, PATH_GOLD_FACT)
        print(f"🎉 Carga Gold concluída via Airflow: {total} registros na Tabela Fato.")
        return total
    finally:
        spark.stop()


default_args = {
    "owner": "data-engineering",
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
    tags=["pyspark", "delta-lake", "datalakehouse", "b3", "nasdaq", "medallion"],
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
