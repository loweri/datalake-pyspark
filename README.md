# 🚀 Data Lakehouse Financeiro — PySpark + Delta Lake + Apache Airflow

![Python](https://img.shields.io/badge/Python-3.14-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5%20%2F%204.1-E25A1C?logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.1%20%2F%204.3-00ADD8)
![Airflow](https://img.shields.io/badge/Apache_Airflow-3.3.0-017CEE?logo=apacheairflow)
![License](https://img.shields.io/badge/license-MIT-green)

[🇧🇷 Português](#português) | [🇺🇸 English](#english)

---

<a name="português"></a>
## 🇧🇷 Português

Este repositório contém a implementação de um **Data Lakehouse** local com arquitetura **Medallion** (Bronze → Silver → Gold), utilizando processamento distribuído em **PySpark**, armazenamento ACID resiliente em **Delta Lake** (com Time Travel e Schema Enforcement) e orquestração automatizada pelo **Apache Airflow**.

### 🏗️ 1. Arquitetura do Pipeline (Camada Bronze)

```mermaid
flowchart TD
    subgraph Origin ["Fontes de Dados (APIs Financeiras)"]
        API1["Yahoo Finance API\n(yfinance)"]
    end

    subgraph Memory ["Processamento Distribuído (PySpark Engine)"]
        SE["Schema Enforcement\n(StructType Validation)"]
        DF["PySpark DataFrame\n(Memória RAM Distribuída)"]
        API1 -->|Payload JSON| SE
        SE -->|Validação OK| DF
    end

    subgraph BronzeLayer ["🥇 Camada Bronze (Data Lake Local)"]
        DeltaBronze["🥉 storage/bronze/\n(Delta Table — Formato Parquet + _delta_log)\nParticionado por ticker_code"]
        DF -->|Delta Write / Overwrite| DeltaBronze
    end
```

### 💡 Decisões de Arquitetura

| Decisão | Justificativa de Engenharia |
| :--- | :--- |
| **Delta Lake (Transações ACID)** | Garante gravação segura (*Atomicidade*). Se a ingestão quebrar no meio, o Data Lake permanece 100% íntegro. |
| **Schema Enforcement (`StructType`)** | Contrato de dados estrito na entrada. Rejeita tipos de dados incompatíveis antes da gravação. |
| **Particionamento por `ticker_code`** | *Partition Pruning*: Permite que consultas futuras leiam apenas as pastas do ativo desejado sem varrer o dataset inteiro. |

---

<a name="english"></a>
## 🇺🇸 English

This repository contains a local **Data Lakehouse** implementation with **Medallion** architecture (Bronze → Silver → Gold), powered by **PySpark** distributed processing, **Delta Lake** ACID resilient storage (Time Travel & Schema Enforcement), and orchestrated by **Apache Airflow**.

### 🏗️ 1. Pipeline Architecture (Bronze Layer)

```mermaid
flowchart TD
    subgraph OriginUS ["Data Sources (Financial APIs)"]
        API1US["Yahoo Finance API\n(yfinance)"]
    end

    subgraph MemoryUS ["Distributed Processing (PySpark Engine)"]
        SEUS["Schema Enforcement\n(StructType Validation)"]
        DFUS["PySpark DataFrame\n(Distributed RAM)"]
        API1US -->|JSON Payload| SEUS
        SEUS -->|Validation OK| DFUS
    end

    subgraph BronzeLayerUS ["🥇 Bronze Layer (Local Data Lake)"]
        DeltaBronzeUS["🥉 storage/bronze/\n(Delta Table — Parquet + _delta_log)\nPartitioned by ticker_code"]
        DFUS -->|Delta Write / Overwrite| DeltaBronzeUS
    end
```

### 💡 Architectural Decisions

| Decision | Engineering Rationale |
| :--- | :--- |
| **Delta Lake (ACID Transactions)** | Ensures atomic writes. If ingestion fails mid-way, the Data Lake remains 100% consistent without data corruption. |
| **Schema Enforcement (`StructType`)** | Strict data contract on entry. Rejects incompatible data types before writing to disk. |
| **Partitioning by `ticker_code`** | *Partition Pruning*: Enables future queries to scan only targeted ticker directories instead of scanning the full dataset. |

---

## 🛠️ How to Run / Como Executar

```bash
# 1. Environment Setup
sudo apt update && sudo apt install -y openjdk-17-jdk
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Run Jupyter Notebook
jupyter notebook
```

---

*Desenvolvido por / Developed by **Ericles Fernandes Oliveira** · Engenharia de Dados* 🚀
