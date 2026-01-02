# 🚀 Lakeflow Clickstream Pipeline: Declarative Data Engineering

![Overall Project Architecture](images_and_videos/Overall%20project%20image.png)

![Databricks](https://img.shields.io/badge/Platform-Databricks-orange.svg)
![Spark](https://img.shields.io/badge/Framework-Apache_Spark-red.svg)
![Lakeflow](https://img.shields.io/badge/Engine-Lakeflow_Declarative-blue.svg)

---

## 📖 Executive Summary

A **production-style, real-time Lakeflow Spark Declarative Pipeline** for ingesting and processing high-velocity clickstream data on Azure Databricks.

This project intentionally applies **best practices and key features captured during Databricks certification**, translating them into a **declarative, serverless, and resilient pipeline** aligned with the **June 2025 GA Lakeflow syntax**.  

The design prioritises **clarity, correctness, cost efficiency, and operational simplicity**, shifting from procedural ETL toward a **desired-state data engineering model**.

---

## 🏗️ Architecture & Data Flow

The pipeline follows a **Medallion Lakehouse Architecture**, governed by **Unity Catalog** and deployed via **Databricks Asset Bundles (DABs)**.

### **Data Source**
- Python-based clickstream event producer
- Authenticated using **Azure Managed Identity**
- Emits **nested JSON events** into ADLS Gen2 for near-real-time ingestion

### **Lakehouse Layers**
- **Bronze (Ingestion)**  
  - Incremental ingestion using **Auto Loader**
  - Schema enforcement via `StructType`
  - `_rescued_data` column to isolate malformed records and schema drift without stopping the pipeline

- **Silver (Refinement)**  
  - Exactly-once processing using checkpointing and event-time watermarking
  - PII pseudonymisation using **SHA-256 hashing**
  - Deterministic state handling for late-arriving data

- **Gold (Analytics)**  
  - Analytics-ready **Star Schema**
  - **SCD Type 2** history tracking
  - **Materialized Views** for low-latency BI consumption

---

## 🧠 Lakeflow Declarative Engineering (GA June 2025)

The pipeline is implemented using the **Lakeflow Spark Declarative API** (`pyspark.pipelines as dp`), replacing procedural orchestration with explicit, intention-driven semantics.

- **Explicit dataset intent**
  - `@dp.table`
  - `@dp.materialized_view`
  - `@dp.temporary_view`

- **Declarative change handling**
  - `dp.apply_changes(stored_as_scd_type=1/2)` replaces hand-written `MERGE INTO` logic
  - Eliminates custom state, trigger, and checkpoint orchestration

- **Event-time semantics**
  - Watermarking declared at the data level using `dp.read_stream().withWatermark()`
  - Execution, retries, and recovery handled by the platform

This approach improves **readability, reviewability, and maintainability**, while reducing engineering time spent on orchestration and debugging.

---

## 🛡️ Resilience, Self-Healing & Cost Efficiency

The pipeline is designed to operate continuously with minimal manual intervention:

- **Serverless execution**
  - Fully managed compute with no cluster lifecycle management
  - Billed per execution time (not idle)
  - Automatically scales for bursty workloads

- **Schema & data quality isolation**
  - `_rescued_data` captures corrupted records
  - Declarative expectations (`@dp.expect`) log, quarantine, or drop low-quality data without halting valid processing

- **Automatic optimisation**
  - **Liquid Clustering** (`clusterByAuto=True`) dynamically optimises data layout
  - Eliminates manual partitioning and Z-Ordering

- **Fault tolerance**
  - Delta Lake ACID guarantees prevent partial writes
  - Checkpointing enables safe restarts and automatic backfill
  - Job-level retries handle transient failures

---

## 🔎 Observability & Governance

Databricks-native observability features provide fast diagnosis and controlled recovery:

- Pipeline run history with success/failure diagnostics
- Quality expectation and quarantine reports
- End-to-end lineage across tables and views
- Execution metrics for throughput and latency analysis

Access and governance are enforced via:
- **Unity Catalog**
- **Microsoft Entra ID group-based RBAC**
- Secret-free authentication using Managed Identity

---

## 🚢 Deployment & Automation

The project is deployed using **Databricks Asset Bundles (DABs)**:

- Environment-agnostic configuration
- Version-controlled infrastructure
- Hands-free deployment via **GitHub Actions**
- Clear separation of pipeline logic and orchestration

---

## 📂 Repository Structure

| Path | Description |
|------|------------|
| `databricks.yml` | DAB blueprint and environment configuration |
| `resources/` | Pipeline and job definitions (Infrastructure-as-Code) |
| `src/` | Lakeflow declarative transformations |
| `scripts/` | Managed-identity clickstream event producer |

---

## 🛠️ Skills Demonstrated

- **Databricks:** Lakeflow Declarative Pipelines, Asset Bundles, Unity Catalog, Delta Lake  
- **Azure:** ADLS Gen2, Managed Identity, Entra ID (RBAC), Serverless Compute  
- **Data Engineering:** Exactly-once processing, SCD Type 2, Star Schema, PII pseudonymisation  

---

### Closing Note

This project represents a deliberate effort to move from **certification knowledge to production-style execution**, applying declarative and platform-managed patterns to reduce complexity, cost, and operational risk.
