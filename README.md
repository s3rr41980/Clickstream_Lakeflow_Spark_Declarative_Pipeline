# 🚀 Lakeflow Clickstream Pipeline: Declarative Data Engineering

![Databricks](https://img.shields.io/badge/Platform-Databricks-orange.svg)
![Spark](https://img.shields.io/badge/Framework-Apache_Spark_4.1-red.svg)
![Lakeflow](https://img.shields.io/badge/Engine-Lakeflow_SDP-blue.svg)

## 📖 Executive Summary
A production-grade **Lakeflow Spark Declarative Pipeline (SDP)** designed to ingest and process high-velocity clickstream data. This project implements the latest **2025 Databricks Standards**, moving away from procedural ETL toward a "Desired State" architecture. It automates state management, schema resilience, and performance tuning using the Databricks Intelligence Engine.

---

## 🏗️ Technical Architecture & Data Modeling
The project utilizes a **Medallion Architecture** governed by **Unity Catalog**:

* **Source:** Python-based producer using **Managed Identity** to stream nested JSON to ADLS Gen2.
* **Bronze (Ingestion):** Schema enforcement via `StructType` and **Auto Loader**. Includes a `_rescued_data` column to ensure corrupted records don't halt the pipeline.
* **Silver (Refinement):** **Exactly-once semantics** with event-time watermarking. PII is pseudonymized using **SHA-256 hashing** for GDPR/CCPA compliance.
* **Gold (Analytics):** Optimized Star Schema with **SCD Type 2** history tracking and Materialized Views for low-latency BI consumption.



---

## 🌟 Modern Standards (Lakeflow SDP)
Upgraded to the **June 2025 GA Lakeflow Syntax** (`pyspark.pipelines` as `dp`), providing clearer semantics and reduced implementation risk:
* **Declarative Logic:** Uses `@dp.table` and `@dp.materialized_view` to define state, replacing complex manual `MERGE INTO` or `INSERT` logic.
* **Managed History:** `dp.apply_changes()` handles SCD Type 2 logic, ensuring deterministic state management and safe rollbacks.
* **Event-Time Processing:** Integrated `withWatermark()` for late-arriving data reconciliation.

---

## 🛡️ Self-Healing & Operational Resiliency
This pipeline is designed to be "Hands-Free" and resilient to real-world data issues:

* **Serverless DLT:** Trading higher unit costs for **lower TCO**. Benefits from zero-infra overhead and sub-second scaling, typically resulting in **26% better TCO** due to reduced idle time and faster results.
* **Predictive Optimization:** Leverages **Photon** and **Liquid Clustering** (`cluster_by_auto=True`). This eliminates manual Z-Ordering and partitioning by dynamically optimizing data layout based on query patterns.
* **Reliability:** ACID transactions via Delta Lake prevent ghost records or partial writes. Built-in **Checkpointing** ensures the pipeline knows exactly which files are processed, enabling automatic backfilling.
* **Observability:** Integrated lineage, success/failure reporting, and a dedicated **Quarantine Report** for failed quality expectations.

---

## 📂 Repository Structure

| Folder/File | Purpose |
| :--- | :--- |
| `databricks.yml` | **DAB Blueprint:** Environment-agnostic config (Variables, Targets). |
| `resources/` | **Infrastructure-as-Code:** Serverless compute and pipeline definitions. |
| `src/` | **Source logic:** Spark SDP transformations and UC Governance SQL. |
| `scripts/` | **Producer:** Managed-identity authenticated clickstream generator. |

---

## 🛠️ Skills & Certifications
* **Databricks:** Asset Bundles (DABs), Delta Live Tables, Unity Catalog, Liquid Clustering.
* **Azure:** ADLS Gen2, Managed Identity, Entra ID (RBAC), Secret Management.
* **Data Science/Eng:** PII Pseudonymization, Star Schema Modeling, SCD Type 2.

---