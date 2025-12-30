# Lakeflow Clickstream Pipeline: Declarative Data Engineering

[![Databricks](https://img.shields.io/badge/Platform-Databricks-orange.svg)](https://www.databricks.com/)
[![Spark](https://img.shields.io/badge/Framework-Apache_Spark_4.1-red.svg)](https://spark.apache.org/)
[![Lakeflow](https://img.shields.io/badge/Engine-Lakeflow_SDP-blue.svg)](https://www.databricks.com/product/lakeflow)

## Executive Summary
This repository contains a production-ready **Lakeflow Spark Declarative Pipeline (SDP)** designed to process real-time clickstream data. Moving beyond traditional procedural ETL, this project leverages **Declarative Engineering** to define the "desired state" of data, allowing the Spark 4.1 engine to autonomously handle orchestration, state management, and performance optimization.

---

## Technical Architecture
The project follows a **Medallion (Multi-Hop) Architecture**, ensuring data quality and governance at every stage:

* **Bronze (Ingestion):** Raw JSON events are ingested from Azure Data Lake Storage (ADLS) using **Auto Loader**. Malformed data is caught via a `_rescued_data` column for schema evolution.
* **Silver (Refinement):** Data is standardized, timestamps are normalized, and PII (Emails/User IDs) are hashed using **SHA-256** for GDPR/CCPA compliance. **Watermarking** is applied to manage state for late-arriving streaming data.
* **Gold (Analytics):** High-performance tables optimized with **Liquid Clustering** and **SCD Type 2** tracking for historical user behavior analysis.



---

## Key Features & 2025 Standards

### 1. Declarative Engineering (`@dp` API)
By using the **Spark 4.1 `pyspark.pipelines`** library, the code focuses on *what* the data should look like rather than *how* to move it. 
* **Automatic Dependency Tracking:** The engine builds a DAG to ensure tables update in the correct order.
* **Managed State:** Checkpoints and triggers are handled by the platform, eliminating manual "Toil."

### 2. Autonomous Optimization
* **Auto Liquid Clustering:** Implemented `cluster_by_auto=True` on the Gold Layer, allowing the Databricks Intelligence Engine to physically reorganize data based on real-time query patterns.
* **Serverless Compute:** The pipeline runs on serverless infrastructure, scaling to zero when idle to minimize cloud costs.

### 3. Infrastructure as Code (DABs)
The entire environment is managed via **Databricks Asset Bundles (DABs)**, allowing for environment-agnostic deployments (Dev vs. Prod) using YAML configuration.

---

## Repository Structure

| File / Folder | Description |
| :--- | :--- |
| `databricks.yml` | **Project Blueprint:** Defines variables, workspace targets, and cloud storage paths. |
| `resources/` | **Resource Definitions:** Configures the Lakeflow Pipeline, compute settings, and library links. |
| `src/` | **Source Code:** Core PySpark logic utilizing `@dp` decorators and quality expectations. |
| `.github/workflows/` | **CI/CD:** Automated deployment pipelines via GitHub Actions. |

---

## Data Governance & RBAC
This project implements a strict security model using **Unity Catalog** and **Entra ID Groups** to ensure the Principle of Least Privilege.

| Principal | Level | Permission | Reason |
| :--- | :--- | :--- | :--- |
| `databricks-project-admins` | Catalog | **OWNER** | Full administrative control over the data lifecycle. |
| `databricks-project-engineers`| Schema | **USE & SELECT**| Restricted access to the `clickstream_dlt` production schema only. |

---

## Getting Started

### Prerequisites
* Databricks CLI (v0.218+)
* Azure Databricks Workspace with Unity Catalog enabled.

### Steps
1.  **Clone & Authenticate:**
    ```bash
    git clone <your-repo-url>
    databricks auth login --host <workspace-url>
    ```
2.  **Validate & Deploy:**
    ```bash
    databricks bundle validate
    databricks bundle deploy --target dev
    ```
3.  **Run Pipeline:**
    ```bash
    databricks bundle run clickstream_pipeline
    ```

---

## Skills Demonstrated
* **Languages:** Python (PySpark), SQL, YAML.
* **Data Architecture:** Medallion Architecture, SCD Type 2, Fact/Dimension Modeling, Data Governance.
* **DevOps:** Infrastructure as Code (DABs), CI/CD (GitHub Actions).
* **Specialized:** Liquid Clustering, Auto Loader, Data Quality Expectations, Unity Catalog.

---
