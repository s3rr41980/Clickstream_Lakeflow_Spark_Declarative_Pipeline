--  ********************************************************************************
--  PROJECT: Clickstream Lakeflow Pipeline - Governance Layer
--  AUTHOR: Jack Goh
--  DATE: 2025-12-31
--  OBJECTIVE: Implementation of Unity Catalog RBAC (Role-Based Access Control)
--  ********************************************************************************

--  1. CATALOG INITIALIZATION
--  Creating the isolated environment for the clickstream project.
CREATE CATALOG IF NOT EXISTS databricks_clickstream_dev;

--  2. SCHEMA (DATABASE) ORGANIZATION
--  Creating the target schema for the DLT pipeline output.
USE CATALOG databricks_clickstream_dev;
CREATE SCHEMA IF NOT EXISTS clickstream_dlt
  COMMENT 'Target schema for the Medallion architecture (Bronze/Silver/Gold) tables.';

--  3. PERMISSION DELEGATION (Administrative Layer)
--  Granting full ownership to the project admin group synced from Microsoft Entra ID.
--  This ensures that the dev team can manage their own tables and pipelines.
GRANT ALL PRIVILEGES ON CATALOG databricks_clickstream_dev TO `databricks-project-admins`;

--  4. ANALYST ACCESS (Principle of Least Privilege)
--  In a production scenario, we grant restricted access to data consumers (BI/Analysts).
--  Analysts should only see the 'Gold' layer and 'Materialized Views'.
--  Note: Replace 'analyst_group' with your actual consumer group if applicable.
GRANT USE CATALOG ON CATALOG databricks_clickstream_dev TO `data_analysts`;
GRANT USE SCHEMA ON SCHEMA clickstream_dlt TO `data_analysts`;
GRANT SELECT ON SCHEMA clickstream_dlt TO `data_analysts`;

--  5. STORAGE GOVERNANCE: External Locations
--  Securing the raw ingestion landing zone.
--  This ensures only authorized identities can read from the Azure Data Lake storage.
--  Landing Zone: abfss://raw@azuredarabricksstorage.dfs.core.windows.net/bronze_events_landing/
GRANT READ FILES ON EXTERNAL LOCATION `landing_zone_location` TO `databricks-project-admins`;

--  6. AUDIT & LINEAGE CHECK
--  Verification command to ensure the security principal is correctly assigned.
SHOW GRANTS ON CATALOG databricks_clickstream_dev;