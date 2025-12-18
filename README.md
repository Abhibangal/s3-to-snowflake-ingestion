# S3 to Snowflake Ingestion Framework

A **Snowflake-native ingestion framework** to load data from **AWS S3 into Snowflake** using:

- External Stages
- `COPY INTO` (full pushdown)
- Schema inference & schema evolution
- Table-driven configuration
- Snowflake Stored Procedures
- Git-based deployment

This framework is designed for **enterprise-grade ingestion**, supporting **multiple data sources**, **idempotent re-runs**, and **ad-hoc loads**.

---

## 🚀 Key Features

- Fully **Snowflake-native** (no data pulled into Python)
- Supports **CSV, JSON**, and other structured formats
- Automatic **table creation** using `INFER_SCHEMA`
- **Schema evolution enabled** on raw tables
- **Idempotent ingestion** (already-loaded files are skipped)
- **Ad-hoc ingestion** support
- **Query tagging** for lineage & observability
- Git-based **CI/CD friendly**
- Single **reusable stored procedure**

---

## 🏗 Architecture Overview

AWS S3
|
| (External Stage)
v
Snowflake
├── CONFIG_SCH
│ ├── INGESTION_SOURCE_ENV
│ ├── INGESTION_DATASET_CONFIG
│ └── INGESTION_ADHOC_CONFIG
|
├── RAW TABLES (Schema Evolution Enabled)
|
└── Python Stored Procedure (Snowpark)


---

## 📂 Repository Structure

.
├── ingestion/
│ └── aws_s3_snf_ingestion.py # Core ingestion logic
│
├── sql/
│ ├── ingestion_source_env.sql
│ ├── ingestion_dataset_config.sql
│ └── ingestion_adhoc_config.sql
│
├── sp/
│ └── run_s3_to_snowflake_ingestion.sql
│
└── README.md


---

## 🧩 Configuration Tables

### 1️⃣ INGESTION_SOURCE_ENV
Defines **environment-specific configuration** per data source.

| Column | Description |
|------|------------|
| SOURCE_NAME | Logical data source (e.g. `aact`) |
| TARGET_DATABASE | Target database |
| TARGET_SCHEMA | Target schema |
| STAGE_NAME | Snowflake external stage |
| CURRENT_ENV_FLAG | Active environment |

---

### 2️⃣ INGESTION_DATASET_CONFIG
Defines **dataset-level ingestion rules**.

| Column | Description |
|------|------------|
| DATA_SOURCE | Source name |
| DATASET_NAME | Logical dataset |
| TABLE_NAME | Target table |
| FILE_NAME | File name |
| S3_PATH_TEMPLATE | S3 path template |
| FILE_TYPE | CSV / JSON |
| FILE_FORMAT_OBJECT | Snowflake file format |
| COPY_OPTIONS | COPY options (VARIANT) |
| QUERY_TAG | Query tag (VARIANT) |

---

### 3️⃣ INGESTION_ADHOC_CONFIG
Used for **one-time or ad-hoc ingestion**.

| Column | Description |
|------|------------|
| ADHOC_ID | Unique adhoc identifier |
| DATA_SOURCE | Source name |
| TABLE_NAME | Target table |
| FILE_NAME | File name |
| S3_PATH | Full S3 path |
| STATUS | PENDING / COMPLETED / FAILED |

---

## ▶️ How to Run Ingestion

### Normal ingestion (all datasets for a source)
```sql
CALL RUN_S3_TO_SNOWFLAKE_INGESTION('aact', NULL);
```
Ad-hoc ingestion
```
CALL RUN_S3_TO_SNOWFLAKE_INGESTION('aact', 'ADHOC_001');
```
🔄 COPY Result Handling

| Scenario     | Behavior                    |
| ------------ | --------------------------- |
| First load   | Data loaded                 |
| Re-run       | Skipped (already loaded)    |
| Force reload | Controlled via COPY options |
| Invalid file | Marked as failed            |

Snowflake status
Copy executed with 0 files processed.
is correctly handled as already loaded.

Table Creation Logic

CSV / structured files

Uses INFER_SCHEMA

CREATE TABLE USING TEMPLATE

JSON files

Single VARIANT column

All tables

ENABLE_SCHEMA_EVOLUTION = TRUE

🏷 Query Tagging

Query tags are applied dynamically per dataset
```{
  "app": "data_ingestion",
  "source": "aact",
  "dataset": "browse_conditions"
}```
