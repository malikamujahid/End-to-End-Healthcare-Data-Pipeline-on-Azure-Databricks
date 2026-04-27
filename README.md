# End-to-End-Healthcare-Data-Pipeline-on-Azure-Databricks
End-to-End Healthcare Data Pipeline on Azure Databricks
A production-grade data pipeline built on Azure Databricks implementing the Medallion Architecture (Bronze, Silver, Gold) on a real-world healthcare dataset. The pipeline covers the full journey from raw data ingestion through to business-ready analytical tables, with data quality validation, incremental loading, and Delta Lake optimisation at each layer.

**Architecture Overview**
The pipeline follows the Medallion Architecture pattern, a data design approach that organises data into three progressive layers, each serving a distinct purpose.

**Raw CSV (Azure Blob Storage)**
        |
        v
 **Bronze Layer**
   (Raw ingestion — immutable, traceable, hashed)
        |
        v
**Silver Layer**
   (Cleaned, validated, DQ-flagged, enriched)
        |
        v
**Gold Layer**
   (Aggregated, business-ready analytical tables)

**Dataset**
The pipeline is built on a healthcare dataset containing patient admission records including demographics, medical conditions, billing information, treatment details, and discharge data.
**Source format:** CSV
**Storage:** Azure Blob Storage
**Records:** 10,000+ patient admissions

**Bronze Layer**
The bronze layer ingests raw data exactly as it arrives with no transformations, no assumptions. Every record is preserved with full ingestion metadata appended at load time.

**What the bronze layer does:**
Reads raw CSV from Azure Blob Storage using a parameterised config
Appends ingestion metadata — _ingested_at, _ingest_date, _source_file, _batch_id
Generates a _record_hash by hashing the full row content — used for deduplication in silver
Writes to Delta Lake as a managed table bronze.healthcare_raw

The bronze layer is designed to be immutable. If something goes wrong downstream, the raw data is always there to reprocess from.

**Silver Layer**
The silver layer is where raw becomes trustworthy. Every transformation here has a deliberate reason.

**What the silver layer does:**
Removes salutations and suffixes from patient names using a custom PySpark UDF
Extracts structured FirstName, LastName, and MiddleName from a single name field
Standardises gender and blood type values
Deduplicates records using _record_hash to handle pipeline reruns safely
Casts all columns to correct data types — dates, decimals, integers
Derives length_of_stay_days from admission and discharge dates
Derives age_group demographic buckets for population-level analysis
Handles nulls with meaningful defaults rather than dropping records
Applies six individual data quality flags per record plus a composite dq_passed flag
Writes to Delta Lake partitioned by _ingest_date and medical_condition

Records that fail data quality checks are not dropped. They are flagged and remain in the silver table — visible and traceable for investigation. Only dq_passed = True records are carried into the gold layer.

**Gold Layer**
The gold layer does not store individual records. It stores answers.
Three purpose-built analytical tables are produced, each serving a different business domain. All gold tables use a Delta Lake **MERGE** pattern for incremental updates rather than full overwrites, and are optimised with **ZORDER** for efficient downstream querying.
**gold.condition_summary — Clinical Analytics**
Aggregated metrics per medical condition including total patients, average and maximum length of stay, average and total billing, most common admission type, and abnormal test rate as a percentage.
**gold.billing_summary — Financial Analytics**
Billing breakdown segmented by insurance provider, age group, and medical condition. Includes total billing, average billing, and min/max billing per segment.
**gold.patient_demographics — Population Analytics**
Demographic distribution of patients by age group, gender, and medical condition. Includes average age, average length of stay, average billing, and abnormal test rate per demographic segment.\

## Technology Stack

The pipeline is built entirely on the Microsoft Azure ecosystem.

- **Azure Databricks** : distributed compute, notebook environment, and Delta Lake engine
- **Delta Lake**: ACID-compliant storage layer supporting MERGE, OPTIMIZE, ZORDER, and time travel
- **PySpark** : distributed data transformation across all three pipeline layers
- **Azure Blob Storage** : raw and processed data storage across bronze, silver, and gold
- **Microsoft Azure** : cloud platform hosting the full pipeline end to end

**Article Series**

1/n Project overview and architecture
2/nBronze layer — raw ingestion and metadata
3/nSilver layer Part 1 — name cleaning and standardisation
4/nSilver layer Part 2 — type casting, derived features, and DQ flags

Medium: **https://medium.com/@malika9102003** 
LinkedIn: **https://www.linkedin.com/in/malikamujahid/**
