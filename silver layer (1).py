# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Healthcare Data Pipeline
# MAGIC ## Medallion Architecture on Azure Databricks
# MAGIC
# MAGIC This notebook transforms raw bronze ingestion data into a clean, validated,
# MAGIC and enriched silver layer ready for gold layer aggregations.
# MAGIC
# MAGIC ### What this notebook covers
# MAGIC - Name standardisation, suffix/salutation removal, and name tokenisation
# MAGIC - Gender and blood type standardisation
# MAGIC - Data type casting and schema enforcement
# MAGIC - Derived clinical features (length of stay, age group)
# MAGIC - Null handling with meaningful defaults
# MAGIC - Data quality flags per record
# MAGIC - Delta Lake write to `silver.healthcare_clean`
# MAGIC
# MAGIC **Source Table :** `bronze.healthcare_raw`
# MAGIC **Target Table :** `silver.healthcare_clean`
# MAGIC **Format       :** Delta Lake
# MAGIC **Partitioned by:** `_ingest_date`, `medical_condition`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Load Configuration
# MAGIC
# MAGIC Loads the pipeline configuration from a centralised JSON config file
# MAGIC using the shared `utils_config` utility. Configuration includes storage
# MAGIC account details, container paths, and environment settings used
# MAGIC throughout the pipeline.

# COMMAND ----------

# DBTITLE 1,run utils_config
# MAGIC %run "/Workspace/Users/malika.mujahid@studentambassadors.com/utils_config"

# COMMAND ----------

# DBTITLE 1,exec config
cfg = load_config("/Workspace/Users/malika.mujahid@studentambassadors.com/config (1).json")   # or your actual config path
print(cfg["storage_account_name"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Name Standardisation & Hospital Cleaning
# MAGIC
# MAGIC Reads the raw bronze table and applies the following transformations:
# MAGIC
# MAGIC - **`initcap` + trim** on patient name for consistent display formatting
# MAGIC - **Special character removal** from hospital names to ensure clean joins downstream
# MAGIC - **Custom suffix/salutation UDF** strips titles and suffixes
# MAGIC   (Mr., Mrs., Ms., Dr., Jr., Sr., PhD etc.) from patient names,
# MAGIC   producing a clean `name_clean` field used throughout the pipeline
# MAGIC
# MAGIC The UDF normalises each token to uppercase before matching against
# MAGIC a predefined suffix set, making it robust to mixed-case inputs.

# COMMAND ----------

# DBTITLE 1,standardize columns+ clean suffix
from pyspark.sql.types import StringType
import pyspark.sql.functions as F

df = spark.table("bronze.healthcare_raw")

# Standardize display formatting (Silver)
df = (df
      .withColumn("name", F.initcap(F.trim(F.col("name"))))
      .withColumn("hospital", F.regexp_replace(F.col("hospital"), r"[^A-Za-z0-9 ]", ""))
)

def clean_suffix(name: str) -> str:
    if not name:
        return name

    suffix_set = {"JR","SR","III","II","PHD","MR","MRS","MS","DR","NIECE","SON","DAUGHTER"}

    tokens = name.strip().split()
    kept = []
    for t in tokens:
        normalized = t.strip(".,()").upper()
        if normalized in suffix_set:
            continue
        kept.append(t)
    return " ".join(kept).strip()

clean_suffix_udf = F.udf(clean_suffix, StringType())

df = df.withColumn("name_clean", clean_suffix_udf(F.col("name")))



# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Name Tokenisation & Demographic Standardisation
# MAGIC
# MAGIC Splits `name_clean` into structured name components and standardises
# MAGIC gender and blood type fields.
# MAGIC
# MAGIC **Name parsing:**
# MAGIC - `NameTokens` — array of name parts split on whitespace
# MAGIC - `FirstName` — first token
# MAGIC - `LastName` — last token
# MAGIC - `MiddleName` — any tokens between first and last (empty string if none)
# MAGIC
# MAGIC **Gender standardisation:**
# MAGIC Normalises raw gender values to consistent codes.
# MAGIC
# MAGIC | Raw Value | Standardised |
# MAGIC |-----------|-------------|
# MAGIC | M, Male   | M           |
# MAGIC | F, Female | F           |
# MAGIC | Null / blank / other | UN |
# MAGIC
# MAGIC **Blood type standardisation:**
# MAGIC Uppercases and trims blood type values. Nulls and blanks → `UN`.

# COMMAND ----------

# DBTITLE 1,cleansing : name firstname + lastname
import pyspark.sql.functions as F

# 1) Split cleaned name into tokens (handles multiple spaces)
df = df.withColumn("NameTokens", F.split(F.trim(F.col("name_clean")), r"\s+"))

# 2) First / Last / Middle name
df = df.withColumn(
    "FirstName",
    F.when(F.size("NameTokens") >= 1, F.col("NameTokens")[0])
)

df = df.withColumn(
    "LastName",
    F.when(F.size("NameTokens") >= 2, F.element_at(F.col("NameTokens"), -1))
)

df = df.withColumn(
    "MiddleName",
    F.when(
        F.size("NameTokens") >= 3,
        F.concat_ws(" ", F.expr("slice(NameTokens, 2, size(NameTokens)-2)"))
    ).otherwise(F.lit(""))
)

# 3) Gender mapping (robust)
g = F.upper(F.trim(F.col("gender")))
df = df.withColumn(
    "gender",
    F.when(F.col("gender").isNull() | (F.trim(F.col("gender")) == ""), F.lit("UN"))
     .when(g.isin("M", "MALE"), F.lit("M"))
     .when(g.isin("F", "FEMALE"), F.lit("F"))
     .otherwise(F.lit("UN"))
)

# 4) Blood type standardization
bt = F.upper(F.trim(F.col("blood_type")))
df = df.withColumn(
    "blood_type",
    F.when(F.col("blood_type").isNull() | (F.trim(F.col("blood_type")) == ""), F.lit("UN"))
     .otherwise(bt)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Write Initial Silver Table
# MAGIC
# MAGIC Creates the `silver` database if it does not exist and writes the
# MAGIC current dataframe as a managed Delta table `silver.healthcare_clean`.
# MAGIC
# MAGIC > **Note:** Additional transformations (type casting, derived features,
# MAGIC > DQ flags) are applied in subsequent cells and the table is overwritten
# MAGIC > with the complete schema in the final write step.

# COMMAND ----------

# DBTITLE 1,create silver layer
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
(df.write
 .format("delta")
 .mode("overwrite")              # use overwrite while developing
 .saveAsTable("silver.healthcare_clean")
)
spark.sql("SELECT name,gender, blood_type FROM silver.healthcare_clean").show() 

# COMMAND ----------

# MAGIC %md
# MAGIC spark.sql("""
# MAGIC SELECT *
# MAGIC FROM silver.healthcare_clean
# MAGIC """).show()
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Deduplication
# MAGIC
# MAGIC Removes duplicate records using the `_record_hash` column generated
# MAGIC during bronze ingestion. This ensures re-ingestion of the same source
# MAGIC file does not produce duplicate records in the silver table.

# COMMAND ----------

# DBTITLE 1,Deduplicate data
# DBTITLE 1,deduplication on record hash
before = df.count()
df = df.dropDuplicates(["_record_hash"])
after = df.count()

print(f"Records before deduplication : {before:,}")
print(f"Records after deduplication  : {after:,}")
print(f"Duplicates removed           : {before - after:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Data Type Casting
# MAGIC
# MAGIC Bronze ingests all fields as strings to preserve raw fidelity.
# MAGIC Silver enforces the correct schema by casting each column to its
# MAGIC intended data type.
# MAGIC
# MAGIC | Column             | Bronze Type | Silver Type    |
# MAGIC |--------------------|-------------|----------------|
# MAGIC | age                | string      | integer        |
# MAGIC | billing_amount     | string      | decimal(18,2)  |
# MAGIC | room_number        | string      | integer        |
# MAGIC | date_of_admission  | string      | date           |
# MAGIC | discharge_date     | string      | date           |

# COMMAND ----------

# DBTITLE 1,Data typecasting
# DBTITLE 1,type casting
from pyspark.sql.types import IntegerType, DecimalType

df = (df
      .withColumn("age",
                  F.col("age").cast(IntegerType()))
      .withColumn("billing_amount",
                  F.col("billing_amount").cast(DecimalType(18, 2)))
      .withColumn("room_number",
                  F.col("room_number").cast(IntegerType()))
      .withColumn("date_of_admission",
                  F.to_date(F.col("date_of_admission"), "yyyy-MM-dd"))
      .withColumn("discharge_date",
                  F.to_date(F.col("discharge_date"), "yyyy-MM-dd"))
)

print(" Type casting applied")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Derived Clinical Features
# MAGIC
# MAGIC Derives two analytically valuable columns from existing fields.
# MAGIC
# MAGIC **`length_of_stay_days`**
# MAGIC The number of days between admission and discharge. A core metric
# MAGIC in hospital performance, resource planning, and billing analysis.
# MAGIC
# MAGIC **`age_group`**
# MAGIC Demographic bucketing of patient age for population-level analysis
# MAGIC in the gold layer.
# MAGIC
# MAGIC | Age Range | Group       |
# MAGIC |-----------|-------------|
# MAGIC | 0 – 17    | Paediatric  |
# MAGIC | 18 – 34   | Young Adult |
# MAGIC | 35 – 59   | Middle Aged |
# MAGIC | 60+       | Senior      |

# COMMAND ----------

# DBTITLE 1,Derived columns from existing data
# DBTITLE 1,derived features: length of stay + age group
df = df.withColumn(
    "length_of_stay_days",
    F.datediff(F.col("discharge_date"), F.col("date_of_admission"))
)

df = df.withColumn(
    "age_group",
    F.when(F.col("age") < 18,  "Paediatric")
     .when(F.col("age") < 35,  "Young Adult")
     .when(F.col("age") < 60,  "Middle Aged")
     .otherwise("Senior")
)

print("Derived features added")
df.select(
    "age", "age_group",
    "date_of_admission", "discharge_date", "length_of_stay_days"
).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Null Handling
# MAGIC
# MAGIC Replaces null values with meaningful defaults rather than dropping
# MAGIC records. Preserving records at the silver layer maintains data
# MAGIC completeness — nulls are independently flagged in the DQ step
# MAGIC so downstream consumers can filter as needed.
# MAGIC
# MAGIC | Column             | Default Value    |
# MAGIC |--------------------|------------------|
# MAGIC | medication         | `Not Prescribed` |
# MAGIC | test_results       | `Pending`        |
# MAGIC | insurance_provider | `Unknown`        |
# MAGIC | MiddleName         | *(empty string)* |

# COMMAND ----------

# DBTITLE 1,Handle nulls
# DBTITLE 1,null handling
df = (df
      .withColumn("medication",
                  F.coalesce(F.col("medication"), F.lit("Not Prescribed")))
      .withColumn("test_results",
                  F.coalesce(F.col("test_results"), F.lit("Pending")))
      .withColumn("insurance_provider",
                  F.coalesce(F.col("insurance_provider"), F.lit("Unknown")))
      .withColumn("MiddleName",
                  F.coalesce(F.col("MiddleName"), F.lit("")))
)

print("Null handling applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Data Quality Flags
# MAGIC
# MAGIC Applies validation rules to each record, producing individual boolean
# MAGIC flag columns and a composite `dq_passed` column.
# MAGIC
# MAGIC Records that fail DQ are **not dropped** — they are flagged and
# MAGIC passed through so that data quality issues remain visible and
# MAGIC traceable for gold layer consumers.
# MAGIC
# MAGIC | Flag                      | Rule                                          |
# MAGIC |---------------------------|-----------------------------------------------|
# MAGIC | `dq_age_valid`            | Age between 0 and 120                         |
# MAGIC | `dq_billing_valid`        | Billing amount greater than 0                 |
# MAGIC | `dq_dates_valid`          | Discharge date on or after admission date     |
# MAGIC | `dq_length_of_stay_valid` | Length of stay between 0 and 365 days         |
# MAGIC | `dq_name_valid`           | Cleaned name is non-null and length > 1       |
# MAGIC | `dq_passed`               | All individual flags pass                     |

# COMMAND ----------

# DBTITLE 1,Data Quality Checks (DQ)
# DBTITLE 1,data quality flags
df = (df
      .withColumn("dq_age_valid",
                  F.col("age").between(0, 120))
      .withColumn("dq_billing_valid",
                  F.col("billing_amount") > 0)
      .withColumn("dq_dates_valid",
                  F.col("discharge_date") >= F.col("date_of_admission"))
      .withColumn("dq_length_of_stay_valid",
                  F.col("length_of_stay_days").between(0, 365))
      .withColumn("dq_name_valid",
                  F.col("name_clean").isNotNull() &
                  (F.length(F.col("name_clean")) > 1))
      .withColumn("dq_passed",
                  F.col("dq_age_valid") &
                  F.col("dq_billing_valid") &
                  F.col("dq_dates_valid") &
                  F.col("dq_length_of_stay_valid") &
                  F.col("dq_name_valid"))
)

# DQ Summary
total  = df.count()
passed = df.filter(F.col("dq_passed") == True).count()
failed = total - passed

print(f"{'─' * 45}")
print(f"  Total records   : {total:,}")
print(f"  DQ Passed       : {passed:,}  ({round(passed/total*100,1)}%)")
print(f"  DQ Failed       : {failed:,}  ({round(failed/total*100,1)}%)")
print(f"{'─' * 45}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Overwrite Silver Table with Complete Schema
# MAGIC
# MAGIC Overwrites the silver Delta table with the fully enriched and validated
# MAGIC dataset. The table is partitioned by `_ingest_date` and `medical_condition`
# MAGIC to enable efficient predicate pushdown in gold layer queries.

# COMMAND ----------

# DBTITLE 1,Overwrite Data
# DBTITLE 1,overwrite silver with complete schema
(df.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .partitionBy("_ingest_date", "medical_condition")
   .saveAsTable("silver.healthcare_clean")
)

print("Silver table overwritten with complete schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 — Validation
# MAGIC
# MAGIC Reads back from the silver table to confirm the write was successful
# MAGIC and spot-checks key derived and DQ columns.

# COMMAND ----------

# DBTITLE 1,Drop Irrelevant Columns
# DBTITLE 1, drop intermediate columns
df = df.drop("NameTokens")

# DBTITLE 1,overwrite silver with complete schema
(df.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .partitionBy("_ingest_date", "medical_condition")
   .saveAsTable("silver.healthcare_clean")
)

print("Silver table overwritten with complete schema")

# COMMAND ----------

# DBTITLE 1,Data Validation
# MAGIC %sql
# MAGIC -- DBTITLE 1, view silver table
# MAGIC SELECT * FROM silver.healthcare_clean LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.healthcare_clean
# MAGIC LIMIT 10