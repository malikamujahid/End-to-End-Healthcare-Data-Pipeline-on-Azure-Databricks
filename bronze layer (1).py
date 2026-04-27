# Databricks notebook source
# DBTITLE 1,to run the notebook for secret credentials
# MAGIC %run "/Workspace/Users/malika.mujahid@studentambassadors.com/utils_config"

# COMMAND ----------

# DBTITLE 1,Load config file
cfg = load_config("/Workspace/Users/malika.mujahid@studentambassadors.com/config (1).json")   # or your actual config path
print(cfg["storage_account_name"])

# COMMAND ----------

# DBTITLE 1,bronze layer with spark
cfg = load_config("/Workspace/Users/malika.mujahid@studentambassadors.com/config (1).json")
from pyspark.sql.functions import (
    current_timestamp, lit, sha2, concat_ws,
    coalesce, col, to_date
)
import uuid
import re

spark.conf.set(
    f"fs.azure.account.key.{cfg['storage_account_name']}.blob.core.windows.net",
    cfg["storage_account_key"]
)

data_path = f"wasbs://{cfg['container_name']}@{cfg['storage_account_name']}.blob.core.windows.net/{cfg['blob_name']}"

df = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(data_path)
)

def clean_col(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"\s+", "_", name)          # spaces -> underscores
    name = re.sub(r"[^a-z0-9_]", "", name)    # remove special characters
    return name

df = df.select([col(c).alias(clean_col(c)) for c in df.columns])
batch_id = str(uuid.uuid4())
data_cols = [c for c in df.columns if not c.startswith("_metadata")]  # safety

hash_expr = concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in data_cols])

df_bronze = (df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_ingest_date", to_date(col("_ingested_at")))
    .withColumn("_source_file", col("_metadata.file_path")) 
    .withColumn("_batch_id", lit(batch_id))
    .withColumn("_record_hash", sha2(hash_expr, 256))
)

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

(df_bronze.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("bronze.healthcare_raw")
)

spark.sql("SELECT count(*) FROM bronze.healthcare_raw").show()


# COMMAND ----------

# DBTITLE 1,check delta table
# MAGIC %sql
# MAGIC DESCRIBE DETAIL bronze.healthcare_raw;
# MAGIC
# MAGIC SELECT  
# MAGIC  gender, name
# MAGIC FROM bronze.healthcare_raw
# MAGIC WHERE name LIKE '%Bobby%';

# COMMAND ----------

