# Databricks notebook source
# MAGIC %md
# MAGIC ## Exercise 2.1: DataFrame API nd Schema Enforcement
# MAGIC
# MAGIC Before any ENA agency data touches Delta Lake, it must pass throughta schema contract. This notebook demonstrates how to define explicit `StructType` schemas for two agency feeds (Skatteverket CSV and Arbetsformedlingen JSON), read them with schema enforcement, apply column-level transformations, and understand what happens when a row violates the schema.
# MAGIC
# MAGIC **What this notebook demonstrates:**
# MAGIC - Defining `StructType` schemas which will be the foundation of all Bronze reads
# MAGIC - `PERMISSIVE` vs `FAILFAST` read modes
# MAGIC - Core DataFrame operations: `withColumn()`, `withColumnRenamed()`, `select()`, `cast()`
# MAGIC
# MAGIC **Dependencies:** `SETUP_RUN_ONCE.py` must have been run first.

# COMMAND ----------

BASE_PATH = "/Volumes/ena_dev/default/ena_platform"
spark.conf.set("spark.sql.shuffle.partitions", 8)

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DateType, DoubleType
)

skatteverket_schema = StructType([
    StructField("municipality_code",    IntegerType(), nullable=False),
    StructField("report_date",          DateType(), nullable=False),
    StructField("tax_compliance_rate",  DoubleType(), nullable=True),
    StructField("registered_taxpayers", IntegerType(), nullable=True),
    StructField("late_filings",         IntegerType(), nullable=True),
])

# COMMAND ----------

arbetsformedlingen_schema = StructType([
    StructField("municipality_code", IntegerType(), nullable=False),
    StructField("report_date",       DateType(), nullable=False),
    StructField("job_seekers",       IntegerType(), nullable=True),
    StructField("placements",        IntegerType(), nullable=True),
    StructField("placement_rate",    DoubleType(), nullable=True),
])

# COMMAND ----------

skatteverket_path = f"{BASE_PATH}/landing/skatteverket"

df_skatteverket = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(skatteverket_schema)
    .load(skatteverket_path)
)

df_skatteverket.printSchema()
df_skatteverket.show(5, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, round as spark_round

df_transformed = (
    df_skatteverket
    .withColumn(
        "compliance_gap",
        spark_round(100.0 - col("tax_compliance_rate"), 2)
    )
    .withColumnRenamed("municipality_code", "kommun_kod")
    .select("kommun_kod", "report_date", "tax_compliance_rate", "compliance_gap")
)

df_transformed.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Cell 7
# Writing a bad row on purpose. Value of municipality_code is given as "abc123" instead of an integer
bad_csv_path = f"{BASE_PATH}/landing/skatteverket/bad_test.csv"
dbutils.fs.put(
    bad_csv_path,
    "municipality_code,report_date,tax_compliance_rate,registered_taxpayers,late_filings\n"
    "INVALID,2026-01-01,92.5,1500,30\n",
    overwrite=True
)

# Ensure file exists before attempting to read
print(dbutils.fs.head(bad_csv_path))

try:
    df_bad_csv = spark.read.format("csv") \
        .option("header", True) \
        .option("mode", "FAILFAST") \
        .schema(skatteverket_schema) \
        .load(bad_csv_path) \
        .collect() # triggers the read
except Exception as e:
    print(f"FAILFAST caught a schema violation:\n{e.args}")
finally:
    dbutils.fs.rm(bad_csv_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Cover:
# MAGIC - **`StructType` is the contract:** changing a field type here without updating the downstream Silver cast is the most common source of pipeline breakage on the ENA platform.
# MAGIC - **`PERMISSIVE` for production ingestion, `FAILFAST` for CI:** production needs to keep processing; CI needs to fail loudly.
# MAGIC - **`nullable=False` is advisory, not enforced at read time:** Spark won't reject a null in a nullable=False field during a CSV read, so the enforcement happens at Delta write via `NOT NULL` constraints.
# MAGIC - **Next:** 2.2 will use these DataFrames as inputs for joins and window functions.
