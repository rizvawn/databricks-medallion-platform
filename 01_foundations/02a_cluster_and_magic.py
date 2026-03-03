# type: ignore
# Databricks notebook source
# MAGIC %md
# MAGIC **Exercise 1.2a:** Cluster Configuration, Magic Commands, and the Shuffle Benchmark
# MAGIC This notebook demonstrates the runtime environment that powers every ENA pipeline job.
# MAGIC On serverless Databricks, compute is managed automatically, but understanding Spark's
# MAGIC configuration, especially `spark.sql.shuffle.partitions`, is critical for pipeline
# MAGIC performance. A poorly tuned shuffle can make a Silver aggregation across five agency
# MAGIC tables take 10x longer than necessary.
# MAGIC
# MAGIC **What this notebook demonstrates:**
# MAGIC - Inspecting the active Spark version and configuration
# MAGIC - Benchmarking `groupBy` shuffle performance at 200 vs 8 partitions
# MAGIC - Using `%sql`, `%sh`, and `%fs` magic commands in a polyglot notebook
# MAGIC
# MAGIC **Dependencies:** `SETUP_RUN_ONCE.py` must have been run first.
# MAGIC

# COMMAND ----------

BASE_PATH = "/Volumes/ena_dev/default/ena_platform"

# Setting and printing spark version and current shuffle partitions
spark_version = spark.version
shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")

print(f"Spark version: {spark_version}")
print(f"Shuffle partitions: {shuffle_partitions}")

# Printing the time difference between groupBy operations, once with shuffle partitions at 200
# and then at 8
def shuffle_benchmark(partitions):
    import time
    from pyspark.sql.functions import col

    spark.conf.set("spark.sql.shuffle.partitions", partitions)
    start = time.time()
    result = spark_df.groupBy(col("id") % 100).count()
    _ = result.collect()
    elapsed = time.time() - start
    print(f"***Shuffle benchmark*** \npartitions: {partitions} \ntime elapsed: {elapsed}\n")

# creates a Dataframe in memory on the fly with five million rows and a single column called 'id'
spark_df = spark.range(5_000_000)
shuffle_benchmark(200)
shuffle_benchmark(8)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Showing catalog schemas using SQL
# MAGIC SHOW SCHEMAS IN ena_dev

# COMMAND ----------

# MAGIC %sh
# MAGIC # Showing python version using bash
# MAGIC python --version

# COMMAND ----------

# MAGIC %md
# MAGIC Listing the content of the landing zone path using Databricks file system

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Volumes/ena_dev/default/ena_platform/landing

# COMMAND ----------

# MAGIC %md
# MAGIC ## What I Learned
# MAGIC
# MAGIC **Shuffle partitions matter more than they seem:**
# MAGIC With 200 partitions on a single-node serverless environment, Spark creates 200 tasks
# MAGIC for a simple `groupBy`, most of which are nearly empty. At 8 partitions the overhead
# MAGIC drops dramatically. In production with large clusters 200 is often too low; on
# MAGIC single-node it is almost always too high.
# MAGIC
# MAGIC **`spark.conf.set()` changes config at runtime:**
# MAGIC Configuration can be changed mid-notebook without restarting the session. This is
# MAGIC useful for tuning experiments but dangerous in production pipelines where config should
# MAGIC be set once at the top and never overridden mid-job.
# MAGIC
# MAGIC **Magic commands are cell-level, not line-level:**
# MAGIC `%sql`, `%sh`, and `%fs` apply to the entire cell. A `%sql` cell cannot contain
# MAGIC Python, and Python variables (like `BASE_PATH`) are not accessible inside it.
# MAGIC
# MAGIC **Gotchas:**
# MAGIC - `col("id") % 100` is a Spark column expression; `"id" % 100` is a Python string
# MAGIC format operation and raises a TypeError.
# MAGIC - `spark.sparkContext` is not available on Databricks serverless, therefore `spark.version`
# MAGIC and `spark.conf` are used directly instead.
# MAGIC
# MAGIC **Next exercise:** `02b_agency_summary.py`, parameterised notebook called via `%run`.
