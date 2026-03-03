# type: ignore
# Databricks notebook source

# COMMAND ----------
# %md
# # Exercise 1.2a: Cluster Configuration, Magic Commands, and the Shuffle Benchmark
#
# This notebook demonstrates the runtime environment that powers every ENA pipeline job.
# On serverless Databricks, compute is managed automatically, but understanding Spark's
# configuration, especially `spark.sql.shuffle.partitions`, is critical for pipeline
# performance. A poorly tuned shuffle can make a Silver aggregation across five agency
# tables take 10x longer than necessary.
#
# **What this notebook demonstrates:**
# - Inspecting the active Spark version and configuration
# - Benchmarking `groupBy` shuffle performance at 200 vs 8 partitions
# - Using `%sql`, `%sh`, and `%fs` magic commands in a polyglot notebook
#
# **Dependencies:** `SETUP_RUN_ONCE.py` must have been run first.

# COMMAND ----------
BASE_PATH = "/Volumes/ena_dev/default/ena_platform"

# COMMAND ----------
# Setting and printing spark version and current shuffle partitions

spark_version = spark.version
shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")

print(f"Spark version: {spark_version}")
print(f"Shuffle partitions: {shuffle_partitions}")

# COMMAND ----------
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

spark_df = spark.range(5_000_000) # creates a Dataframe in memory on the fly 
                                  #  with five million rows and a single column called 'id'
shuffle_benchmark(200)
shuffle_benchmark(8)

# COMMAND ----------
# %sql

-- Showing catalog schemas using SQL
SHOW SCHEMAS IN ena_dev

# COMMAND ----------
# %sh
# Showing python version using bash

python --version

# COMMAND ----------
# %fs
# Listing the content of the landing zone path using Databricks file system

ls /Volumes/ena_dev/default/ena_platform/landing

# COMMAND ----------
# %md
# ## What I Learned
#
# **Shuffle partitions matter more than they seem:**
# With 200 partitions on a single-node serverless environment, Spark creates 200 tasks
# for a simple `groupBy`, most of which are nearly empty. At 8 partitions the overhead
# drops dramatically. In production with large clusters 200 is often too low; on
# single-node it is almost always too high.
#
# **`spark.conf.set()` changes config at runtime:**
# Configuration can be changed mid-notebook without restarting the session. This is
# useful for tuning experiments but dangerous in production pipelines where config should
# be set once at the top and never overridden mid-job.
#
# **Magic commands are cell-level, not line-level:**
# `%sql`, `%sh`, and `%fs` apply to the entire cell. A `%sql` cell cannot contain
# Python, and Python variables (like `BASE_PATH`) are not accessible inside it.
#
# **Gotchas:**
# - `col("id") % 100` is a Spark column expression; `"id" % 100` is a Python string
#   format operation and raises a TypeError.
# - `spark.sparkContext` is not available on Databricks serverless, therefore `spark.version`
#   and `spark.conf` are used directly instead.
#
# **Next exercise:** `02b_agency_summary.py`, parameterised notebook called via `%run`.
