# Databricks notebook source
# MAGIC %md
# MAGIC ### Exercise 1.2b: Parameterized Agency Landing Zone Summary
# MAGIC
# MAGIC This notebook demonstrates Databricks widget parameterisation which is the pattern used by every ENA Workflow task to pass an agency name at runtime. It accepts an `agency_name` widget, validates it against the five known ENA agencies, lists the files present in that agency's landing zone, and exits with a structured result string readable in Workflow run history.
# MAGIC
# MAGIC **What this notebook demonstrated:""
# MAGIC - Declaring and reading a `dbutils.widget.text()` parameter
# MAGIC - Validating widget input before touching the file system
# MAGIC - Building paths from `BASE_PATH` without hardcoding.
# MAGIC - Returning a result with `dbutils.notebook.exit()`
# MAGIC

# COMMAND ----------

BASE_PATH = "/Volumes/ena_dev/default/ena_platform"

dbutils.widgets.text("agency_name", "skatteverket")
agency = dbutils.widgets.get("agency_name")

print(f"Running summary for agency: {agency}")

# COMMAND ----------

KNOWN_AGENCIES = [
    "skatteverket",
    "arbetsformedlingen",
    "forsakringskassan",
    "socialstyrelsen",
    "scb",
]

if agency not in KNOWN_AGENCIES:
    raise ValueError(f"Unknown agency: '{agency}'. Must be one of {KNOWN_AGENCIES}")

# COMMAND ----------

landing_path = f"{BASE_PATH}/landing/{agency}"
files = dbutils.fs.ls(landing_path)
file_count = len(files)

print(f"Landing zone: {landing_path}")
print(f"Files found: {file_count}\n")

for f in files:
    print(f" {f.name} ({f.size} bytes)")

# COMMAND ----------

dbutils.notebook.exit(f"agency={agency} | files={file_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Widget lifecycle:** widgets persist for the session; call `dbutils.widgets.removeAll()` at the end of exploratory sessions to avoid stale values bleeding into re-runs.
# MAGIC **%run vs dbutils.notebook.run():** %run runs in the same session (shared SparkContext, shared variables); `dbutils.notebook.run()` spawns an *isolated* job, so use the latter for parallelism or isolation in production Workflows.
# MAGIC **Gotcha:** `dbutils.notebook.exit()` only works as a return signal for `dbutils.notebook.run()`. When running interactively, it just stops execution without causing any error.
