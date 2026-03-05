# Databricks notebook source
# MAGIC %md
# MAGIC ### Exercise 1.2c: Calling a Parameterised Notebook
# MAGIC Demonstrates two ways to invoke `02b_agency_summary` with a runtime parameter:
# MAGIC - `%run`: same session, synchronous, for interactive chaining
# MAGIC - `dbutils.notebook.run()`: isolated execution, returns a result string

# COMMAND ----------

# MAGIC %run ./02b_agency_summary.py $agency_name="arbetsformedlingen"

# COMMAND ----------

result = dbutils.notebook.run(
    "./02b_agency_summary.py",
    60,
    {"agency_name": "forsakringskassan"}
)

print(f"Notebook returned: {result}")
