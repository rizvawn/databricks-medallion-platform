# type: ignore
# Databricks notebook source

# COMMAND ----------
# %md
# # Exercise 1.1 — Workspace, Unity Catalog Volumes, and the ENA Folder Structure
#
# **Purpose:** Explore the ENA platform's Unity Catalog volume layout using `dbutils.fs`.
# This is the foundation every subsequent notebook depends on — if the volume structure
# is wrong, every ingestion and transformation step in later phases will fail.
#
# **To build:**
# - A 3-level directory tree of the ENA volume
# - A verification check confirming all five agency landing zones exist
# - A file write/read round-trip proving the volume is accessible and writable
#
# **Dependencies:** `SETUP_RUN_ONCE.py` must have been run first.

# COMMAND ----------

# Setting the base path — all paths in this notebook derive from here, never hardcoded
BASE_PATH = "/Volumes/ena_dev/default/ena_platform"

# COMMAND ----------

# TODO: Printing the volume directory tree to 3 levels deep by using:
# dbutils.fs.ls(BASE_PATH) to get the top-level items.

items = dbutils.fs.ls(BASE_PATH)

for item in items:
    if item.size == 0:
        print(item.path)
        folders = dbutils.fs.ls(item.path)
        for folder in folders:
            if folder.size == 0:
                print("\t" + folder.path)
                subfolders = dbutils.fs.ls(folder.path)
                for subfolder in subfolders:
                    if subfolder.size == 0:
                        print("\t\t" + subfolder.path)

# COMMAND ----------
# %md
# ### ENA Volume Structure — Annotated
#
# Explaining each folder's purpose.
#
# | Folder | Purpose |
# |---|---|
# | `landing/` | TODO |
# | `bronze/` | TODO |
# | `silver/` | TODO |
# | `gold/` | TODO |
# | `quarantine/` | TODO |
# | `checkpoints/` | TODO |
# | `ml/` | TODO |

# COMMAND ----------

# TODO: Confirming all five agency landing zone folders exist by
# using dbutils.fs.ls() on the landing subfolder.

expected_agencies = [
    "skatteverket",
    "arbetsformedlingen",
    "forsakringskassan",
    "socialstyrelsen",
    "scb",
]

agencies = dbutils.fs.ls(BASE_PATH + "/landing")
existing_agencies = [agency.name.rstrip("/") for agency in agencies]
for expected in expected_agencies:
    if expected in existing_agencies:
        print(f"{expected} landing zone folder exists.")
    else:
        print(f"{expected} landing zone folder doesn't exist.")


# COMMAND ----------

# TODO: Writing a small test file to the Skatteverket landing zone, then reading it back.
# This proves the volume is writable before any real agency data lands there.
#
# Steps:
# 1. Define a test_path using BASE_PATH without hardcode it
# 2. Use dbutils.fs.put() to write a short string to that path
# 3. Use dbutils.fs.head() to read it back and print the contents
# 4. Clean up: use dbutils.fs.rm() to delete the test file

# COMMAND ----------
# %md
# ### Learned points:
#
# TODO: Answering the following:
# - What is a Unity Catalog volume, and how does it differ from plain DBFS?
# - How does `dbutils.fs` interact with a volume path?
# - Why does the ENA platform separate data into landing / bronze / silver / gold?
# - What would happen if two agencies accidentally wrote to each other's landing zone?