# type: ignore
# Databricks notebook source

# COMMAND ----------
# %md
# # Exercise 1.1: Workspace, Unity Catalog Volumes, and the ENA Folder Structure
#
# This notebook orients a new DIGG data engineer to the ENA platform's storage layout.
# Every downstream exercise, ingestion, transformation, streaming, depends on the folder
# structure created by `SETUP_RUN_ONCE.py`. This notebook verifies that structure exists,
# is correctly organised, and is writable before any real agency data touches it.
#
# **What this notebook demonstrates:**
# - Navigating a Unity Catalog volume using `dbutils.fs`
# - Verifying that all five agency landing zones are present
# - Confirming the volume is readable and writable via a file round-trip
#
# **Dependencies:** `SETUP_RUN_ONCE.py` must have been run first to create
# the `ena_dev` catalog, `default` schema, and `ena_platform` volume.

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

# Confirming all five agency landing zone folders exist by
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

# Writing a small test file to the Skatteverket landing zone, then reading it back.
# This proves the volume is writable before any real agency data lands there.
#
# Steps:
# 1. Define a test_path using BASE_PATH without hardcode it
# 2. Use dbutils.fs.put() to write a short string to that path
# 3. Use dbutils.fs.head() to read it back and print the contents
# 4. Clean up: use dbutils.fs.rm() to delete the test file

TEST_PATH = BASE_PATH + "/landing/skatteverket/test.txt"
dbutils.fs.put(TEST_PATH, "This is a test string.")
test_string = dbutils.fs.head(TEST_PATH)
print(f"Successfully reading: \"{test_string}\" from {TEST_PATH}")
dbutils.fs.rm(TEST_PATH)

# COMMAND ----------
# %md
# ## What I Learned
#
# **Unity Catalog volume vs DBFS:**
# A Unity Catalog volume is a governed storage location inside a three-part namespace
# (`catalog.schema.volume`). Unlike plain DBFS, which is a flat, ungoverned file system
# accessible to anyone on the workspace, volumes have access controls, audit logs, and
# lineage tracking applied by Unity Catalog.
#
# **How `dbutils.fs` interacts with a volume:**
# `dbutils.fs` treats a volume path like any other file system path. `ls`, `put`, `head`,
# and `rm` all work identically, the difference is that every operation is governed and
# auditable under Unity Catalog.
#
# **Why landing / bronze / silver / gold:**
# Each layer represents a different stage of data trustworthiness. Landing holds raw
# agency files as delivered. Bronze preserves the source schema with audit columns added.
# Silver applies cleaning, typing, deduplication, and joins. Gold produces policy-ready
# aggregations. Separating layers means a failure at Silver never corrupts Bronze data.
#
# **What would happen if two agencies wrote to each other's landing zone:**
# Data isolation would break. DIGG's platform promises each agency that their data is
# handled under strict accountability. Mixed landing zones would corrupt audit trails,
# make pipeline errors unattributable, and violate the governance contract with each agency.
#
# **Gotchas:**
# - `dbutils.fs.ls()` returns folder names with a trailing slash, always `.rstrip("/")`
#   before comparing against expected names.
# - `dbutils.fs.put()` takes path first, content second.
#
# **Next exercise:** `02a_cluster_and_magic.py`, cluster configuration and magic commands.