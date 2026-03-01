# Databricks Medallion Platform

## Purpose

This is a personal demo and learning project for exploring **Databricks** and its ecosystem of tools, including PySpark, Delta Lake, Auto Loader, Structured Streaming, Unity Catalog, and MLflow.

## Inspiration

The project is inspired by **ENA (Ena nationella infrastruktur)**, a programme led by **DIGG (Myndigheten för digital förvaltning)** to coordinate AI and data services across Swedish public agencies. This project mimics the structure and goals of such a platform as a learning exercise.

> **Disclaimer:** This project has no official affiliation with DIGG, ENA, or any Swedish government agency. All agency names are used for illustrative purposes only to simulate realistic data engineering scenarios.

## Data

All data in this project is **fully synthetic**. It is generated programmatically using the `Faker` library with a Swedish locale (`sv_SE`) to produce realistic-looking but entirely fictional records. No real personal, agency, or government data is used at any point.

## Architecture

The project follows a **Medallion architecture** with three layers:

- **Bronze** — raw ingested data from simulated agency feeds
- **Silver** — cleaned, deduplicated, and enriched data
- **Gold** — aggregated analytics and composite scores for decision support

## Goal

To build hands-on experience with modern data engineering practices on Databricks by working through realistic, end-to-end pipeline scenarios.
