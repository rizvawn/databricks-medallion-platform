# type: ignore
# First, create the Unity Catalog infrastructure if it doesn't exist
print("Setting up Unity Catalog infrastructure...\n")

# Create catalog
spark.sql("CREATE CATALOG IF NOT EXISTS ena_dev")
print("✅  Catalog 'ena_dev' ready")

# Create default schema
spark.sql("CREATE SCHEMA IF NOT EXISTS ena_dev.default")
print("✅  Schema 'ena_dev.default' ready")

# Create the main volume
spark.sql("""
    CREATE VOLUME IF NOT EXISTS ena_dev.default.ena_platform
    COMMENT 'ENA platform storage for all agency data, medallion layers, and ML artifacts'
""")
print("✅  Volume 'ena_dev.default.ena_platform' ready\n")

# Create folder structure within the volume
folders = [
    # Agency landing zones, raw files from each of the five ENA agencies
    "/Volumes/ena_dev/default/ena_platform/landing/skatteverket",
    "/Volumes/ena_dev/default/ena_platform/landing/arbetsformedlingen",
    "/Volumes/ena_dev/default/ena_platform/landing/forsakringskassan",
    "/Volumes/ena_dev/default/ena_platform/landing/socialstyrelsen",
    "/Volumes/ena_dev/default/ena_platform/landing/scb",
    # Medallion layers
    "/Volumes/ena_dev/default/ena_platform/bronze",
    "/Volumes/ena_dev/default/ena_platform/silver",
    "/Volumes/ena_dev/default/ena_platform/gold",
    # Operations
    "/Volumes/ena_dev/default/ena_platform/quarantine",
    "/Volumes/ena_dev/default/ena_platform/checkpoints",
    # ML artefacts
    "/Volumes/ena_dev/default/ena_platform/ml/models",
]

print("Creating folder structure...")
for folder in folders:
    dbutils.fs.mkdirs(folder) # type: ignore
    print(f"✅  Created: {folder}")

print("\n" + "="*70)
print("Setup complete!")
print("="*70)