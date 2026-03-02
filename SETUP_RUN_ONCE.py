folders = [
    # Agency landing zones, raw files from each of the five ENA agencies
    "dbfs:/ena-platform/landing/skatteverket",
    "dbfs:/ena-platform/landing/arbetsformedlingen",
    "dbfs:/ena-platform/landing/forsakringskassan",
    "dbfs:/ena-platform/landing/socialstyrelsen",
    "dbfs:/ena-platform/landing/scb",
    # Medallion layers
    "dbfs:/ena-platform/bronze",
    "dbfs:/ena-platform/silver",
    "dbfs:/ena-platform/gold",
    # Operations
    "dbfs:/ena-platform/quarantine",
    "dbfs:/ena-platform/checkpoints",
    # ML artefacts
    "dbfs:/ena-platform/ml/models",
]

for folder in folders:
    dbutils.fs.mkdirs(folder) # type: ignore
    print(f"✅  Created: {folder}")

print("\nSetup complete!")