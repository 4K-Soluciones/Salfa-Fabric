# Fabric workspace: Lakehouse + Notebooks

This folder contains scaffolding for Microsoft Fabric lakehouse work and example notebooks to ingest CDS views, perform ETP in Python, store results in a silver layer, and export CSVs in a gold layer.

Setup (fill placeholders before running):

- Update `lakehouse_config.yml` with your lakehouse names, OneLake root path, and JDBC connection info for the CDS view.
- Store credentials/secrets in your Fabric workspace or Azure Key Vault and reference them in notebook connection code.

Run (in Microsoft Fabric environment / Spark notebook):

1. Open `fabric/notebooks/01_ingest_etl.ipynb` and set the JDBC connection details.
2. Run the notebook to ingest the CDS view and write the cleaned data to the silver path.
3. Open `fabric/notebooks/02_gold_export.ipynb`, adjust aggregations, and run to produce CSV files into the gold path.

Notes:
- The example notebooks use PySpark with generic JDBC placeholders. Replace with the connector you use in your environment (ODBC, SAP connector, or Fabric-native connectors).
- Silver layer is stored as Parquet for performance; gold exports are CSV as requested.

If you want, I can: create secrets templates, add a sample CI workflow, or adapt notebooks to a specific connector (SAP, OData, MSSQL).