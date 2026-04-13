# Master Thesis Data Platform: Synthetic Data, ADF ETL, and SCD Type 2 Evaluation

## Thesis Purpose

This repository is the implementation backbone of a master thesis focused on end-to-end data engineering for customer-lifecycle analytics.

The main goal is to build and evaluate a reproducible pipeline that:

1. Generates realistic synthetic customer history over time.
2. Lands that history in a cloud data lake.
3. Processes the data through an Azure Data Factory orchestration layer.
4. Loads and maintains warehouse tables with Slowly Changing Dimension (SCD) Type 2 logic.
5. Captures operational and performance monitoring data for analysis.

In short, this project is not just a generator. It is a full research environment for comparing and validating SCD Type 2 processing behavior under controlled synthetic workloads.

## Research Scope and Practical Outcome

The thesis focuses on the practical question of how a modern cloud-native pipeline can support historical dimension tracking at scale, with repeatable runs and measurable execution metrics.

This repository delivers:

- A configurable synthetic data simulator.
- Versioned ADF assets.
- A SQL project (DACPAC) for controlled schema promotion.
- Monitoring tables and procedures for run-level and procedure-level observability.
- CI/CD workflows that automate generation, deployment, and pipeline triggering.

## End-to-End Architecture

### 1) Synthetic data generation (Python)

The Python layer simulates daily changes in a customer base, including:

- Initial snapshot generation.
- New user arrivals.
- Profile and subscription changes.
- Ongoing day-by-day evolution of user records.

Output is written as CSV snapshot files and prepared for ingestion.

### 2) Storage and orchestration (ADLS + ADF)

Generated files are uploaded to ADLS Gen2.
ADF pipelines then orchestrate lookup, staging, processing, merge, and monitoring steps.

The master pipeline accepts runtime parameters:

- `fileName`
- `SCD2Method`
- `EnvironmentName`

This allows repeatable experiments with explicit run context.

### 3) Warehouse and monitoring (SQL / DACPAC)

The SQL project contains warehouse schemas and ETL procedures, including raw, stage, and prod layers.
Monitoring objects log both pipeline-level and procedure-level execution metadata, enabling post-run analysis.

## Repository Structure

### Python generator and utilities

- `data_generator/data_generator.py`: Main entry point.
- `data_generator/simulate.py`: Simulation orchestration across days.
- `data_generator/generate_initial_snapshot.py`: Day-0 dataset creation.
- `data_generator/generate_new_users.py`: Daily user growth logic.
- `data_generator/clustered_modify.py`: Controlled attribute modifications.
- `data_generator/adls/prepare_directory.py`: ADLS path preparation.
- `data_generator/adls/upload_snapshots.py`: Snapshot upload logic.
- `data_generator/adls/clean_filesystem.py`: Optional ADLS cleanup helper.
- `data_generator/utils/utils.py`: Shared utility helpers.

### Azure Data Factory assets

All ADF JSON is grouped under `adf/`:

- `adf/factory/`: Factory definition.
- `adf/pipeline/`: Pipeline definitions.
- `adf/dataset/`: Dataset definitions.
- `adf/linkedService/`: Linked service definitions.
- `adf/publish_config.json`: ADF publish metadata.

Notable pipeline:

- `pl_Master_Full_SCD2_ETL`

### SQL warehouse project

- `db/dacpac/DataWarehouse.sqlproj`: SQL project root.
- `db/dacpac/src/Schemas/`: Schema declarations.
- `db/dacpac/src/Objects/`: Tables and stored procedures by schema.
- `db/dacpac/PostDeploy.sql`: Post-deployment script.

Included schemas:

- `raw`
- `stage`
- `prod`
- `config`
- `monitor`

## ADF Git Configuration

Current ADF Git settings:

- Collaboration branch: `main`
- Publish branch: `adf_publish`
- Root folder: `/adf`

Important workflow rule:

- Author and modify ADF objects in the collaboration branch.
- Publish from ADF Studio to materialize executable state.
- Do not manually edit `adf_publish`.

## CI/CD and Automation Workflows

### Data generation workflow

- `.github/workflows/regenerate-data-into-adls.yml`
- Purpose: run Python generation and upload snapshots to ADLS.

### ADF trigger workflow

- `.github/workflows/trigger-full-scd2-adf-pipeline.yml`
- Purpose: manual trigger of the master ADF pipeline.
- Uses GitHub Environments and Azure OIDC authentication.
- Supports environment routing (`dev`, `test`, `qa`, `prod`) and parameterized execution.

### Database deployment workflows

- `.github/workflows/deploy-db-dev.yml`: lint, build DACPAC, publish to Dev.
- `.github/workflows/deploy-db.yml`: lint, build DACPAC, publish to Prod.

### Branch governance

- `.github/workflows/branch-policy.yml`

## Required Configuration

### For data generation

- `DATALAKE_CONNECTION_STRING`
- `DATALAKE_KEY`

### For ADF triggering

- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_SUBSCRIPTION_ID`
- `ADF_RESOURCE_GROUP`
- `ADF_FACTORY_NAME`
- `ADF_PIPELINE_NAME`

### For database deployment

- `DB_SERVER`
- `DB_USER`
- `DB_PASSWORD`
- `DEV_DB_NAME`
- `PROD_DB_NAME`

## Database Delivery Model

The database lifecycle is managed via DACPAC artifact promotion:

- Dev and Prod receive the same schema artifact lineage.
- Deployments are reproducible and reviewable from source control.
- SQL linting is enforced before deployment.

Current publish behavior:

- `/p:DropObjectsNotInSource=false`

This avoids automatic object drops and reduces destructive deployment risk.

## Local Development Quick Start

```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -r requirements.txt
python -m data_generator.data_generator
```

Optional runtime overrides:

```bash
DATALAKE_FILE_SYSTEM=generated-data
DATALAKE_DIRECTORY=daily
DATALAKE_FILE_NAME=generated_customers.csv
```

## Thesis Reproducibility Notes

- Keep generator code, ADF assets, and SQL schema versioned together.
- Publish ADF changes before benchmark or comparison runs.
- Use pipeline parameters (`fileName`, `SCD2Method`, `EnvironmentName`) to track run context.
- Use monitoring tables to compare execution outcomes and performance across runs.

## Summary

This repository represents a complete thesis-grade implementation of a synthetic-data-driven ETL platform with SCD Type 2 history handling, cloud orchestration, and deployment automation. It is designed to support both engineering delivery and research evaluation.
