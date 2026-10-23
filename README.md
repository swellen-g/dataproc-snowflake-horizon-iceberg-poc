# Dataproc + Snowflake Horizon Iceberg POC

A small, separate POC for reading eligible Snowflake Iceberg tables directly from Spark through the Snowflake Horizon Iceberg REST Catalog API, then optionally appending to an existing Iceberg target. It deliberately does not use the Snowflake JDBC driver or Snowflake Spark connector for data transfer.

The goal is to benchmark the complete workload, not to assume this design is cheaper. Spark compute, GCS storage and transfer, Snowflake catalog/API usage, and any cross-region egress still contribute to cost.

## Scope and table eligibility

This starter targets **Snowflake-managed Iceberg tables**, using Horizon Catalog and short-lived GCP OIDC tokens (workload identity federation). It is not a generic connector for arbitrary Snowflake tables.

Before running, the Snowflake team must confirm that:

- The source and target are eligible Snowflake-managed Iceberg tables (Snowflake documents external-engine read/write for Iceberg v2/v3 tables; verify the current table-specific limitations).
- The tables' storage is in a supported cloud/region and Horizon Catalog is enabled for the account.
- The service user, role, data-protection policies, table ownership, and append privileges are configured for external-engine access.
- The GCP service account's OIDC identity is trusted by the Snowflake service user through Snowflake Workload Identity Federation.
- The Spark/Iceberg libraries in the selected Dataproc runtime are compatible with the Horizon REST API and the table format version.

**Governance gate:** external-engine access can change how Snowflake masking/row-access policies are applied. Confirm policy enforcement with the security/data-governance owners before accessing sensitive data. Do not use production payment data until approved.

## Authentication

The batch gets a short-lived OIDC identity token from the GCP metadata server. No private key or long-lived Snowflake password is put in this repo or passed in the job command. Snowflake must be configured to trust the GCP OIDC issuer, audience, and service-account subject. The workload identity provider header and role scope must match that Snowflake configuration.

This example assumes Dataproc runs with a service account and that its metadata server can issue an identity token for `GOOGLE_OIDC_AUDIENCE`. This must be verified in the customer's GCP environment before the end-to-end run.

## Configure environment

Set these values in the submitting shell or approved job configuration. They are non-secret identifiers except that `GOOGLE_OIDC_AUDIENCE` is identity configuration, not a token:

```bash
export SNOWFLAKE_DATABASE="GSAP"
export SNOWFLAKE_ACCOUNT_IDENTIFIER="ORG-ACCOUNT"
export SNOWFLAKE_ROLE="STRIIM_SVC_ROLE"
export ICEBERG_STORAGE_REGION="us-central1"
export GOOGLE_OIDC_AUDIENCE="YOUR_CONFIGURED_SNOWFLAKE_AUDIENCE"
export SNOWFLAKE_WIF_PROVIDER="OIDC"
```

Use the Snowflake account identifier format documented for Horizon Catalog (`organization-account`), not a JDBC hostname or warehouse name. `SNOWFLAKE_DATABASE` is the database/catalog name. `ICEBERG_STORAGE_REGION` is where the Iceberg files are stored.

The GCP service account passed to the Serverless batch must be the identity trusted by the Snowflake WIF configuration. It also needs the minimum GCP permissions to submit/run the batch and access its staging bucket.

## Run a read-only smoke test

Choose the Dataproc Serverless runtime after confirming its bundled Spark and Iceberg versions satisfy the Snowflake Horizon requirements. Runtime versions and preinstalled Iceberg libraries can change, so do not add a second Iceberg runtime JAR without checking for classpath conflicts.

```bash
gcloud dataproc batches submit pyspark horizon_iceberg_batch.py \
  --region=us-central1 \
  --version=2.2 \
  --deps-bucket=gs://YOUR_DATAPROC_STAGING_BUCKET \
  --service-account=YOUR_DATAPROC_BATCH_SERVICE_ACCOUNT \
  -- \
  --account-identifier "$SNOWFLAKE_ACCOUNT_IDENTIFIER" \
  --role "$SNOWFLAKE_ROLE" \
  --storage-region "$ICEBERG_STORAGE_REGION" \
  --oidc-audience "$GOOGLE_OIDC_AUDIENCE" \
  --wif-provider "$SNOWFLAKE_WIF_PROVIDER" \
  --source-database "$SNOWFLAKE_DATABASE" \
  --source-schema "YOUR_SOURCE_SCHEMA" \
  --source-table "YOUR_SOURCE_ICEBERG_TABLE" \
  --limit 1000
```

The job is read-only unless `--append` is explicitly supplied. A row limit is a smoke-test convenience, not a reliable cost estimate; Spark may still read more underlying files depending on table layout and planning.

## Append to a pre-created POC target

Only after the read-only test succeeds and Snowflake confirms the target and grants, submit with `--append` and a separate, pre-created POC target table:

```bash
gcloud dataproc batches submit pyspark horizon_iceberg_batch.py \
  --region=us-central1 \
  --version=2.2 \
  --deps-bucket=gs://YOUR_DATAPROC_STAGING_BUCKET \
  --service-account=YOUR_DATAPROC_BATCH_SERVICE_ACCOUNT \
  -- \
  --account-identifier "$SNOWFLAKE_ACCOUNT_IDENTIFIER" \
  --role "$SNOWFLAKE_ROLE" \
  --storage-region "$ICEBERG_STORAGE_REGION" \
  --oidc-audience "$GOOGLE_OIDC_AUDIENCE" \
  --wif-provider "$SNOWFLAKE_WIF_PROVIDER" \
  --source-database "$SNOWFLAKE_DATABASE" \
  --source-schema "YOUR_SOURCE_SCHEMA" \
  --source-table "YOUR_SOURCE_ICEBERG_TABLE" \
  --target-database "$SNOWFLAKE_DATABASE" \
  --target-schema "YOUR_POC_SCHEMA" \
  --target-table "YOUR_PRECREATED_POC_ICEBERG_TABLE" \
  --limit 1000 \
  --append
```

This starter currently passes the dataframe through unchanged; port the dbt model logic in `transform_dataframe()` before comparing transformation costs. It only appends; it never overwrites, truncates, or creates the target. The target must already exist and have a compatible schema. Do not point the first write at a production table.

## Cost comparison checklist

Compare equivalent completed outputs and include:

- Dataproc Serverless batch charge and startup/runtime duration.
- Horizon catalog/API cloud-services charges and any cross-region Snowflake egress.
- GCS storage, operations, and network egress for the table location.
- Any Snowflake warehouse usage for the original model and any remaining Snowflake operations.
- Retry/maintenance cost, Iceberg snapshot maintenance, and policy/governance requirements.

The original report's example command used `LIMIT 1` while its metrics reported more than 2.2 million rows. Reconcile the command, source/target table names, and row counts before using those measurements as a comparison baseline.

## Local checks

The unit tests cover catalog configuration and identifier quoting. They do not validate cloud credentials, Snowflake grants, runtime JAR compatibility, or table policy behavior.

```bash
python3 -m unittest discover -s tests -v
python3 -m py_compile horizon_iceberg_batch.py
```

## References

- [Snowflake Horizon Catalog: external-engine access to Iceberg](https://docs.snowflake.com/en/user-guide/tables-iceberg-access-using-external-query-engine-snowflake-horizon)
- [Snowflake Iceberg table overview and billing considerations](https://docs.snowflake.com/en/user-guide/tables-iceberg)
- [Dataproc Serverless batch jobs](https://docs.cloud.google.com/managed-spark/docs)