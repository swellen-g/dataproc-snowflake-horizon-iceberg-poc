#!/usr/bin/env python3
"""Read and optionally append Iceberg data through Snowflake Horizon Catalog."""

from argparse import ArgumentParser
import os
from urllib.parse import urlencode
from urllib.request import Request, urlopen


METADATA_IDENTITY_URL = (
    "http://metadata.google.internal/computeMetadata/v1/"
    "instance/service-accounts/default/identity"
)


def quote_identifier(identifier: str) -> str:
    """Quote one Spark SQL identifier."""
    if not identifier or "\x00" in identifier:
        raise ValueError("Table identifiers must be non-empty and cannot contain NUL.")
    return "`" + identifier.replace("`", "``") + "`"


def table_identifier(database: str, schema: str, table: str) -> str:
    """Build a safely quoted Iceberg catalog identifier."""
    return ".".join(quote_identifier(part) for part in (database, schema, table))


def transform_dataframe(dataframe):
    """Return the dataframe after applying migrated dbt model logic."""
    return dataframe


def fetch_gcp_oidc_token(audience: str) -> str:
    """Fetch a short-lived GCP identity token from the Dataproc metadata server."""
    if not audience:
        raise ValueError("GOOGLE_OIDC_AUDIENCE must be configured for Snowflake WIF.")

    url = f"{METADATA_IDENTITY_URL}?{urlencode({'audience': audience, 'format': 'full'})}"
    request = Request(url, headers={"Metadata-Flavor": "Google"})
    with urlopen(request, timeout=10) as response:
        token = response.read().decode("utf-8").strip()

    if not token:
        raise RuntimeError("GCP metadata server returned an empty OIDC token.")
    return token


def build_catalog_configs(
    account_identifier: str,
    database: str,
    role: str,
    region: str,
    oidc_token: str,
    identity_provider: str = "OIDC",
) -> dict[str, str]:
    """Build Spark Iceberg REST catalog settings for Snowflake Horizon."""
    catalog_name = database.upper()
    prefix = f"spark.sql.catalog.{catalog_name}"
    return {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": catalog_name,
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        f"{prefix}.uri": (
            f"https://{account_identifier}.snowflakecomputing.com/polaris/api/catalog"
        ),
        f"{prefix}.warehouse": catalog_name,
        f"{prefix}.credential": oidc_token,
        f"{prefix}.scope": f"session:role:{role}",
        f"{prefix}.client.region": region,
        f"{prefix}.header.X-Snowflake-Workload-Identity-Provider": identity_provider,
        f"{prefix}.header.X-Iceberg-Access-Delegation": "vended-credentials",
    }


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--account-identifier", default=os.getenv("SNOWFLAKE_ACCOUNT_IDENTIFIER")
    )
    parser.add_argument("--role", default=os.getenv("SNOWFLAKE_ROLE"))
    parser.add_argument("--storage-region", default=os.getenv("ICEBERG_STORAGE_REGION"))
    parser.add_argument("--oidc-audience", default=os.getenv("GOOGLE_OIDC_AUDIENCE"))
    parser.add_argument(
        "--wif-provider", default=os.getenv("SNOWFLAKE_WIF_PROVIDER", "OIDC")
    )
    parser.add_argument("--source-database", default=os.getenv("SNOWFLAKE_DATABASE"))
    parser.add_argument("--source-schema", default=os.getenv("SNOWFLAKE_SOURCE_SCHEMA"))
    parser.add_argument("--source-table", default=os.getenv("SNOWFLAKE_SOURCE_TABLE"))
    parser.add_argument("--target-database", default=os.getenv("SNOWFLAKE_DATABASE"))
    parser.add_argument("--target-schema", default=os.getenv("SNOWFLAKE_TARGET_SCHEMA"))
    parser.add_argument("--target-table", default=os.getenv("SNOWFLAKE_TARGET_TABLE"))
    parser.add_argument("--limit", type=int, help="Optional row limit for a smoke test.")
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to the existing target table. Without this flag, the job is read-only.",
    )
    return parser.parse_args()


def main():
    args = parse_arguments()
    if args.limit is not None and args.limit < 1:
        raise ValueError("--limit must be a positive integer.")

    for name, value in (
        ("--source-database", args.source_database),
        ("--source-schema", args.source_schema),
        ("--source-table", args.source_table),
    ):
        if not value:
            raise ValueError(f"{name} is required for the source Iceberg table.")

    for name, value in (
        ("--account-identifier", args.account_identifier),
        ("--role", args.role),
        ("--storage-region", args.storage_region),
        ("--oidc-audience", args.oidc_audience),
    ):
        if not value:
            raise ValueError(f"{name} is required.")

    source = table_identifier(
        args.source_database, args.source_schema, args.source_table
    )
    target = None
    if args.append:
        for name, value in (
            ("--target-database", args.target_database),
            ("--target-schema", args.target_schema),
            ("--target-table", args.target_table),
        ):
            if not value:
                raise ValueError(f"{name} is required when --append is used.")
        target = table_identifier(
            args.target_database, args.target_schema, args.target_table
        )
        if source == target:
            raise ValueError("Source and target must differ for an append run.")
        if args.target_database.upper() != args.source_database.upper():
            raise ValueError(
                "This POC configures one Horizon catalog database per run; "
                "source and target databases must match."
            )

    oidc_token = fetch_gcp_oidc_token(args.oidc_audience)

    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName("SnowflakeHorizonIcebergPOC")
    for key, value in build_catalog_configs(
        args.account_identifier,
        args.source_database,
        args.role,
        args.storage_region,
        oidc_token,
        args.wif_provider,
    ).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    try:
        source_df = spark.read.format("iceberg").load(source)
        if args.limit is not None:
            source_df = source_df.limit(args.limit)
        transformed_df = transform_dataframe(source_df)

        print(f"Source Iceberg table: {source}")
        print(f"Output schema: {transformed_df.schema.simpleString()}")

        if target:
            transformed_df.writeTo(target).append()
            print(f"Appended rows to Iceberg target: {target}")
        else:
            print("Read-only run complete; no target table was modified.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
