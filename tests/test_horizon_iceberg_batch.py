import unittest

from horizon_iceberg_batch import build_catalog_configs, table_identifier


class HorizonIcebergBatchTests(unittest.TestCase):
    def test_table_identifier_quotes_each_part(self):
        self.assertEqual(
            table_identifier("MY.DB", "RAW", "EVENTS"),
            "`MY.DB`.`RAW`.`EVENTS`",
        )

    def test_table_identifier_escapes_backticks(self):
        self.assertEqual(
            table_identifier("DB`ONE", "RAW", "EVENTS"),
            "`DB``ONE`.`RAW`.`EVENTS`",
        )

    def test_catalog_config_uses_horizon_and_vended_credentials(self):
        configs = build_catalog_configs(
            "org-account", "analytics", "spark_role", "us-central1", "token"
        )
        prefix = "spark.sql.catalog.ANALYTICS"

        self.assertEqual(
            configs[f"{prefix}.uri"],
            "https://org-account.snowflakecomputing.com/polaris/api/catalog",
        )
        self.assertEqual(configs[f"{prefix}.credential"], "token")
        self.assertEqual(configs[f"{prefix}.scope"], "session:role:spark_role")
        self.assertEqual(
            configs[f"{prefix}.header.X-Iceberg-Access-Delegation"],
            "vended-credentials",
        )


if __name__ == "__main__":
    unittest.main()
