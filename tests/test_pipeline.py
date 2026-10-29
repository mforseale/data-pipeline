"""Meaningful checks for Spark cleaning, source provenance and the real DAG."""

from datetime import datetime, timezone
from decimal import Decimal
import unittest

from pipeline.job import snapshot_iso, spark_session, transform
from pipeline.seed import load_rows


class SparkTransformTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()
        schema = "ingest_id long, customer_id string, recency string, history string, visit string, conversion string, spend string, segment string, channel string, is_demo boolean"
        base = {"customer_id": "0", "recency": "2", "history": "100.00", "visit": "0",
                "conversion": "0", "spend": "0", "segment": "No E-Mail", "channel": "Web", "is_demo": False}
        rows = [{**base, "ingest_id": 1},
                {**base, "ingest_id": 2, "customer_id": "1", "visit": "1", "conversion": "1", "spend": "50.25"},
                {**base, "ingest_id": 3, "customer_id": "2", "visit": "1", "conversion": "1", "spend": "50.25"},
                {**base, "ingest_id": 4, "channel": "Phone", "is_demo": True}]
        bad = [{"customer_id": ""}, {"spend": "-5"}, {"spend": "NaN"}, {"visit": "2"},
               {"segment": "Unknown"}, {"conversion": "1"}, {"recency": "13"}]
        rows += [{**base, "ingest_id": index + 5, "customer_id": str(10 + index),
                  "is_demo": True, **defect} for index, defect in enumerate(bad)]
        cls.raw = cls.spark.createDataFrame(rows, schema=schema)
        cls.frames, cls.counts = transform(cls.raw, "2026-10-28")
        cls.facts = cls.frames["customer_facts"].collect()
        cls.rejected = cls.frames["quarantine"].collect()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_row_accounting_and_quarantine(self):
        self.assertEqual(self.counts, {"raw": 11, "rejected": 7, "clean": 3, "duplicates": 1})
        self.assertEqual(len(self.rejected), 7)
        self.assertTrue(all(row.reason and row.raw_payload for row in self.rejected))

    def test_zero_spend_customer_is_preserved(self):
        zero = next(row for row in self.facts if row.customer_id == 0)
        self.assertEqual(zero.spend, Decimal("0.00"))
        self.assertEqual(zero.conversion, 0)

    def test_latest_valid_version_wins_deterministically(self):
        latest = next(row for row in self.facts if row.customer_id == 0)
        self.assertEqual(latest.channel, "Phone")
        self.assertEqual(latest.source_ingest_id, 4)

    def test_identical_features_of_different_customers_are_not_removed(self):
        self.assertEqual({row.customer_id for row in self.facts}, {0, 1, 2})

    def test_money_is_decimal_and_aggregates_include_all_customers(self):
        self.assertTrue(all(isinstance(row.spend, Decimal) for row in self.facts))
        metrics = self.frames["experiment_metrics"].collect()
        self.assertEqual(sum(row.users for row in metrics), 3)
        self.assertEqual(sum(row.buyers for row in metrics), 2)
        self.assertEqual(sum(row.revenue for row in metrics), Decimal("100.50"))

    def test_invalid_data_is_not_cast_to_zero_or_silently_dropped(self):
        all_reasons = ",".join(row.reason for row in self.rejected)
        for reason in ["invalid_customer_id", "invalid_spend", "invalid_visit", "invalid_segment", "inconsistent_purchase", "invalid_recency"]:
            self.assertIn(reason, all_reasons)


class ContractTests(unittest.TestCase):
    def test_snapshot_date_is_explicit_and_validated(self):
        self.assertEqual(snapshot_iso("2026-10-28"), "2026-10-28")
        for value in ["2026-99-28", "28/10/2026", "2026-10-28;DROP TABLE x"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                snapshot_iso(value)

    def test_source_has_original_rows_and_explicit_demo_defects(self):
        records, metadata = load_rows()
        self.assertEqual(len(records), 64012)
        self.assertEqual(sum(row["is_demo"] for row in records), 12)
        self.assertEqual(len(metadata["sha256"]), 64)
        self.assertEqual([row["customer_id"] for row in records[:3]], ["0", "1", "2"])

    def test_airflow_dag_has_real_dependencies_schedule_and_retries(self):
        from dags.crm_daily_etl import dag
        expected = {"extract_source_bound": {"spark_transform_and_publish"},
                    "spark_transform_and_publish": {"check_warehouse"},
                    "check_warehouse": {"build_report"}, "build_report": set()}
        self.assertEqual({task.task_id: task.downstream_task_ids for task in dag.tasks}, expected)
        self.assertEqual(dag.schedule, "0 10 * * *")
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertTrue(all(task.retries == 1 for task in dag.tasks))


if __name__ == "__main__":
    unittest.main()
