"""Actual integration: two Spark runs, independent CSV reconciliation, rollback."""

import argparse
import csv
from decimal import Decimal
import json

from psycopg import sql
from pipeline.db import ROOT, connect
from pipeline.job import TABLE_COLUMNS, extract_bound, run_batch, validate_snapshot
from pipeline.seed import seed


def fingerprint(snapshot_date):
    result = {}
    with connect("warehouse") as connection:
        for table in TABLE_COLUMNS:
            order = "customer_id" if table == "customer_facts" else "ingest_id" if table == "quarantine" else "segment,channel" if table == "channel_metrics" else "segment"
            query = sql.SQL("SELECT md5(string_agg(md5(t::text),'|' ORDER BY {})) AS digest, COUNT(*) AS rows FROM {} AS t WHERE snapshot_date=%s").format(sql.SQL(order), sql.Identifier(table))
            result[table] = connection.execute(query, (snapshot_date,)).fetchone()
    return result


def reconcile_original_csv(snapshot_date):
    expected = {}
    with (ROOT / "data/raw/hillstrom.csv").open(encoding="utf-8", newline="") as stream:
        for row in csv.DictReader(stream):
            group = expected.setdefault(row["segment"], {"users": 0, "buyers": 0, "revenue": Decimal("0")})
            group["users"] += 1
            group["buyers"] += int(row["conversion"])
            group["revenue"] += Decimal(row["spend"])
    with connect("warehouse") as connection:
        actual = connection.execute("SELECT segment,users,buyers,revenue FROM experiment_metrics WHERE snapshot_date=%s", (snapshot_date,)).fetchall()
        for row in actual:
            assert {key: row[key] for key in ["users", "buyers", "revenue"]} == expected[row["segment"]]
        assert len(actual) == len(expected)
        assert connection.execute("SELECT COUNT(*) AS n FROM quarantine WHERE snapshot_date=%s AND NOT is_demo", (snapshot_date,)).fetchone()["n"] == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date", required=True)
    args = parser.parse_args()
    seed()
    print("Integration 1/3: initial Spark publication", flush=True)
    first = run_batch(extract_bound(args.snapshot_date))
    initial_hash = fingerprint(args.snapshot_date)
    assert first["counts"] == {"raw": 64012, "rejected": 7, "clean": 64000, "duplicates": 5}
    reconcile_original_csv(args.snapshot_date)
    print("Integration 2/3: repeat publication of the same snapshot", flush=True)
    second = run_batch(extract_bound(args.snapshot_date))
    assert fingerprint(args.snapshot_date) == initial_hash, "Repeat changed analytical data"
    print("Integration 3/3: inject a failure inside the publication transaction", flush=True)
    failed = extract_bound(args.snapshot_date)
    try:
        run_batch(failed, fail_before_commit=True)
    except RuntimeError as error:
        assert "Injected failure before COMMIT" in str(error)
    else:
        raise AssertionError("The controlled failure was not raised")
    assert fingerprint(args.snapshot_date) == initial_hash, "Rollback did not preserve the previous snapshot"
    with connect("warehouse") as connection:
        assert connection.execute("SELECT status FROM etl_runs WHERE run_id=%s", (failed["run_id"],)).fetchone()["status"] == "failed"
    evidence = {"snapshot_date": args.snapshot_date, "spark_version": first["spark_version"],
                "counts": first["counts"], "source_sha256": first["source_sha256"],
                "idempotence": True, "rollback_before_commit": True,
                "csv_reconciliation": True, "original_rows_rejected": 0,
                "data_fingerprints": initial_hash,
                "success_run_ids": [first["run_id"], second["run_id"]],
                "controlled_failure_run_id": failed["run_id"],
                "warehouse": validate_snapshot(args.snapshot_date),
                "airflow_run": {"status": "awaiting_separate_scheduler_check"}}
    (ROOT / "results/verification.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print(json.dumps(evidence, indent=2), flush=True)


if __name__ == "__main__":
    main()
