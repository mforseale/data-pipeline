"""A consistent source snapshot, Spark transformations and atomic publication."""

import argparse
from datetime import date
import json
import os
import time
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.types.json import Jsonb
from pyspark.sql import SparkSession, Window, functions as F

from pipeline.db import connect, dsn, initialize_warehouse

FACT_COLUMNS = ["snapshot_date", "customer_id", "segment", "channel", "recency", "history",
                "visit", "conversion", "spend", "source_ingest_id"]
METRIC_COLUMNS = ["snapshot_date", "segment", "users", "buyers", "visitors", "revenue",
                  "conversion_rate", "revenue_per_user"]
CHANNEL_COLUMNS = ["snapshot_date", "segment", "channel", "users", "buyers", "revenue"]
QUARANTINE_COLUMNS = ["snapshot_date", "ingest_id", "customer_id", "reason", "raw_payload", "is_demo"]
TABLE_COLUMNS = {"customer_facts": FACT_COLUMNS, "experiment_metrics": METRIC_COLUMNS,
                 "channel_metrics": CHANNEL_COLUMNS, "quarantine": QUARANTINE_COLUMNS}


def snapshot_iso(value):
    parsed = date.fromisoformat(str(value))
    if str(value) != parsed.isoformat():
        raise ValueError("Snapshot date must use YYYY-MM-DD")
    return parsed.isoformat()


def extract_bound(snapshot_date):
    snapshot_date = snapshot_iso(snapshot_date)
    with connect("source") as connection:
        manifest = connection.execute("SELECT * FROM source_manifest WHERE dataset='hillstrom'").fetchone()
        if not manifest:
            raise ValueError("Initialize the source with python -m pipeline.seed")
        bound = connection.execute("SELECT MAX(ingest_id) AS upper_bound, COUNT(*) AS raw_count FROM raw_customers").fetchone()
        if not bound["raw_count"]:
            raise ValueError("The source is empty")
    return {"snapshot_date": snapshot_date, "run_id": uuid4().hex,
            "source_upper_bound": bound["upper_bound"], "raw_count": bound["raw_count"],
            "expected_clean_rows": manifest["expected_clean_rows"], "source_sha256": manifest["sha256"]}


def spark_session():
    spark = (SparkSession.builder.master("local[2]").appName("crm-daily-etl")
             .config("spark.driver.memory", "1g")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.default.parallelism", "2")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.jars", os.environ.get("POSTGRES_JDBC_JAR", "/opt/jars/postgresql.jar"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def jdbc(kind):
    config = conninfo_to_dict(dsn(kind))
    return {"url": f"jdbc:postgresql://{config['host']}:{config.get('port', '5432')}/{config['dbname']}",
            "user": config["user"], "password": config["password"], "driver": "org.postgresql.Driver"}


def transform(raw, snapshot_date):
    """Use Spark SQL expressions only: safe casting, quarantine and version deduplication."""
    snapshot_date = snapshot_iso(snapshot_date)
    typed = raw.select(
        "ingest_id", "is_demo", F.to_json(F.struct(*[F.col(c) for c in raw.columns])).alias("raw_payload"),
        F.col("customer_id").alias("raw_customer_id"),
        F.trim("customer_id").try_cast("bigint").alias("customer_id"),
        F.trim("segment").alias("segment"), F.trim("channel").alias("channel"),
        F.trim("recency").try_cast("int").alias("recency"),
        F.trim("history").try_cast("decimal(18,2)").alias("history"),
        F.trim("visit").try_cast("int").alias("visit"),
        F.trim("conversion").try_cast("int").alias("conversion"),
        F.trim("spend").try_cast("decimal(18,2)").alias("spend"))
    checks = {
        "invalid_customer_id": F.col("customer_id").isNull() | (F.col("customer_id") < 0),
        "invalid_recency": F.col("recency").isNull() | ~F.col("recency").between(1, 12),
        "invalid_history": F.col("history").isNull() | (F.col("history") < 0),
        "invalid_spend": F.col("spend").isNull() | (F.col("spend") < 0),
        "invalid_visit": F.col("visit").isNull() | ~F.col("visit").isin(0, 1),
        "invalid_conversion": F.col("conversion").isNull() | ~F.col("conversion").isin(0, 1),
        "invalid_segment": F.col("segment").isNull() | ~F.col("segment").isin("No E-Mail", "Mens E-Mail", "Womens E-Mail"),
        "invalid_channel": F.col("channel").isNull() | ~F.col("channel").isin("Web", "Phone", "Multichannel"),
        "inconsistent_purchase": (F.col("conversion") == 1) != (F.col("spend") > 0),
        "purchase_without_visit": F.col("conversion") > F.col("visit"),
    }
    reasons = F.filter(F.array(*[F.when(test, F.lit(reason)) for reason, test in checks.items()]), lambda x: x.isNotNull())
    marked = typed.withColumn("reasons", reasons).cache()
    invalid = (marked.filter(F.size("reasons") > 0)
               .withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
               .withColumn("reason", F.concat_ws(",", "reasons"))
               .withColumn("customer_id", F.col("raw_customer_id"))
               .select(*QUARANTINE_COLUMNS))
    valid = marked.filter(F.size("reasons") == 0)
    version_order = Window.partitionBy("customer_id").orderBy(F.col("ingest_id").desc())
    latest = valid.withColumn("version_rank", F.row_number().over(version_order)).filter(F.col("version_rank") == 1)
    facts = (latest.withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
             .withColumnRenamed("ingest_id", "source_ingest_id").select(*FACT_COLUMNS).cache())
    metrics = (facts.groupBy("snapshot_date", "segment")
               .agg(F.count("*").alias("users"), F.sum("conversion").alias("buyers"),
                    F.sum("visit").alias("visitors"), F.sum("spend").alias("revenue"))
               .withColumn("conversion_rate", F.col("buyers") / F.col("users"))
               .withColumn("revenue_per_user", F.col("revenue").cast("double") / F.col("users"))
               .select(*METRIC_COLUMNS))
    channels = (facts.groupBy("snapshot_date", "segment", "channel")
                .agg(F.count("*").alias("users"), F.sum("conversion").alias("buyers"),
                     F.sum("spend").alias("revenue")).select(*CHANNEL_COLUMNS))
    raw_count, invalid_count, clean_count = raw.count(), invalid.count(), facts.count()
    counts = {"raw": raw_count, "rejected": invalid_count, "clean": clean_count,
              "duplicates": raw_count - invalid_count - clean_count}
    return {"customer_facts": facts, "experiment_metrics": metrics,
            "channel_metrics": channels, "quarantine": invalid}, counts


def verify_in_connection(connection, snapshot_date, expected_clean=None):
    facts = connection.execute("SELECT COUNT(*) AS users, COALESCE(SUM(spend),0) AS revenue, COALESCE(SUM(conversion),0) AS buyers FROM customer_facts WHERE snapshot_date=%s", (snapshot_date,)).fetchone()
    metrics = connection.execute("SELECT COALESCE(SUM(users),0) AS users, COALESCE(SUM(revenue),0) AS revenue, COALESCE(SUM(buyers),0) AS buyers FROM experiment_metrics WHERE snapshot_date=%s", (snapshot_date,)).fetchone()
    channels = connection.execute("SELECT COALESCE(SUM(users),0) AS users, COALESCE(SUM(revenue),0) AS revenue, COALESCE(SUM(buyers),0) AS buyers FROM channel_metrics WHERE snapshot_date=%s", (snapshot_date,)).fetchone()
    if facts != metrics or facts != channels:
        raise ValueError("Facts and aggregate metrics do not reconcile")
    if not facts["users"] or (expected_clean is not None and facts["users"] != expected_clean):
        raise ValueError("Unexpected published customer count")
    invalid = connection.execute("SELECT COUNT(*) AS n FROM customer_facts WHERE snapshot_date=%s AND (spend<0 OR conversion NOT IN (0,1) OR visit NOT IN (0,1) OR (conversion=1)<>(spend>0))", (snapshot_date,)).fetchone()["n"]
    if invalid:
        raise ValueError("Invalid rows reached the analytical facts")
    return {"users": facts["users"], "revenue_usd": str(facts["revenue"]), "buyers": facts["buyers"]}


def publish(connection, stage_tables, snapshot_date, counts, run_id, started, fail_before_commit=False):
    """The connection context commits all target tables together or rolls them back."""
    connection.execute("SELECT pg_advisory_xact_lock(7342012026)")
    for target, columns in TABLE_COLUMNS.items():
        connection.execute(sql.SQL("DELETE FROM {} WHERE snapshot_date=%s").format(sql.Identifier(target)), (snapshot_date,))
        column_list = sql.SQL(",").join(map(sql.Identifier, columns))
        connection.execute(sql.SQL("INSERT INTO {} ({}) SELECT {} FROM staging.{}").format(
            sql.Identifier(target), column_list, column_list, sql.Identifier(stage_tables[target])))
    verify_in_connection(connection, snapshot_date, counts["clean"])
    if fail_before_commit:
        raise RuntimeError("Injected failure before COMMIT: previous snapshot must survive")
    connection.execute("UPDATE etl_runs SET status='success', counts=%s, finished_at=now(), duration_seconds=%s, error=NULL WHERE run_id=%s",
                       (Jsonb(counts), time.perf_counter() - started, run_id))


def run_batch(extract, fail_before_commit=False):
    snapshot_date = snapshot_iso(extract["snapshot_date"])
    initialize_warehouse()
    started, spark = time.perf_counter(), None
    token = uuid4().hex[:12]
    stage_tables = {target: f"batch_{token}_{target}" for target in TABLE_COLUMNS}
    with connect("warehouse") as connection:
        connection.execute("INSERT INTO etl_runs(run_id,snapshot_date,status,source_sha256,source_upper_bound) VALUES (%s,%s,'running',%s,%s) ON CONFLICT(run_id) DO UPDATE SET status='running', error=NULL",
                           (extract["run_id"], snapshot_date, extract["source_sha256"], extract["source_upper_bound"]))
    try:
        spark = spark_session()
        upper = int(extract["source_upper_bound"])
        query = f"(SELECT * FROM raw_customers WHERE ingest_id <= {upper}) AS source_snapshot"
        raw = (spark.read.format("jdbc").options(**jdbc("source"))
               .option("dbtable", query).option("partitionColumn", "ingest_id")
               .option("lowerBound", 1).option("upperBound", upper + 1)
               .option("numPartitions", 2).option("fetchsize", 10000).load().cache())
        frames, counts = transform(raw, snapshot_date)
        if counts["raw"] != extract["raw_count"] or counts["clean"] != extract["expected_clean_rows"]:
            raise ValueError("Source snapshot or accepted-customer count changed unexpectedly")
        for target, frame in frames.items():
            (frame.write.format("jdbc").options(**jdbc("warehouse"))
             .option("dbtable", "staging." + stage_tables[target]).option("batchsize", 5000)
             .option("numPartitions", 2).mode("overwrite").save())
        with connect("warehouse") as connection:
            publish(connection, stage_tables, snapshot_date, counts, extract["run_id"], started, fail_before_commit)
        return {**extract, "counts": counts, "spark_version": spark.version}
    except Exception as error:
        with connect("warehouse") as connection:
            connection.execute("UPDATE etl_runs SET status='failed', error=%s, finished_at=now(), duration_seconds=%s WHERE run_id=%s",
                               (str(error)[:2000], time.perf_counter() - started, extract["run_id"]))
        raise
    finally:
        if spark is not None:
            spark.stop()
        with connect("warehouse") as connection:
            for table in stage_tables.values():
                connection.execute(sql.SQL("DROP TABLE IF EXISTS staging.{}").format(sql.Identifier(table)))


def validate_snapshot(snapshot_date):
    snapshot_date = snapshot_iso(snapshot_date)
    with connect("warehouse") as connection:
        return verify_in_connection(connection, snapshot_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--fail-before-commit", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run_batch(extract_bound(args.snapshot_date), args.fail_before_commit), indent=2))
