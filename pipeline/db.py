"""Connections and schemas for this project's isolated databases."""

import os
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[1]
RAW_FIELDS = ["recency", "history_segment", "history", "mens", "womens", "zip_code",
              "newbie", "channel", "segment", "visit", "conversion", "spend"]
SOURCE_DDL = """
CREATE TABLE IF NOT EXISTS raw_customers (
  ingest_id BIGSERIAL PRIMARY KEY, customer_id TEXT,
  recency TEXT, history_segment TEXT, history TEXT, mens TEXT, womens TEXT,
  zip_code TEXT, newbie TEXT, channel TEXT, segment TEXT, visit TEXT,
  conversion TEXT, spend TEXT, is_demo BOOLEAN NOT NULL DEFAULT FALSE,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS source_manifest (
  dataset TEXT PRIMARY KEY, sha256 TEXT NOT NULL, expected_clean_rows INTEGER NOT NULL,
  synthetic_duplicates INTEGER NOT NULL, synthetic_invalid_rows INTEGER NOT NULL
);
"""
WAREHOUSE_DDL = """
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS customer_facts (
  snapshot_date DATE NOT NULL, customer_id BIGINT NOT NULL, segment TEXT NOT NULL,
  channel TEXT NOT NULL, recency INTEGER NOT NULL, history NUMERIC(18,2) NOT NULL,
  visit INTEGER NOT NULL, conversion INTEGER NOT NULL, spend NUMERIC(18,2) NOT NULL,
  source_ingest_id BIGINT NOT NULL, PRIMARY KEY (snapshot_date, customer_id)
);
CREATE TABLE IF NOT EXISTS experiment_metrics (
  snapshot_date DATE NOT NULL, segment TEXT NOT NULL, users BIGINT NOT NULL,
  buyers BIGINT NOT NULL, visitors BIGINT NOT NULL, revenue NUMERIC(20,2) NOT NULL,
  conversion_rate DOUBLE PRECISION NOT NULL, revenue_per_user DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (snapshot_date, segment)
);
CREATE TABLE IF NOT EXISTS channel_metrics (
  snapshot_date DATE NOT NULL, segment TEXT NOT NULL, channel TEXT NOT NULL,
  users BIGINT NOT NULL, buyers BIGINT NOT NULL, revenue NUMERIC(20,2) NOT NULL,
  PRIMARY KEY (snapshot_date, segment, channel)
);
CREATE TABLE IF NOT EXISTS quarantine (
  snapshot_date DATE NOT NULL, ingest_id BIGINT NOT NULL, customer_id TEXT,
  reason TEXT NOT NULL, raw_payload TEXT NOT NULL, is_demo BOOLEAN NOT NULL,
  PRIMARY KEY (snapshot_date, ingest_id)
);
CREATE TABLE IF NOT EXISTS etl_runs (
  run_id TEXT PRIMARY KEY, snapshot_date DATE NOT NULL, status TEXT NOT NULL,
  source_sha256 TEXT NOT NULL, source_upper_bound BIGINT NOT NULL,
  counts JSONB NOT NULL DEFAULT '{}', started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  finished_at TIMESTAMPTZ, duration_seconds DOUBLE PRECISION, error TEXT
);
"""


def dsn(kind):
    if kind not in {"source", "warehouse"}:
        raise ValueError("Unknown database")
    fallback = ("postgresql://source_demo:source_demo@localhost:55432/source" if kind == "source" else
                "postgresql://warehouse_demo:warehouse_demo@localhost:55433/warehouse")
    return os.environ.get(f"{kind.upper()}_DSN", fallback)


def connect(kind):
    return psycopg.connect(dsn(kind), row_factory=dict_row)


def initialize_warehouse():
    with connect("warehouse") as connection:
        connection.execute(WAREHOUSE_DDL)
