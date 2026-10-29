"""Portable checks; deliberately do not claim to execute Spark or Airflow."""

import ast
import csv
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
metadata = json.loads((root / "data/raw/source.json").read_text(encoding="utf-8"))
raw_file = root / "data/raw/hillstrom.csv"
digest = hashlib.sha256(raw_file.read_bytes()).hexdigest()
assert digest == metadata["sha256"]
with raw_file.open(encoding="utf-8", newline="") as stream:
    rows = list(csv.DictReader(stream))
assert len(rows) == 64000
python_files = [*root.glob("pipeline/*.py"), *root.glob("dags/*.py"),
                *root.glob("scripts/*.py"), *root.glob("tests/*.py")]
for file in python_files:
    ast.parse(file.read_text(encoding="utf-8"), filename=str(file))
summary = {"status": "preflight_only", "source_sha256_verified": True,
           "source_rows": len(rows), "python_syntax_files": len(python_files),
           "spark_executed": False, "airflow_dag_executed": False,
           "limitation": "These are static checks, not an ETL run or integration tests."}
(root / "results/preflight.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
