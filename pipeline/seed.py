"""Initialize the operational source once; preserve its original customer rows."""

import csv
import hashlib
import json

from pipeline.db import ROOT, RAW_FIELDS, SOURCE_DDL, connect, initialize_warehouse


def load_rows():
    raw_file = ROOT / "data/raw/hillstrom.csv"
    metadata = json.loads((ROOT / "data/raw/source.json").read_text(encoding="utf-8"))
    if hashlib.sha256(raw_file.read_bytes()).hexdigest() != metadata["sha256"]:
        raise ValueError("Source checksum changed")
    with raw_file.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    if len(rows) != 64000 or set(rows[0]) != set(RAW_FIELDS):
        raise ValueError("Unexpected original dataset schema or row count")
    records = [{"customer_id": str(index), **row, "is_demo": False} for index, row in enumerate(rows)]
    # Duplicates are known technical IDs, not matching customer attributes.
    duplicates = [{**row, "is_demo": True} for row in records[:5]]
    invalid = []
    defects = [{"customer_id": ""}, {"spend": "-5"}, {"spend": "not-a-number"},
               {"visit": "2"}, {"segment": "Unknown"},
               {"conversion": "1", "spend": "0"}, {"recency": "13"}]
    for index, defect in enumerate(defects):
        invalid.append({**records[0], "customer_id": str(64000 + index), "is_demo": True, **defect})
    return records + duplicates + invalid, metadata


def seed():
    records, metadata = load_rows()
    with connect("source") as connection:
        connection.execute(SOURCE_DDL)
        previous = connection.execute("SELECT * FROM source_manifest WHERE dataset='hillstrom'").fetchone()
        if previous:
            if previous["sha256"] != metadata["sha256"]:
                raise ValueError("Existing source uses another snapshot; refusing to replace it")
            print("Source already initialized; no rows inserted")
        else:
            if connection.execute("SELECT COUNT(*) AS n FROM raw_customers").fetchone()["n"]:
                raise ValueError("Unregistered source rows exist; refusing to overwrite them")
            columns = ["customer_id", *RAW_FIELDS, "is_demo"]
            with connection.cursor().copy("COPY raw_customers (" + ",".join(columns) + ") FROM STDIN") as copy:
                for row in records:
                    copy.write_row([row[column] for column in columns])
            connection.execute("INSERT INTO source_manifest VALUES ('hillstrom', %s, 64000, 5, 7)", (metadata["sha256"],))
            print(f"Source initialized: {len(records)} rows = 64000 original + 5 demo duplicates + 7 demo invalid")
    initialize_warehouse()


if __name__ == "__main__":
    seed()
