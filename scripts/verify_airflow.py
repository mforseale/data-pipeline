"""Trigger a real persisted DAG run and retain its task states without exposing tokens."""

import argparse
from datetime import datetime, timezone
import json
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

from pipeline.db import ROOT
from pipeline.job import snapshot_iso


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date", required=True)
    args = parser.parse_args()
    snapshot_date = snapshot_iso(args.snapshot_date)
    base, token = "http://127.0.0.1:8080", None
    def api(method, route, payload=None):
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = "Bearer " + token
        request = Request(base + route, headers=headers, method=method,
                          data=json.dumps(payload).encode() if payload is not None else None)
        try:
            with urlopen(request, timeout=20) as response:
                return json.load(response)
        except HTTPError as error:
            raise RuntimeError(f"Airflow API {method} {route}: HTTP {error.code}: {error.read().decode()[:1500]}") from error
    token = api("GET", "/auth/token")["access_token"]
    dag_route = "/api/v2/dags/crm_daily_etl"
    deadline = time.monotonic() + 120
    while True:
        try:
            api("GET", dag_route)
            break
        except RuntimeError:
            if time.monotonic() > deadline:
                raise
            time.sleep(2)
    api("PATCH", dag_route, {"is_paused": False})
    run_id = "portfolio_check_" + uuid4().hex[:12]
    api("POST", dag_route + "/dagRuns", {"dag_run_id": run_id,
        "logical_date": datetime.now(timezone.utc).isoformat(), "conf": {"snapshot_date": snapshot_date}})
    print("Waiting for persisted Airflow DAG run:", run_id, flush=True)
    deadline = time.monotonic() + 600
    while True:
        run = api("GET", dag_route + "/dagRuns/" + run_id)
        if run["state"] in {"success", "failed"}:
            break
        if time.monotonic() > deadline:
            raise TimeoutError("Airflow run did not finish within ten minutes")
        time.sleep(2)
    response = api("GET", dag_route + "/dagRuns/" + run_id + "/taskInstances")
    tasks = [{"task_id": row["task_id"], "state": row["state"]} for row in response["task_instances"]]
    evidence_file = ROOT / "results/verification.json"
    evidence = json.loads(evidence_file.read_text(encoding="utf-8"))
    evidence["airflow_run"] = {"dag_id": "crm_daily_etl", "run_id": run_id,
                              "status": run["state"], "run_type": run.get("run_type"),
                              "schedule_enabled": not api("GET", dag_route)["is_paused"],
                              "tasks": tasks}
    evidence_file.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    assert run["state"] == "success" and len(tasks) == 4 and all(t["state"] == "success" for t in tasks), tasks
    print(json.dumps(evidence["airflow_run"], indent=2), flush=True)


if __name__ == "__main__":
    main()
