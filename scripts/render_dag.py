"""Render the actual imported Airflow DAG with system Graphviz."""
from pathlib import Path
import subprocess

from dags.crm_daily_etl import dag

root = Path(__file__).resolve().parents[1]
lines = ['digraph crm { rankdir=LR; node [shape=box, style="rounded,filled", fillcolor="#eef5fc", fontname="Helvetica"];']
for task in dag.tasks:
    lines.append(f'"{task.task_id}";')
    for downstream in sorted(task.downstream_task_ids):
        lines.append(f'"{task.task_id}" -> "{downstream}";')
lines.append('}')
(root / "results").mkdir(exist_ok=True)
subprocess.run(["dot", "-Tpng", "-o", str(root / "results/dag.png")], input="\n".join(lines), text=True, check=True)
print("Rendered DAG:", dag.dag_id, len(dag.tasks), "tasks")
