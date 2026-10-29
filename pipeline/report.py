"""Tables, scientific plots and a portable HTML report from the target warehouse."""

import base64
import csv
from decimal import Decimal
import html
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from pipeline.db import ROOT, connect
from pipeline.job import snapshot_iso, validate_snapshot


def write_csv(path, rows):
    if not rows:
        return
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def build_report(snapshot_date):
    snapshot_date = snapshot_iso(snapshot_date)
    reconciled = validate_snapshot(snapshot_date)
    output = ROOT / "results" / snapshot_date
    output.mkdir(parents=True, exist_ok=True)
    with connect("warehouse") as connection:
        metrics = connection.execute("SELECT * FROM experiment_metrics WHERE snapshot_date=%s ORDER BY segment", (snapshot_date,)).fetchall()
        channels = connection.execute("SELECT * FROM channel_metrics WHERE snapshot_date=%s ORDER BY segment,channel", (snapshot_date,)).fetchall()
        quarantine = connection.execute("SELECT ingest_id,customer_id,reason,is_demo FROM quarantine WHERE snapshot_date=%s ORDER BY ingest_id", (snapshot_date,)).fetchall()
        sample = connection.execute("SELECT * FROM customer_facts WHERE snapshot_date=%s ORDER BY customer_id LIMIT 50", (snapshot_date,)).fetchall()
        audit = connection.execute("SELECT * FROM etl_runs WHERE snapshot_date=%s AND status='success' ORDER BY finished_at DESC LIMIT 1", (snapshot_date,)).fetchone()
    for name, rows in [("experiment_metrics", metrics), ("channel_metrics", channels),
                       ("quarantine", quarantine), ("customer_facts_sample_50", sample)]:
        write_csv(output / f"{name}.csv", rows)
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10})
    labels = [row["segment"].replace(" E-Mail", "").replace("No", "Без письма") for row in metrics]
    fig, axes = plt.subplots(1, 3, figsize=(13, 4.6))
    for axis, values, title, unit in [
        (axes[0], [row["users"] for row in metrics], "Назначенных участников", "Покупателей"),
        (axes[1], [100 * row["conversion_rate"] for row in metrics], "Совершили покупку за 14 дней", "%"),
        (axes[2], [row["revenue_per_user"] for row in metrics], "Выручка на участника", "USD")]:
        bars = axis.bar(labels, values, color=["#176bb6", "#536b87", "#9a74bd"], width=0.6)
        axis.set(title=title, ylabel=unit)
        axis.bar_label(bars, fmt="%.0f" if axis is axes[0] else "%.2f", padding=4, fontsize=9)
        axis.set_ylim(0, max(map(float, values)) * 1.18)
        axis.grid(axis="y", alpha=0.18)
        axis.set_axisbelow(True)
    fig.suptitle("Витрина из PostgreSQL после Spark ETL: историческое окно эксперимента", y=0.99)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(output / "product_metrics.png", dpi=160)
    plt.close(fig)
    counts = audit["counts"]
    fig, axis = plt.subplots(figsize=(8.5, 3.8))
    bars = axis.barh(["Учебные технические дубли", "Учебные некорректные строки"],
                     [counts["duplicates"], counts["rejected"]], color=["#d89a35", "#c65b5b"])
    axis.bar_label(bars, padding=5)
    axis.set(title="Контроль качества: дефекты добавлены специально", xlabel="Строк", xlim=(0, max(counts["duplicates"], counts["rejected"]) + 2))
    fig.tight_layout()
    fig.savefig(output / "quality.png", dpi=150)
    plt.close(fig)
    table_rows = "\n".join(f"| {r['segment']} | {r['users']:,} | {r['buyers']:,} | {r['revenue']:.2f} | {r['conversion_rate']:.3%} | {r['revenue_per_user']:.3f} |" for r in metrics)
    dag_link = "\n![Фактическая структура DAG](../dag.png)\n" if (ROOT / "results/dag.png").exists() else ""
    text = f"""# ETL результатов CRM-эксперимента

## Что получилось

Данные прочитаны **из PostgreSQL через JDBC**, преобразованы настоящим Spark и опубликованы **в отдельную PostgreSQL**. Дата снимка: **{snapshot_date}**. Исторический датасет содержит итоговые показатели за 14 дней; эта дата обозначает загрузку, а не день покупки.

Прочитано **{counts['raw']:,}** строк. В факты принято **{counts['clean']:,}** покупателей, исключено **{counts['duplicates']}** учебных технических дублей, в карантин помещено **{counts['rejected']}** специально испорченных строк. Оригинальные 64 000 покупателей сохранены. Большие покупки и нулевые траты не удалялись.

## Таблицы для аналитика

| Исходная группа | Участников | Купили | Выручка, USD | Конверсия | Выручка на участника, USD |
| --- | ---: | ---: | ---: | ---: | ---: |
{table_rows}

Это описательные агрегаты, без новой проверки A/B-гипотез. Канальные срезы относятся к покупкам до эксперимента, а не источникам рекламного привлечения. Витрина позволяет продолжить A/B-анализ; история эксперимента не превращается в текущие банковские транзакции.

![Продуктовые метрики](product_metrics.png)

Таблицы: [группы](experiment_metrics.csv), [каналы покупок до эксперимента](channel_metrics.csv), [карантин](quarantine.csv), [образец 50 покупательских фактов](customer_facts_sample_50.csv). Полная таблица фактов хранится в целевой базе.

## Контроль качества и повторяемость

Проверяются обязательные поля используемых витриной признаков, допустимые значения, неотрицательные денежные суммы и соответствие покупки положительным тратам. Некорректные исходные строки сохраняются с причинами; после типизации выбирается последняя корректная версия технического ID покупателя.

Пять дублей и семь ошибок созданы в загрузчике для тестирования, помечены `is_demo`; они не являются заявлением о качестве оригинального архива. Совпадение всех известных признаков разных исходных покупателей не служит основанием удалять их строки.

![Учебные дефекты](quality.png)

Публикация использует уникальные staging-таблицы и одну транзакцию для всех витрин указанной даты. Проверки числа покупателей и сверки фактов с агрегатами выполняются до COMMIT. Ошибка откатывает публикацию. Повтор снимка заменяет его содержимое, не прибавляя покупателей или выручку. Другие даты сохраняются. Это полный снимок малого источника, без CDC.

Сверено в хранилище: {reconciled['users']:,} покупателей, {reconciled['buyers']:,} покупателей с покупкой, {reconciled['revenue_usd']} USD. Денежные суммы сохраняются в decimal и сверяются точно; средние в витрине хранятся с плавающей точкой.

## Airflow

DAG `crm_daily_etl`: `extract_source_bound → spark_transform_and_publish → check_warehouse → build_report`. Расписание — ежедневно в **10:00 Asia/Tomsk**, одна повторная попытка, запрет одновременных запусков DAG. Через XCom проходят счётчики и параметры, не таблицы покупателей. Переиспользуются те же Python-функции, что и в локальном CLI.
{dag_link}
Интерфейс: [Airflow на localhost](http://localhost:8085). Факт запуска и проверки повторяемости подтверждаются отдельно в [результатах проверки](../verification.json); наличие DAG-файла не подменяет успешное выполнение.

## Происхождение и аудит

[Автор исходного эксперимента — Hillstrom / MineThatData](https://blog.minethatdata.com/2008/03/minethatdata-e-mail-analytics-and-data.html). SHA-256 CSV: `{audit['source_sha256']}`. Верхняя граница извлечения: ingest_id ≤ {audit['source_upper_bound']}. Идентификатор опубликованного запуска: `{audit['run_id']}`. Время ETL в этом запуске: {audit['duration_seconds']:.2f} с; это локальное измерение, а не оценка масштабируемости кластера.

## Формулировка для анкеты

«Реализовал ETL-пайплайн PostgreSQL → Spark → PostgreSQL для аналитики CRM-эксперимента на 64 тыс. покупателей. Настроил DAG Airflow с ежедневным расписанием, повторными попытками и проверками качества. Выполнил типизацию, дедупликацию по техническому ID, карантин ошибочных строк, построение фактов и продуктовых витрин. Реализовал атомарную публикацию снимка и проверил повторный запуск без задвоения и откат при ошибке. Подготовил таблицы и графики для продуктового анализа».
"""
    (output / "report.md").write_text(text, encoding="utf-8")
    images = "".join(f'<figure><img alt="{html.escape(name)}" src="data:image/png;base64,{base64.b64encode((output/name).read_bytes()).decode()}"></figure>' for name in ["product_metrics.png", "quality.png"])
    html_rows = "".join("<tr>" + "".join(f"<td>{html.escape(value)}</td>" for value in [r["segment"], f"{r['users']:,}", f"{r['buyers']:,}", f"{r['revenue']:.2f}", f"{r['conversion_rate']:.3%}", f"{r['revenue_per_user']:.3f}"]) + "</tr>" for r in metrics)
    page = f'''<!doctype html><html lang="ru"><meta charset="utf-8"><title>ETL CRM-эксперимента</title><style>body{{font:16px/1.6 system-ui;margin:40px auto;max-width:1100px;padding:0 24px;color:#172a46}}table{{border-collapse:collapse;width:100%;font-size:14px}}th,td{{padding:10px;border-bottom:1px solid #dbe2ea;text-align:left}}img{{width:100%;height:auto}}.lead{{background:#eef5fc;padding:20px;border-radius:10px}}code{{overflow-wrap:anywhere}}small{{color:#536b87}}</style><h1>ETL результатов CRM-эксперимента</h1><p class="lead">Снимок {snapshot_date}: {counts['raw']:,} исходных строк → {counts['clean']:,} покупательских фактов. {counts['duplicates']} учебных дублей, {counts['rejected']} учебных некорректных строк.</p><p>PostgreSQL → Spark SQL → транзакционная публикация в PostgreSQL → Airflow → отчёт. Данные исторические; дата снимка не является датой покупки.</p><table><thead><tr><th>Группа</th><th>Участники</th><th>Купили</th><th>Выручка, USD</th><th>Конверсия</th><th>Выручка / участник</th></tr></thead><tbody>{html_rows}</tbody></table>{images}<p>Расписание Airflow: ежедневно в 10:00 Asia/Tomsk. Повтор публикации не увеличивает выручку и число покупателей; ошибка до COMMIT сохраняет прежний снимок. Полные детали и текст для анкеты — в report.md.</p><small>Hillstrom / MineThatData, исторический эксперимент 2008 года. SHA-256: <code>{audit['source_sha256']}</code></small></html>'''
    (output / "report.html").write_text(page, encoding="utf-8")
    (ROOT / "results/index.md").write_text(f"# Результаты ETL\n\nПоследний сформированный снимок: {snapshot_date}.\n\n- [Отчёт с графиками]({snapshot_date}/report.md)\n- [HTML для просмотра и отправки]({snapshot_date}/report.html)\n- [Проверки и доказательства](verification.json)\n", encoding="utf-8")
    return {"snapshot_date": snapshot_date, "report_md": str(output / "report.md"), "report_html": str(output / "report.html")}
