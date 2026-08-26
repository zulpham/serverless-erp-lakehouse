"""Serverless ERP Lakehouse - Pure SQL Template Renderer (< 2ms Execution).

Reads externalized SQL templates from disk, substitutes dynamic partition
variables (Event-Time semantics), and returns a clean SQL query string for
synchronous execution in AWS Step Functions via Athena.
"""

import os
import pathlib
import re

DATABASE_NAME = os.environ.get(
    "DATABASE_NAME", "serverless_erp_lakehouse_db"
)
SQL_DIR = pathlib.Path(__file__).parent / "sql"


def extract_partitions_from_path(partition_path: str) -> tuple[str, str]:
    """Extracts year and month strictly from the event payload partition_path."""
    year_match = re.search(r"year=(\d{4})", partition_path)
    month_match = re.search(r"month=(\d{2})", partition_path)
    year = year_match.group(1) if year_match else "2026"
    month = month_match.group(1) if month_match else "08"
    return year, month


def lambda_handler(event: dict, context=None) -> dict:
    """Renders SQL template file into a query string."""
    entity_name = event.get("entity_name", "dim_customers")
    partition_path = event.get("partition_path", "year=2026/month=08")

    year, month = extract_partitions_from_path(partition_path)

    sql_file_path = SQL_DIR / f"{entity_name}.sql"
    if not sql_file_path.exists():
        raise FileNotFoundError(
            f"SQL template not found for entity: {entity_name} at {sql_file_path}"
        )

    with open(sql_file_path, "r", encoding="utf-8") as f:
        template = f.read()

    rendered_query = template.format(
        DATABASE_NAME=DATABASE_NAME, YEAR=year, MONTH=month
    )

    return {
        "status": "SUCCESS",
        "entity_name": entity_name,
        "query_string": rendered_query.strip(),
    }
