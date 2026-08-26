"""Serverless ERP Lakehouse - DDL Schema Bootstrapper.

Registers all 5 Apache Iceberg production tables and 5 Bronze Staging
Partition Projection tables into AWS Glue Data Catalog via Amazon Athena.
"""

import logging
import os
import pathlib
import time
import boto3

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("SchemaBootstrapper")

REGION = "ap-southeast-2"
DATABASE_NAME = "serverless_erp_lakehouse_db"
RAW_BUCKET = "serverless-erp-lakehouse-raw-ap-southeast-2-435497"
ICEBERG_BUCKET = "serverless-erp-lakehouse-iceberg-ap-southeast-2-435497"
RESULTS_BUCKET = "serverless-erp-lakehouse-athena-results-ap-southeast-2-435497"
WORKGROUP_NAME = "serverless-erp-lakehouse-workgroup"

athena_client = boto3.client("athena", region_name=REGION)
SQL_FILE = (
    pathlib.Path(__file__).parent / "sql" / "init_schema.sql"
)


def run_ddl_bootstrap():
    with open(SQL_FILE, "r", encoding="utf-8") as f:
        content = f.read()

    rendered = (
        content.replace("{DATABASE_NAME}", DATABASE_NAME)
        .replace("{RAW_BUCKET}", RAW_BUCKET)
        .replace("{ICEBERG_BUCKET}", ICEBERG_BUCKET)
    )

    statements = [s.strip() for s in rendered.split(";") if s.strip()]
    logger.info(
        f"Executing {len(statements)} DDL schema statements in Athena..."
    )

    for idx, stmt in enumerate(statements):
        # Extract the first non-comment line for clean logging
        first_line = [
            l for l in stmt.splitlines() if not l.strip().startswith("--")
        ][0][:60]
        response = athena_client.start_query_execution(
            QueryString=stmt,
            QueryExecutionContext={"Database": DATABASE_NAME},
            ResultConfiguration={"OutputLocation": f"s3://{RESULTS_BUCKET}/"},
            WorkGroup=WORKGROUP_NAME,
        )
        qid = response["QueryExecutionId"]

        while True:
            resp = athena_client.get_query_execution(QueryExecutionId=qid)
            status = resp["QueryExecution"]["Status"]["State"]

            if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
                if status == "SUCCEEDED":
                    logger.info(
                        f"[{idx+1:02d}/{len(statements):02d}] {first_line}... -> SUCCEEDED"
                    )
                else:
                    reason = resp["QueryExecution"]["Status"].get(
                        "StateChangeReason"
                    )
                    logger.error(
                        f"[{idx+1:02d}/{len(statements):02d}] {first_line}... -> {status}: {reason}"
                    )
                break
            time.sleep(0.5)

    logger.info("ALL DDL SCHEMA STATEMENTS COMPLETED!")


if __name__ == "__main__":
    run_ddl_bootstrap()