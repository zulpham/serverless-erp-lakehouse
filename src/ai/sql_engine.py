"""Serverless ERP Lakehouse - Enterprise AI Text-to-SQL Engine.

Architecture & Engineering Standards:
1. Pure Lean Engine: 100% Rust-backed Polars and PyAthena DictCursor (Zero Pandas dependency, < 40 MB footprint).
2. Global Execution Context Reuse: SDK clients, connection pools, and in-memory TTL schema caches survive across warm invocations.
3. Dynamic Schema Reflection: Queries AWS Glue Data Catalog with in-memory TTL caching (3600s) to support Apache Iceberg Schema Evolution.
4. Two-Pass LLM Strategy: Pass 1 translates user intent into Trino SQL; Pass 2 synthesizes contextual executive business insights.
5. SQLGlot AST Guardrails: Mathematically blocks DML/DDL mutations and injects LIMIT 100 bounds into the abstract syntax tree.
6. L3 Result Reuse Caching: Configures PyAthena 24-hour result caching (Zero-Cost on repeated inquiries).
7. Non-Blocking Async Mode: Supports asynchronous QueryExecutionId returns for Event-Driven WebSocket / REST APIs.
"""

from datetime import datetime, timezone
import json
import logging
import os
import pathlib
import re
import time
from typing import Any, Dict, Optional, Tuple
import boto3
import botocore.config
from botocore.exceptions import ClientError
import polars as pl
from pyathena import connect
from pyathena.cursor import DictCursor
import sqlglot
from sqlglot import exp

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("LakehouseAIEngine")

# ==============================================================================
# 1. GLOBAL SCOPE CONFIGURATION & CONNECTION POOLING
# ==============================================================================
# Initialized in Global Scope to eliminate TLS handshake storms and socket exhaustion
# during high-concurrency peak traffic (Execution Context Reuse).

REGION = os.environ.get("AWS_REGION", "ap-southeast-2")
DATABASE_NAME = os.environ.get(
    "DATABASE_NAME", "serverless_erp_lakehouse_db"
)
WORKGROUP_NAME = os.environ.get(
    "ATHENA_WORKGROUP_NAME", "serverless-erp-lakehouse-workgroup"
)
RESULTS_BUCKET = os.environ.get(
    "ATHENA_RESULTS_BUCKET_NAME",
    f"serverless-erp-lakehouse-athena-results-{REGION}-435497",
)
TITAN_MODEL_ID = "amazon.titan-text-express-v1"

# Boto3 Adaptive Retry Configuration with 50-Socket Connection Pooling
boto_config = botocore.config.Config(
    retries={"max_attempts": 5, "mode": "adaptive"},
    connect_timeout=5,
    read_timeout=15,
    max_pool_connections=50,
)

# Reusable SDK Clients in Global Scope
bedrock_client = boto3.client(
    "bedrock-runtime", region_name=REGION, config=boto_config
)
glue_client = boto3.client("glue", region_name=REGION, config=boto_config)

# Reusable Athena Connection with L3 24-Hour Result Reuse Enabled
athena_conn = connect(
    s3_staging_dir=f"s3://{RESULTS_BUCKET}/",
    work_group=WORKGROUP_NAME,
    region_name=REGION,
    schema_name=DATABASE_NAME,
    result_reuse_enable=True,
    result_reuse_minutes=1440,
)

# Global In-Memory Schema Cache: (schema_string, expiration_timestamp)
SCHEMA_CACHE: Dict[str, Any] = {"data": "", "expiry": 0.0}
SCHEMA_CACHE_TTL_SECONDS = 3600  # 1 Hour TTL


# ==============================================================================
# 2. CORE LAKEHOUSE AI ENGINE CLASS
# ==============================================================================
class LakehouseAIEngine:
    """Production Text-to-SQL Engine with Dynamic Glue Reflection, Bedrock, and Polars Lean Engine."""

    def __init__(self):
        # References global singletons to avoid re-instantiating connections on warm invocations
        self.bedrock = bedrock_client
        self.glue = glue_client
        self.conn = athena_conn

    # --------------------------------------------------------------------------
    # 2.1 DYNAMIC GLUE CATALOG SCHEMA REFLECTION (1-HOUR TTL CACHING)
    # --------------------------------------------------------------------------
    def get_dynamic_schema_context(self) -> str:
        """Dynamically inspects AWS Glue Data Catalog to generate prompt schema context.

        Uses in-memory TTL caching to prevent Glue API throttling while ensuring
        the model automatically adapts to Apache Iceberg schema evolution.
        """
        now = time.time()
        if SCHEMA_CACHE["data"] and now < SCHEMA_CACHE["expiry"]:
            return SCHEMA_CACHE["data"]  # 0 ms Latency Cache Hit

        try:
            logger.info(
                f"Refreshing dynamic schema context from Glue Catalog: {DATABASE_NAME}"
            )
            response = self.glue.get_tables(DatabaseName=DATABASE_NAME)
            tables = response.get("TableList", [])

            schema_lines = [f"DATABASE: {DATABASE_NAME}\n"]

            for tbl in tables:
                tbl_name = tbl["Name"]
                # Exclude transient Bronze staging tables from the LLM prompt view
                if tbl_name.startswith("stg_raw_"):
                    continue

                table_type = tbl.get("Parameters", {}).get(
                    "table_type", "STANDARD"
                )
                schema_lines.append(f"TABLE: {tbl_name} (Type: {table_type})")
                schema_lines.append("COLUMNS:")

                for col in tbl.get("StorageDescriptor", {}).get("Columns", []):
                    col_name = col["Name"]
                    col_type = col["Type"]
                    comment = (
                        f" - {col['Comment']}" if col.get("Comment") else ""
                    )
                    schema_lines.append(
                        f"  - {col_name} ({col_type.upper()}){comment}"
                    )

                schema_lines.append("")

            generated_schema = "\n".join(schema_lines)
            SCHEMA_CACHE["data"] = generated_schema
            SCHEMA_CACHE["expiry"] = now + SCHEMA_CACHE_TTL_SECONDS
            return generated_schema

        except Exception as e:
            logger.warning(
                f"Error querying Glue Catalog: {e}. Falling back to default schema representation."
            )
            return f"DATABASE: {DATABASE_NAME}\nTABLES: fact_order_details, dim_customers, dim_products, dim_employees, dim_suppliers"

    # --------------------------------------------------------------------------
    # 2.2 PASS-1: REAL BEDROCK LLM SQL GENERATION
    # --------------------------------------------------------------------------
    def _invoke_bedrock_llm(self, prompt: str, max_tokens: int = 800) -> str:
        """Invokes Amazon Titan Text Express model natively on Bedrock."""
        payload = {
            "inputText": prompt,
            "textGenerationConfig": {
                "maxTokenCount": max_tokens,
                "stopSequences": [],
                "temperature": 0.0,
                "topP": 1.0,
            },
        }
        response = self.bedrock.invoke_model(
            modelId=TITAN_MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(payload),
        )
        body = json.loads(response["body"].read().decode("utf-8"))
        return body["results"][0]["outputText"].strip()

    def _fallback_semantic_router(self, query: str) -> str:
        """Defensive rule-based fallback used ONLY when Bedrock API is unreachable."""
        q = query.lower()
        if "pelanggan" in q or "customer" in q or "belanja" in q:
            return f"""
            SELECT 
                c.company_name AS nama_perusahaan,
                c.contact_name AS nama_kontak,
                c.city AS kota,
                c.country AS negara,
                ROUND(SUM(f.net_amount), 2) AS total_belanja
            FROM {DATABASE_NAME}.fact_order_details f
            JOIN {DATABASE_NAME}.dim_customers c ON f.customer_id = c.customer_id
            WHERE year(f.order_date) = 1997
            GROUP BY c.company_name, c.contact_name, c.city, c.country
            ORDER BY total_belanja DESC LIMIT 5
            """
        elif "karyawan" in q or "employee" in q or "sales" in q:
            return f"""
            SELECT 
                CONCAT(e.first_name, ' ', e.last_name) AS nama_karyawan,
                e.title AS jabatan,
                ROUND(SUM(f.net_amount), 2) AS total_penjualan
            FROM {DATABASE_NAME}.fact_order_details f
            JOIN {DATABASE_NAME}.dim_employees e ON f.employee_id = e.employee_id
            GROUP BY e.first_name, e.last_name, e.title
            ORDER BY total_penjualan DESC LIMIT 5
            """
        elif "tren" in q or "bulanan" in q or "month" in q:
            return f"""
            SELECT 
                month(f.order_date) AS bulan,
                ROUND(SUM(f.net_amount), 2) AS total_revenue,
                COUNT(DISTINCT f.order_id) AS total_transaksi
            FROM {DATABASE_NAME}.fact_order_details f
            WHERE year(f.order_date) = 1997
            GROUP BY month(f.order_date)
            ORDER BY bulan ASC
            """
        elif "produk" in q or "product" in q or "terlaris" in q:
            return f"""
            SELECT 
                p.product_name AS nama_produk,
                SUM(f.quantity) AS total_kuantitas_terjual,
                ROUND(SUM(f.net_amount), 2) AS total_pendapatan
            FROM {DATABASE_NAME}.fact_order_details f
            JOIN {DATABASE_NAME}.dim_products p ON f.product_id = p.product_id
            GROUP BY p.product_name
            ORDER BY total_kuantitas_terjual DESC LIMIT 5
            """
        else:
            return f"""
            SELECT 
                f.ship_country AS negara_tujuan,
                COUNT(DISTINCT f.order_id) AS jumlah_pesanan,
                ROUND(AVG(f.discount), 4) AS rata_rata_diskon,
                ROUND(SUM(f.net_amount), 2) AS total_pendapatan
            FROM {DATABASE_NAME}.fact_order_details f
            GROUP BY f.ship_country
            ORDER BY total_pendapatan DESC LIMIT 10
            """

    def generate_sql(self, natural_language_query: str) -> str:
        """Pass 1: Translates natural language into Trino SQL using Amazon Bedrock."""
        schema_context = self.get_dynamic_schema_context()
        prompt = f"""You are a Principal Trino/Presto SQL Architect for Amazon Athena Engine v3.
Based on the live Glue Catalog Schema below, generate a single read-only Trino SQL query.

{schema_context}

Rules:
1. Generate strictly a single Trino SQL SELECT query for Amazon Athena.
2. Tables MUST be prefixed with {DATABASE_NAME}.<table_name>.
3. To filter by year on fact_order_details, use `year(order_date) = YYYY`.
4. Revenue is calculated as SUM(net_amount).
5. Only generate SELECT queries. Never generate mutations.
6. Return ONLY the raw SQL statement.

Business Question: {natural_language_query}
SQL Query:"""

        try:
            raw_text = self._invoke_bedrock_llm(prompt, max_tokens=600)
            sql_match = re.search(
                r"```(?:sql)?\s*(.*?)\s*```", raw_text, re.DOTALL
            )
            clean_sql = (
                sql_match.group(1).strip() if sql_match else raw_text.strip()
            )
            clean_sql = clean_sql.split(";")[0].strip()
            return clean_sql

        except Exception as e:
            logger.warning(
                f"Bedrock LLM unavailable ({e}). Engaging defensive fallback router..."
            )
            return self._fallback_semantic_router(natural_language_query)

    # --------------------------------------------------------------------------
    # 2.3 SQLGLOT AST SECURITY GUARDRAILS (MATHEMATICAL MUTATION BLOCKING)
    # --------------------------------------------------------------------------
    def sanitize_and_guard_sql(self, sql_query: str) -> str:
        """Enforces AST-level security guardrails using SQLGlot (Trino dialect)."""
        try:
            parsed_expressions = sqlglot.parse(sql_query, read="trino")
            if len(parsed_expressions) > 1:
                raise ValueError(
                    "Security Violation: Semicolon-chained multi-statement execution blocked!"
                )

            ast_root = parsed_expressions[0]

            # Enforce Read-Only SELECT statement
            if not isinstance(ast_root, (exp.Select, exp.Union)):
                raise ValueError(
                    f"Security Violation: Non-SELECT statement detected [{type(ast_root).__name__}]!"
                )

            # Block mutation nodes anywhere in the syntax tree
            forbidden_nodes = (
                exp.Insert,
                exp.Update,
                exp.Delete,
                exp.Drop,
                exp.Alter,
                exp.Create,
                exp.Merge,
            )
            if any(ast_root.find_all(forbidden_nodes)):
                raise ValueError(
                    "Security Violation: Mutation operations are strictly forbidden!"
                )

            # Auto-inject LIMIT 100 if outer query lacks a limit
            if not ast_root.args.get("limit"):
                logger.info("Guardrail: Auto-injecting 'LIMIT 100' to query AST")
                ast_root = ast_root.limit(100)

            return ast_root.sql(dialect="trino", pretty=True)

        except Exception as e:
            logger.error(f"SQLGlot Guardrail Violation: {e}")
            raise ValueError(f"SQL Security Guardrail Blocked Query: {e}")

    # --------------------------------------------------------------------------
    # 2.4 PURE LEAN ATHENA EXECUTION (POLARS & DICTCURSOR - ZERO PANDAS)
    # --------------------------------------------------------------------------
    def execute_query(self, sanitized_sql: str) -> pl.DataFrame:
        """Synchronously executes the query and returns a lightweight Polars DataFrame."""
        try:
            logger.info(f"Executing Query on Athena:\n{sanitized_sql}")
            cursor = self.conn.cursor(cursor=DictCursor)
            cursor.execute(sanitized_sql)
            rows = cursor.fetchall()
            if not rows:
                return pl.DataFrame()
            # Convert raw list of dicts directly into Rust-backed Polars DataFrame
            df = pl.DataFrame(rows)
            return df
        except Exception as e:
            logger.error(f"Athena execution error: {e}", exc_info=True)
            raise RuntimeError(f"Amazon Athena Execution Error: {e}")

    def execute_query_async(self, sanitized_sql: str) -> str:
        """Non-Blocking Async Mode: Returns QueryExecutionId instantly for Event-Driven APIs."""
        cursor = self.conn.cursor(cursor=DictCursor)
        query_id = cursor.execute(sanitized_sql).query_id
        logger.info(
            f"Non-Blocking Query Dispatched to Athena. QueryID: {query_id}"
        )
        return query_id

    # --------------------------------------------------------------------------
    # 2.5 PASS-2: REAL TWO-PASS LLM CONTEXTUAL SYNTHESIS (POLARS COMPLIANT)
    # --------------------------------------------------------------------------
    def summarize_insights(
            self, natural_language_query: str, df: pl.DataFrame
    ) -> str:
        """Pass 2: Feeds actual tabular results back into Bedrock to generate contextual insights."""
        if df.is_empty():
            return "Tidak ada data yang ditemukan untuk kueri bisnis ini."

        # Serialize sample data to CSV using Polars native write_csv
        data_sample = df.head(10).write_csv()

        prompt = f"""You are a Principal Business Intelligence Analyst.
Analyze the following query results extracted from the ERP Data Lakehouse and provide a sharp, concise executive summary.

User Question: {natural_language_query}
Extracted Data (Top 10 rows in CSV format):
{data_sample}

Total Rows Returned: {df.height}

Instructions:
1. Provide 3-4 bullet points in professional Indonesian.
2. Directly answer the user's inquiry with exact numerical values from the table.
3. Highlight the top performer, total volume, and strategic takeaways.
4. Keep the summary under 120 words.

Executive Summary:"""

        try:
            summary = self._invoke_bedrock_llm(prompt, max_tokens=400)
            return summary
        except Exception as e:
            logger.warning(
                f"Bedrock Pass-2 synthesis failed ({e}). Using deterministic summary..."
            )
            first_col_name = df.columns[0]
            top_entity = str(df[first_col_name][0]) if df.height > 0 else "N/A"

            # Check for numeric metrics in Polars schema
            numeric_cols = [
                col
                for col, dtype in df.schema.items()
                if dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
            ]
            if numeric_cols:
                metric_name = numeric_cols[-1]
                total_val = (
                    df[metric_name].sum()
                    if df[metric_name].is_not_null().any()
                    else 0
                )
                return f"• Berhasil mengekstrak {df.height} baris dari Apache Iceberg.\n• Entitas teratas: **{top_entity}**.\n• Akumulasi total `{metric_name}`: **${total_val:,.2f}**."
            else:
                return f"• Berhasil mengekstrak {df.height} entitas dari Apache Iceberg.\n• Entitas pertama: **{top_entity}** ({first_col_name})."

    # --------------------------------------------------------------------------
    # 2.6 COMPLETE END-TO-END TWO-PASS PIPELINE
    # --------------------------------------------------------------------------
    def run_pipeline(
            self, natural_language_query: str
    ) -> Tuple[str, str, pl.DataFrame, str]:
        """Executes the full Two-Pass Text-to-SQL pipeline with guardrails."""
        _ = self.get_dynamic_schema_context()

        raw_sql = self.generate_sql(natural_language_query)
        guarded_sql = self.sanitize_and_guard_sql(raw_sql)
        df_result = self.execute_query(guarded_sql)
        summary = self.summarize_insights(natural_language_query, df_result)

        return raw_sql, guarded_sql, df_result, summary
