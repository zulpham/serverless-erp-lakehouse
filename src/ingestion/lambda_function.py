"""Serverless ERP lakehouse - Bronze Ingestion Worker (AWS Lambda).

This module implements a pure functional, event-driven micro-task worker designed
to extract data from OData v4 endpoints, apply contract-driven schema enforcement,
flatten relational nested structures, and stream compressed Parquet files directly
into the Amazon S3 Bronze (Raw) Zone.

Key Architectural Patterns:
1. Pure Functional Worker: Stateless execution controlled via Step Functions event payloads.
2. Stateful Cursor Resumption: Emits and consumes pagination cursors to prevent infinite loops.
3. Event-Time Watermarking: Tracks true transactional event timestamps (e.g., OrderDate).
4. Memory-Driven Chunking: In-memory streaming to S3 in batches to bound RAM usage under 60 MB.
5. Idempotent S3 Key Determinism: Uses deterministic execution and chunk IDs to prevent duplicates.
"""

import io
import json
import logging
import os
from datetime import datetime, timezone
from urllib.parse import quote, urljoin
import boto3
import botocore.config
import polars as pl
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ------------------------------------------------------------------------------
# 1. Structured Logging Configuration
# ------------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ------------------------------------------------------------------------------
# 2. Global AWS SDK Client Initialization with Adaptive Retries
# ------------------------------------------------------------------------------
boto_config = botocore.config.Config(
    retries={"max_attempts": 5, "mode": "adaptive"},
    connect_timeout=10,
    read_timeout=30,
)
s3_client = boto3.client("s3", config=boto_config)
ssm_client = boto3.client("ssm", config=boto_config)

# ------------------------------------------------------------------------------
# 3. Contract-Driven Type Mapping (JSON Schema to Polars Native Types)
# ------------------------------------------------------------------------------
TYPE_MAP = {
    "String": pl.Utf8,
    "Utf8": pl.Utf8,
    "Float64": pl.Float64,
    "Float32": pl.Float32,
    "Int64": pl.Int64,
    "Int32": pl.Int32,
    "Boolean": pl.Boolean,
    "Date": pl.Date,
    "Datetime": pl.Datetime,
}

# ------------------------------------------------------------------------------
# 4. Environment Variables & FinOps Constants
# ------------------------------------------------------------------------------
RAW_BUCKET_NAME = os.environ.get("RAW_BUCKET_NAME")
ODATA_BASE_URL = os.environ.get(
    "ODATA_BASE_URL", "https://services.odata.org/v4/northwind/northwind.svc"
)
PROJECT_NAME = os.environ.get("PROJECT_NAME", "serverless-erp-lakehouse")
MAX_PAGES_LIMIT = 50
CHUNK_ROWS_LIMIT = 5000

# Timeout Buffer MUST exceed blocking HTTP timeout + I/O overhead
HTTP_REQUEST_TIMEOUT_SECONDS = 20
TIMEOUT_BUFFER_MS = (HTTP_REQUEST_TIMEOUT_SECONDS + 5) * 1000


# ------------------------------------------------------------------------------
# 5. Global Scope HTTP Session with TCP Connection Pooling
# ------------------------------------------------------------------------------
def create_global_http_session() -> requests.Session:
    """Initializes a reusable HTTP session in global scope with TCP connection pooling.

    By initializing the session outside the handler, warm Lambda invocations reuse
    existing TCP/TLS connections, eliminating DNS resolution overhead and socket exhaustion.

    Returns:
        requests.Session: Configured HTTP session with exponential backoff retry adapter.
    """
    session = requests.Session()
    retries = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(
        pool_connections=10, pool_maxsize=20, max_retries=retries
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "Connection": "keep-alive",
            "User-Agent": f"Enterprise-Lakehouse-Ingestor/{PROJECT_NAME}",
        }
    )
    return session


http_session = create_global_http_session()


def get_ssm_watermark(entity_name: str) -> str:
    """Retrieves the latest committed Event-Time watermark from AWS SSM Parameter Store.

    Follows the Fail-Fast principle: ParameterNotFound indicates a brand-new table (initial load),
    while any unexpected infrastructure or IAM error immediately raises an exception.

    Args:
        entity_name (str): Name of the entity (e.g., 'Orders', 'Customers').

    Returns:
        str: ISO8601 watermark string if found, otherwise None for initial backfill.

    Raises:
        Exception: Any unexpected AWS SSM API error.
    """
    param_name = f"/{PROJECT_NAME}/watermarks/{entity_name.lower()}"
    try:
        response = ssm_client.get_parameter(Name=param_name)
        return response["Parameter"]["Value"]
    except ssm_client.exceptions.ParameterNotFound:
        logger.info(
            json.dumps(
                {
                    "event": "WATERMARK_INITIAL_LOAD_DETECTED",
                    "entity": entity_name,
                }
            )
        )
        return None
    except Exception as err:
        logger.error(
            json.dumps(
                {
                    "event": "FAIL_FAST_SSM_ERROR",
                    "entity": entity_name,
                    "error": str(err),
                }
            )
        )
        raise err


def format_iso8601_watermark(raw_value) -> str:
    """Normalizes datetime values into a strictly valid ISO8601 UTC string for OData filters.

    Handles datetime objects, timezone-aware offsets (+/-HH:MM), and raw strings to prevent
    illegal double-timezone suffixes (e.g., '+02:00Z') that break OData API endpoints.

    Args:
        raw_value (Union[datetime, str]): Raw timestamp extracted from Polars or JSON.

    Returns:
        str: Normalized ISO8601 string formatted as 'YYYY-MM-DDTHH:MM:SSZ' or timezone-offset.
    """
    if not raw_value:
        return None

    if isinstance(raw_value, datetime):
        if raw_value.tzinfo is None:
            raw_value = raw_value.replace(tzinfo=timezone.utc)
        else:
            raw_value = raw_value.astimezone(timezone.utc)
        return raw_value.strftime("%Y-%m-%dT%H:%M:%SZ")

    val_str = str(raw_value).strip()
    if " " in val_str and "T" not in val_str:
        val_str = val_str.replace(" ", "T")

    if (
            not val_str.endswith("Z")
            and "+" not in val_str[-6:]
            and "-" not in val_str[-6:]
    ):
        val_str += "Z"

    return val_str


def upload_parquet_to_s3(df: pl.DataFrame, bucket: str, s3_key: str):
    """Streams a Polars DataFrame directly to S3 as a ZSTD-compressed Parquet file.

    Uses an in-memory BytesIO context manager to ensure deterministic RAM deallocation
    immediately after upload, preventing memory bloat in serverless execution environments.

    Args:
        df (pl.DataFrame): Polars DataFrame to write.
        bucket (str): Destination Amazon S3 bucket name.
        s3_key (str): Full S3 object key path.
    """
    with io.BytesIO() as buffer:
        df.write_parquet(buffer, compression="zstd")
        buffer.seek(0)
        file_size_bytes = buffer.getbuffer().nbytes

        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )

    logger.info(
        json.dumps(
            {
                "event": "UPLOAD_SUCCESS",
                "s3_destination": f"s3://{bucket}/{s3_key}",
                "rows_count": df.height,
                "file_size_kb": round(file_size_bytes / 1024, 2),
                "compression": "zstd",
            }
        )
    )


def transform_generic_bronze(
        raw_records: list,
        expand_column: str = None,
        primary_key: str = None,
        schema_overrides: dict = None,
) -> pl.DataFrame:
    """Transforms raw JSON records into a Bronze-compliant flat tabular Polars DataFrame.

    Algorithm:
    1. Instantiates a Polars DataFrame with full schema scanning and contract-driven type overrides.
    2. Filters out empty/null nested lists to prevent blind explosion errors.
    3. Unnests child struct fields explicitly while omitting child primary keys to prevent collisions.
    4. Injects audit technical metadata (_ingestion_timestamp).
    5. Pure EL: Strictly avoids business metric calculations (delegated to downstream Silver layer).

    Args:
        raw_records (list): List of JSON record dictionaries extracted from OData.
        expand_column (str, optional): Name of nested relation column (e.g., 'Order_Details').
        primary_key (str, optional): Parent primary key name to exclude from child struct extraction.
        schema_overrides (dict, optional): Mapping of column names to Polars Data Types.

    Returns:
        pl.DataFrame: Flattened, schema-enforced Polars DataFrame ready for S3 staging.
    """
    if not raw_records:
        return pl.DataFrame()

    df = pl.DataFrame(
        raw_records,
        strict=False,
        infer_schema_length=None,
        schema_overrides=schema_overrides,
    )

    if expand_column and expand_column in df.columns:
        df = df.filter(
            pl.col(expand_column).is_not_null()
            & (pl.col(expand_column).list.len() > 0)
        )
        if df.is_empty():
            return pl.DataFrame()

        df_exploded = df.explode(expand_column)
        column_type = df_exploded.schema.get(expand_column)

        if isinstance(column_type, pl.Struct):
            extract_exprs = [
                pl.col(expand_column).struct.field(f.name).alias(f.name)
                for f in column_type.fields
                if f.name != primary_key
            ]
            df_flattened = df_exploded.with_columns(extract_exprs).drop(
                expand_column
            )
        else:
            df_flattened = df_exploded
    else:
        df_flattened = df

    return df_flattened.with_columns(
        pl.lit(datetime.now(timezone.utc).isoformat()).alias(
            "_ingestion_timestamp"
        )
    )


def lambda_handler(event, context) -> dict:
    """Pure Functional Contract-Driven Ingestion Worker.

    Execution Workflow:
    1. Parse dynamic task parameters, schema contracts, and pagination resume tokens from event payload.
    2. Construct deterministic OData query URL with incremental filtering and cursor sorting.
    3. Iterate through pages, buffering records in memory and flushing to S3 when batch size is reached.
    4. Calculate running Event-Time maximum timestamp strictly on data written to S3.
    5. Evaluate Graceful Timeout Guard; interrupt cleanly before Lambda deadline if necessary.
    6. Return a Lean JSON payload (< 1 KB) containing state, resume URL, and metrics to Step Functions.

    Args:
        event (dict): Task payload from AWS Step Functions.
        context (LambdaContext): AWS Lambda runtime context.

    Returns:
        dict: Lean task execution summary for Step Functions state machine orchestration.
    """
    if not RAW_BUCKET_NAME:
        raise ValueError(
            "Environment variable 'RAW_BUCKET_NAME' is not configured!"
        )

    entity_name = event.get("entity_name", "Orders")
    expand_column = event.get("expand_column", None)
    watermark_column = event.get("watermark_column", "OrderDate")
    primary_key = event.get("primary_key", "OrderID")
    is_fact = event.get("is_fact", False)
    resume_url = event.get("resume_url", None)
    start_chunk_index = event.get("start_chunk_index", 0)

    raw_schema_hints = event.get("schema_overrides", {})
    schema_overrides = {
        col: TYPE_MAP[t_str]
        for col, t_str in raw_schema_hints.items()
        if t_str in TYPE_MAP
    }

    execution_id = event.get(
        "execution_id", getattr(context, "aws_request_id", "manual_run")
    )
    safe_exec_id = "".join(
        c if c.isalnum() or c in ("-", "_") else "_" for c in str(execution_id)
    )

    now = datetime.now(timezone.utc)
    partition_path = f"year={now.strftime('%Y')}/month={now.strftime('%m')}"

    watermark = event.get("watermark_in") or (
        get_ssm_watermark(entity_name) if is_fact else None
    )

    if resume_url:
        url = resume_url
    else:
        url = f"{ODATA_BASE_URL}/{entity_name}?$format=json"
        if expand_column:
            url += f"&$expand={expand_column}"
        if is_fact and watermark:
            url += f"&$filter={watermark_column} ge {quote(watermark)}&$orderby={watermark_column} asc, {primary_key} asc"
        elif is_fact:
            url += f"&$orderby={watermark_column} asc, {primary_key} asc"

    page_count = 0
    chunk_index = start_chunk_index
    chunk_records = []
    total_rows = 0
    is_partial_interrupted = False
    latest_committed_event_time = watermark
    earliest_committed_event_time = None
    next_page_cursor = None

    def flush_chunk_to_s3(records_to_flush: list):
        """Helper function: Transforms and flushes buffered records to S3, updating metrics."""
        nonlocal chunk_index, total_rows, latest_committed_event_time, earliest_committed_event_time
        if not records_to_flush:
            return

        df_chunk = transform_generic_bronze(
            records_to_flush, expand_column, primary_key, schema_overrides
        )

        if df_chunk.is_empty():
            records_to_flush.clear()
            return

        chunk_index += 1

        if (
                is_fact
                and watermark_column in df_chunk.columns
                and not df_chunk.is_empty()
        ):
            if df_chunk.schema.get(watermark_column) == pl.Utf8:
                parsed_dates = (
                    df_chunk[watermark_column].drop_nulls().str.to_datetime()
                )
                chunk_max = parsed_dates.max()
                chunk_min = parsed_dates.min()
            else:
                chunk_max = df_chunk[watermark_column].drop_nulls().max()
                chunk_min = df_chunk[watermark_column].drop_nulls().min()

            if chunk_max:
                latest_committed_event_time = format_iso8601_watermark(
                    chunk_max
                )
            if chunk_min and not earliest_committed_event_time:
                earliest_committed_event_time = format_iso8601_watermark(
                    chunk_min
                )

        s3_key = f"raw/{entity_name.lower()}/{partition_path}/{entity_name.lower()}_{safe_exec_id}_part{chunk_index:03d}.parquet"
        upload_parquet_to_s3(df_chunk, RAW_BUCKET_NAME, s3_key)

        total_rows += df_chunk.height
        records_to_flush.clear()

    try:
        while url and page_count < MAX_PAGES_LIMIT:
            if (
                    context
                    and hasattr(context, "get_remaining_time_in_millis")
                    and context.get_remaining_time_in_millis() < TIMEOUT_BUFFER_MS
            ):
                logger.warning(
                    json.dumps(
                        {
                            "event": "GRACEFUL_TIMEOUT_INTERRUPT",
                            "entity": entity_name,
                            "remaining_ms": context.get_remaining_time_in_millis(),
                            "interrupted_at_url": url,
                        }
                    )
                )
                is_partial_interrupted = True
                next_page_cursor = url
                break

            page_count += 1
            response = http_session.get(
                url, timeout=HTTP_REQUEST_TIMEOUT_SECONDS
            )
            response.raise_for_status()
            data = response.json()

            records = data.get("value", [])
            chunk_records.extend(records)

            if len(chunk_records) >= CHUNK_ROWS_LIMIT:
                flush_chunk_to_s3(chunk_records)

            raw_next_link = data.get("@odata.nextLink")
            url = urljoin(url, raw_next_link) if raw_next_link else None
            if url:
                next_page_cursor = url

        flush_chunk_to_s3(chunk_records)

        is_partial = is_partial_interrupted or (url is not None)

        return {
            "status": "SUCCESS",
            "entity": entity_name,
            "execution_id": safe_exec_id,
            "rows_ingested": total_rows,
            "next_chunk_index": chunk_index,
            "partition_path": partition_path,
            "is_partial": is_partial,
            "resume_url": next_page_cursor if is_partial else None,
            "watermark_out": latest_committed_event_time,
            "data_quality_metrics": {
                "min_event_time": earliest_committed_event_time,
                "max_event_time": latest_committed_event_time,
            },
        }

    except Exception as err:
        logger.error(
            json.dumps(
                {
                    "event": "MICRO_TASK_FAILED",
                    "entity": entity_name,
                    "execution_id": safe_exec_id,
                    "failed_at_url": url,
                    "input_payload": event,
                    "error_type": type(err).__name__,
                    "error_message": str(err),
                }
            ),
            exc_info=True,
        )
        raise err
