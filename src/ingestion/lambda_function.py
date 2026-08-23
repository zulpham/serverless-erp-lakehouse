import io
import json
import logging
import os
import time
import boto3
import polars as pl
import requests

from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS SDK S3 Client
s3_client = boto3.client('s3')

# Configure Environment Variables
RAW_BUCKET_NAME = os.environ.get('RAW_BUCKET_NAME')
ODATA_BASE_URL = os.environ.get(
    'ODATA_BASE_URL', "https://services.odata.org/v4/northwind/northwind.svc"
)
MAX_PAGES_LIMIT = 50  # Safeguard against OData infinite loop


def get_http_session_with_retries() -> requests.Session:
    """Create an HTTP Session with automatic retry capability if a network blip occurs"""
    session = requests.Session()
    retries = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_odata_records(
        session: requests.Session, entity_name: str, expand: str = None
) -> list:
    """Pulls all data from OData entities with pagination traversal (@odata.nextLink)"""
    url = f"{ODATA_BASE_URL}/{entity_name}?$format=json"
    if expand:
        url += f"&$expand={expand}"

    all_records = []
    page_count = 0
    start_fetch_time = time.time()

    logger.info(
        json.dumps(
            {
                "event": "FETCH_START",
                "entity": entity_name,
                "target_url": url
            }
        )
    )

    while url and page_count < MAX_PAGES_LIMIT:
        page_count += 1
        page_start = time.time()

        response = session.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()

        records = data.get("value", [])
        all_records.extend(records)

        page_duration_ms = round((time.time() - page_start) * 1000, 2)
        logger.info(
            json.dumps(
                {
                    "event": "PAGE_FETCHED",
                    "entity": entity_name,
                    "page_index": page_count,
                    "records_on_page": len(records),
                    "total_accumulated": len(all_records),
                    "duration_ms": page_duration_ms,
                }
            )
        )

        # Retrieve pagination links if the server divides the data into multiple pages
        url = data.get("@odata.nextLink")

    total_duration_ms = round((time.time() - start_fetch_time) * 1000, 2)
    logger.info(
        json.dumps(
            {
                "event": "FETCH_COMPLETE",
                "entity": entity_name,
                "total_pages": page_count,
                "total_records": len(all_records),
                "total_duration_ms": total_duration_ms
            }
        )
    )
    return all_records


def transform_and_flatten_orders(orders_raw: list) -> pl.DataFrame:
    """Flattening the nested structure Orders -> Order_Details and calculating financial metrics"""
    if not orders_raw:
        logger.warning(
            json.dumps(
                {
                    "event": "TRANSFORM_EMPTY_WARNING",
                    "entity": "Orders"
                }
            )
        )
        return pl.DataFrame()

    df = pl.DataFrame(orders_raw)

    # Explode the nested array Order_Details then unnest its fields into flat table columns
    df_flattened = df.explode("Order_Details").unnest("Order_Details")

    # Add NetAmount and IngestionTimestamp calculations for data auditing
    df_final = df_flattened.with_columns(
        [
            (
                    pl.col("UnitPrice").fill_null(0.0)
                    * pl.col("Quantity").fill_null(0)
                    * (1.0 - pl.col("Discount").fill_null(0.0))
            )
            .cast(pl.Float64)
            .alias("NetAmount"),
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("IngestionTimestamp"),
        ]
    )
    return df_final


def transform_flat_entity(raw_records: list, entity_name: str) -> pl.DataFrame:
    """Standard transformations for flat table dimension entities (Customers, Products, etc.)"""
    if not raw_records:
        logger.warning(
            json.dumps(
                {
                    "event": "TRANSFORM_EMPTY_WARNING",
                    "entity": entity_name,
                }
            )
        )
        return pl.DataFrame()

    df = pl.DataFrame(raw_records)
    return df.with_columns(
        pl.lit(datetime.now(timezone.utc).isoformat()).alias("IngestionTimestamp")
    )


def upload_parquet_to_s3(df: pl.DataFrame, bucket: str, s3_key: str):
    """Writes Polars DataFrame directly to S3 in Parquet format via in-memory RAM buffer"""
    if df.is_empty():
        logger.warning(
            json.dumps(
                {
                    "event": "UPLOAD_SKIPPED_EMPTY_DF",
                    "s3_key": s3_key,
                }
            )
        )
        return

    buffer = io.BytesIO()
    # Uses Polars native Rust Parquet engine
    df.write_parquet(buffer, compression="snappy")
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
            }
        )
    )


def lambda_handler(event, context):
    """AWS Lambda execution entry point called by AWS Step Functions"""
    if not RAW_BUCKET_NAME:
        error_msg = "Environment variable 'RAW_BUCKET_NAME' has not been configured on Lambda!"
        logger.error(
            json.dumps(
                {
                    "event": "CONFIG_ERROR",
                    "error": error_msg,
                }
            )
        )
        raise ValueError(error_msg)

    session = get_http_session_with_retries()
    now = datetime.now(timezone.utc)
    partition_path = f"year={now.strftime('%Y')}/month={now.strftime('%m')}"
    timestamp_suffix = now.strftime("%Y%m%d_%H%M%S")

    ingested_summary = {}

    try:
        # Ingest Fact Entity: Orders (along with the Order_Details details)
        orders_raw = fetch_odata_records(
            session, "Orders", expand="Order_Details"
        )
        df_orders = transform_and_flatten_orders(orders_raw)
        orders_key = f"raw/orders/{partition_path}/orders_{timestamp_suffix}.parquet"
        upload_parquet_to_s3(df_orders, RAW_BUCKET_NAME, orders_key)

        ingested_summary["orders"] = {
            "rows": df_orders.height,
            "s3_path": f"s3://{RAW_BUCKET_NAME}/{orders_key}",
        }

        # Ingest Dimension Entities: Customers, Products, Employees, Suppliers
        dimension_entities = [
            "Customers",
            "Products",
            "Employees",
            "Suppliers",
        ]
        for entity in dimension_entities:
            records = fetch_odata_records(session, entity)
            df_dim = transform_flat_entity(records, entity)
            dim_key = f"raw/{entity.lower()}/{partition_path}/{entity.lower()}_{timestamp_suffix}.parquet"
            upload_parquet_to_s3(df_dim, RAW_BUCKET_NAME, dim_key)

            ingested_summary[entity.lower()] = {
                "rows": df_dim.height,
                "s3_path": f"s3://{RAW_BUCKET_NAME}/{dim_key}",
            }

        # Clean payload output format for consumption by AWS Step Functions
        return {
            "status": "SUCCESS",
            "execution_time": now.isoformat(),
            "raw_bucket": RAW_BUCKET_NAME,
            "ingested_entities": ingested_summary,
        }

    except Exception as err:
        logger.error(
            json.dumps(
                {
                    "event": "FATAL_INGESTION ERROR",
                    "error_type": type(err).__name__,
                    "error_message": str(err)
                }
            ),
            exc_info=True
        )
        raise err
