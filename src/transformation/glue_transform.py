import logging
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

# 1. Konfigurasi Logging Terstruktur
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("EnterpriseGlueTransformation")

# 2. Ambil Parameter Job
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RAW_BUCKET_NAME",
        "ICEBERG_BUCKET_NAME",
        "GLUE_DATABASE",
        "ENTITY_NAME",
    ],
)

PARTITION_PATH = (
    sys.argv[sys.argv.index("--PARTITION_PATH") + 1]
    if "--PARTITION_PATH" in sys.argv
    else "*/*"
)

# 3. Inisialisasi SparkSession dengan Ekstensi Apache Iceberg
spark = (
    SparkSession.builder.config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config(
        "spark.sql.catalog.glue_catalog.warehouse",
        f"s3://{args['ICEBERG_BUCKET_NAME']}/",
    )
    .getOrCreate()
)

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_BUCKET = args["RAW_BUCKET_NAME"]
DATABASE = args["GLUE_DATABASE"]
ENTITY_NAME = args["ENTITY_NAME"].strip().lower()

logger.info(
    f"Memulai PySpark Micro-Task. Entitas: {ENTITY_NAME}, Database: {DATABASE}, Partisi: {PARTITION_PATH}"
)


def read_raw_parquet_safely(raw_path: str, spark_session: SparkSession):
    """
    Membaca file Parquet dengan perlindungan Zero-Data Runs:
    Jika partisi S3 belum ada atau kosong, keluar dengan status SUKSES tanpa memicu alarm SRE palsu.
    """
    try:
        logger.info(f"Membaca partisi raw: {raw_path}")
        return spark_session.read.parquet(raw_path)
    except AnalysisException as e:
        err_msg = str(e)
        if "Path does not exist" in err_msg or (
            hasattr(e, "java_exception")
            and "Path does not exist" in str(e.java_exception)
        ):
            logger.info(
                f"NORMAL ZERO-DATA EXIT: Tidak ada file data baru di {raw_path}. Melewati pemrosesan secara sukses."
            )
            job.commit()
            sys.exit(0)
        raise e


def ensure_schema_evolution_atomic(
    target_table: str, staging_df, spark_session: SparkSession
):
    """Batch Atomic Schema Evolution dengan backtick protection."""
    try:
        target_schema = spark_session.table(target_table).schema
        existing_cols = {f.name.lower() for f in target_schema.fields}

        new_columns = []
        for field in staging_df.schema.fields:
            if field.name.lower() not in existing_cols:
                new_columns.append(
                    f"`{field.name}` {field.dataType.simpleString()}"
                )

        if new_columns:
            cols_ddl = ", ".join(new_columns)
            logger.info(
                f"BATCH SCHEMA EVOLUTION: Menambahkan {len(new_columns)} kolom baru ke {target_table}: {cols_ddl}"
            )
            spark_session.sql(f"ALTER TABLE {target_table} ADD COLUMNS ({cols_ddl})")
    except Exception as e:
        logger.warning(
            f"Peringatan evaluasi schema evolution untuk {target_table}: {str(e)}"
        )


# ==============================================================================
# 4. EKSEKUSI TRANSFORMASI TERISOLASI PER ENTITAS
# ==============================================================================

try:
    if ENTITY_NAME == "orders":
        spark.sql(
            f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{DATABASE}.fact_order_details (
            OrderID INT,
            ProductID INT,
            CustomerID STRING,
            EmployeeID INT,
            OrderDate TIMESTAMP,
            RequiredDate TIMESTAMP,
            ShippedDate TIMESTAMP,
            ShipVia INT,
            Freight DOUBLE,
            ShipName STRING,
            ShipAddress STRING,
            ShipCity STRING,
            ShipRegion STRING,
            ShipPostalCode STRING,
            ShipCountry STRING,
            UnitPrice DOUBLE,
            Quantity INT,
            Discount DOUBLE,
            NetAmount DOUBLE,
            _ingestion_timestamp STRING,
            _processed_timestamp TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (year(OrderDate))
        TBLPROPERTIES (
            'format-version'='2',
            'write.parquet.compression-codec'='zstd'
        )
        """
        )

        raw_path = f"s3://{RAW_BUCKET}/raw/orders/{PARTITION_PATH}/*.parquet"
        df_raw = read_raw_parquet_safely(raw_path, spark)

        df_silver = (
            df_raw.withColumn("OrderID", col("OrderID").cast("int"))
            .withColumn("ProductID", col("ProductID").cast("int"))
            .withColumn("EmployeeID", col("EmployeeID").cast("int"))
            .withColumn("ShipVia", col("ShipVia").cast("int"))
            .withColumn("UnitPrice", col("UnitPrice").cast("double"))
            .withColumn("Quantity", col("Quantity").cast("int"))
            .withColumn("Discount", col("Discount").cast("double"))
            .withColumn("Freight", col("Freight").cast("double"))
            .withColumn("OrderDate", col("OrderDate").cast("timestamp"))
            .withColumn("RequiredDate", col("RequiredDate").cast("timestamp"))
            .withColumn("ShippedDate", col("ShippedDate").cast("timestamp"))
            .withColumn(
                "NetAmount",
                (
                    col("UnitPrice")
                    * col("Quantity")
                    * (1.0 - col("Discount"))
                ).cast("double"),
            )
            .withColumn("_processed_timestamp", current_timestamp())
        )

        window_spec = Window.partitionBy("OrderID", "ProductID").orderBy(
            col("_ingestion_timestamp").desc()
        )
        df_dedup = (
            df_silver.withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        target_table = f"glue_catalog.{DATABASE}.fact_order_details"
        ensure_schema_evolution_atomic(target_table, df_dedup, spark)
        df_dedup.createOrReplaceTempView("staging_orders")

        spark.sql(
            f"""
        MERGE INTO {target_table} target
        USING staging_orders source
        ON target.OrderID = source.OrderID AND target.ProductID = source.ProductID
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        )

    elif ENTITY_NAME == "customers":
        spark.sql(
            f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{DATABASE}.dim_customers (
            CustomerID STRING,
            CompanyName STRING,
            ContactName STRING,
            ContactTitle STRING,
            Address STRING,
            City STRING,
            Region STRING,
            PostalCode STRING,
            Country STRING,
            Phone STRING,
            Fax STRING,
            _ingestion_timestamp STRING,
            _processed_timestamp TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
        """
        )

        raw_path = f"s3://{RAW_BUCKET}/raw/customers/{PARTITION_PATH}/*.parquet"
        df_raw = read_raw_parquet_safely(raw_path, spark)
        df_silver = df_raw.withColumn(
            "_processed_timestamp", current_timestamp()
        )
        window_spec = Window.partitionBy("CustomerID").orderBy(
            col("_ingestion_timestamp").desc()
        )
        df_dedup = (
            df_silver.withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        target_table = f"glue_catalog.{DATABASE}.dim_customers"
        ensure_schema_evolution_atomic(target_table, df_dedup, spark)
        df_dedup.createOrReplaceTempView("staging_customers")

        spark.sql(
            f"""
        MERGE INTO {target_table} target
        USING staging_customers source
        ON target.CustomerID = source.CustomerID
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        )

    elif ENTITY_NAME == "products":
        spark.sql(
            f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{DATABASE}.dim_products (
            ProductID INT,
            ProductName STRING,
            SupplierID INT,
            CategoryID INT,
            QuantityPerUnit STRING,
            UnitPrice DOUBLE,
            UnitsInStock INT,
            UnitsOnOrder INT,
            ReorderLevel INT,
            Discontinued BOOLEAN,
            _ingestion_timestamp STRING,
            _processed_timestamp TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
        """
        )

        raw_path = f"s3://{RAW_BUCKET}/raw/products/{PARTITION_PATH}/*.parquet"
        df_raw = read_raw_parquet_safely(raw_path, spark)
        df_silver = (
            df_raw.withColumn("ProductID", col("ProductID").cast("int"))
            .withColumn("SupplierID", col("SupplierID").cast("int"))
            .withColumn("CategoryID", col("CategoryID").cast("int"))
            .withColumn("UnitPrice", col("UnitPrice").cast("double"))
            .withColumn("UnitsInStock", col("UnitsInStock").cast("int"))
            .withColumn("UnitsOnOrder", col("UnitsOnOrder").cast("int"))
            .withColumn("ReorderLevel", col("ReorderLevel").cast("int"))
            .withColumn("Discontinued", col("Discontinued").cast("boolean"))
            .withColumn("_processed_timestamp", current_timestamp())
        )
        window_spec = Window.partitionBy("ProductID").orderBy(
            col("_ingestion_timestamp").desc()
        )
        df_dedup = (
            df_silver.withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        target_table = f"glue_catalog.{DATABASE}.dim_products"
        ensure_schema_evolution_atomic(target_table, df_dedup, spark)
        df_dedup.createOrReplaceTempView("staging_products")

        spark.sql(
            f"""
        MERGE INTO {target_table} target
        USING staging_products source
        ON target.ProductID = source.ProductID
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        )

    elif ENTITY_NAME == "employees":
        spark.sql(
            f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{DATABASE}.dim_employees (
            EmployeeID INT,
            LastName STRING,
            FirstName STRING,
            Title STRING,
            TitleOfCourtesy STRING,
            BirthDate TIMESTAMP,
            HireDate TIMESTAMP,
            Address STRING,
            City STRING,
            Region STRING,
            PostalCode STRING,
            Country STRING,
            HomePhone STRING,
            Extension STRING,
            Notes STRING,
            ReportsTo INT,
            _ingestion_timestamp STRING,
            _processed_timestamp TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
        """
        )

        raw_path = f"s3://{RAW_BUCKET}/raw/employees/{PARTITION_PATH}/*.parquet"
        df_raw = read_raw_parquet_safely(raw_path, spark)
        df_silver = (
            df_raw.withColumn("EmployeeID", col("EmployeeID").cast("int"))
            .withColumn("ReportsTo", col("ReportsTo").cast("int"))
            .withColumn("BirthDate", col("BirthDate").cast("timestamp"))
            .withColumn("HireDate", col("HireDate").cast("timestamp"))
            .withColumn("_processed_timestamp", current_timestamp())
        )
        window_spec = Window.partitionBy("EmployeeID").orderBy(
            col("_ingestion_timestamp").desc()
        )
        df_dedup = (
            df_silver.withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        target_table = f"glue_catalog.{DATABASE}.dim_employees"
        ensure_schema_evolution_atomic(target_table, df_dedup, spark)
        df_dedup.createOrReplaceTempView("staging_employees")

        spark.sql(
            f"""
        MERGE INTO {target_table} target
        USING staging_employees source
        ON target.EmployeeID = source.EmployeeID
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        )

    elif ENTITY_NAME == "suppliers":
        spark.sql(
            f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{DATABASE}.dim_suppliers (
            SupplierID INT,
            CompanyName STRING,
            ContactName STRING,
            ContactTitle STRING,
            Address STRING,
            City STRING,
            Region STRING,
            PostalCode STRING,
            Country STRING,
            Phone STRING,
            Fax STRING,
            HomePage STRING,
            _ingestion_timestamp STRING,
            _processed_timestamp TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
        """
        )

        raw_path = f"s3://{RAW_BUCKET}/raw/suppliers/{PARTITION_PATH}/*.parquet"
        df_raw = read_raw_parquet_safely(raw_path, spark)
        df_silver = df_raw.withColumn(
            "SupplierID", col("SupplierID").cast("int")
        ).withColumn("_processed_timestamp", current_timestamp())
        window_spec = Window.partitionBy("SupplierID").orderBy(
            col("_ingestion_timestamp").desc()
        )
        df_dedup = (
            df_silver.withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        target_table = f"glue_catalog.{DATABASE}.dim_suppliers"
        ensure_schema_evolution_atomic(target_table, df_dedup, spark)
        df_dedup.createOrReplaceTempView("staging_suppliers")

        spark.sql(
            f"""
        MERGE INTO {target_table} target
        USING staging_suppliers source
        ON target.SupplierID = source.SupplierID
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        )

    else:
        raise ValueError(
            f"Entitas '{ENTITY_NAME}' tidak terdaftar dalam arsitektur lakehouse!"
        )

    logger.info(f"Transformasi entitas {ENTITY_NAME} selesai dengan SUKSES.")

except Exception as err:
    logger.error(
        f"FATAL ERROR saat memproses entitas {ENTITY_NAME}: {str(err)}",
        exc_info=True,
    )
    raise err

job.commit()
print(f"Glue Job untuk entitas {ENTITY_NAME} selesai 100%.")