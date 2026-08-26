-- =============================================================================
-- 1. APACHE ICEBERG SILVER/GOLD PRODUCTION TABLES (ACID WAREHOUSE)
-- =============================================================================

CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.dim_customers
(
    customer_id
    string,
    company_name
    string,
    contact_name
    string,
    contact_title
    string,
    address
    string,
    city
    string,
    region
    string,
    postal_code
    string,
    country
    string,
    phone
    string,
    fax
    string,
    _ingestion_timestamp
    string
)
    LOCATION 's3://{ICEBERG_BUCKET}/dim_customers/'
    TBLPROPERTIES
(
    'table_type'=
    'ICEBERG',
    'format'=
    'parquet',
    'write_compression'=
    'zstd'
);

CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.dim_products
(
    product_id
    int,
    product_name
    string,
    supplier_id
    int,
    category_id
    int,
    quantity_per_unit
    string,
    unit_price
    double,
    units_in_stock
    int,
    units_on_order
    int,
    reorder_level
    int,
    discontinued
    boolean,
    _ingestion_timestamp
    string
)
    LOCATION 's3://{ICEBERG_BUCKET}/dim_products/'
    TBLPROPERTIES
(
    'table_type'=
    'ICEBERG',
    'format'=
    'parquet',
    'write_compression'=
    'zstd'
);

CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.dim_employees
(
    employee_id
    int,
    last_name
    string,
    first_name
    string,
    title
    string,
    title_of_courtesy
    string,
    birth_date
    string,
    hire_date
    string,
    address
    string,
    city
    string,
    region
    string,
    postal_code
    string,
    country
    string,
    home_phone
    string,
    extension
    string,
    notes
    string,
    reports_to
    int,
    _ingestion_timestamp
    string
)
    LOCATION 's3://{ICEBERG_BUCKET}/dim_employees/'
    TBLPROPERTIES
(
    'table_type'=
    'ICEBERG',
    'format'=
    'parquet',
    'write_compression'=
    'zstd'
);

CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.dim_suppliers
(
    supplier_id
    int,
    company_name
    string,
    contact_name
    string,
    contact_title
    string,
    address
    string,
    city
    string,
    region
    string,
    postal_code
    string,
    country
    string,
    phone
    string,
    fax
    string,
    home_page
    string,
    _ingestion_timestamp
    string
)
    LOCATION 's3://{ICEBERG_BUCKET}/dim_suppliers/'
    TBLPROPERTIES
(
    'table_type'=
    'ICEBERG',
    'format'=
    'parquet',
    'write_compression'=
    'zstd'
);

CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.fact_order_details
(
    order_id
    int,
    product_id
    int,
    customer_id
    string,
    employee_id
    int,
    order_date
    timestamp,
    required_date
    string,
    shipped_date
    string,
    ship_via
    int,
    freight
    double,
    ship_name
    string,
    ship_address
    string,
    ship_city
    string,
    ship_region
    string,
    ship_postal_code
    string,
    ship_country
    string,
    unit_price
    double,
    quantity
    bigint,
    discount
    double,
    net_amount
    double,
    _ingestion_timestamp
    string
)
    PARTITIONED BY
(
    year
(
    order_date
))
    LOCATION 's3://{ICEBERG_BUCKET}/fact_order_details/'
    TBLPROPERTIES
(
    'table_type'=
    'ICEBERG',
    'format'=
    'parquet',
    'write_compression'=
    'zstd'
);

-- =============================================================================
-- 2. BRONZE STAGING TABLES (ATHENA PARTITION PROJECTION - ZERO FULL SCAN)
-- =============================================================================

CREATE
EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.stg_raw_customers (
    CustomerID string,
    CompanyName string,
    ContactName string,
    ContactTitle string,
    Address string,
    City string,
    Region string,
    PostalCode string,
    Country string,
    Phone string,
    Fax string,
    `_ingestion_timestamp` string
)
PARTITIONED BY (year string, month string)
STORED AS PARQUET
LOCATION 's3://{RAW_BUCKET}/raw/customers/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.year.type'='integer',
    'projection.year.range'='2020,2030',
    'projection.month.type'='integer',
    'projection.month.range'='1,12',
    'projection.month.digits'='2',
    'storage.location.template'='s3://{RAW_BUCKET}/raw/customers/year=${year}/month=${month}'
);

CREATE
EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.stg_raw_products (
    ProductID int,
    ProductName string,
    SupplierID int,
    CategoryID int,
    QuantityPerUnit string,
    UnitPrice double,
    UnitsInStock int,
    UnitsOnOrder int,
    ReorderLevel int,
    Discontinued boolean,
    `_ingestion_timestamp` string
)
PARTITIONED BY (year string, month string)
STORED AS PARQUET
LOCATION 's3://{RAW_BUCKET}/raw/products/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.year.type'='integer',
    'projection.year.range'='2020,2030',
    'projection.month.type'='integer',
    'projection.month.range'='1,12',
    'projection.month.digits'='2',
    'storage.location.template'='s3://{RAW_BUCKET}/raw/products/year=${year}/month=${month}'
);

CREATE
EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.stg_raw_employees (
    EmployeeID int,
    LastName string,
    FirstName string,
    Title string,
    TitleOfCourtesy string,
    BirthDate string,
    HireDate string,
    Address string,
    City string,
    Region string,
    PostalCode string,
    Country string,
    HomePhone string,
    Extension string,
    Notes string,
    ReportsTo int,
    `_ingestion_timestamp` string
)
PARTITIONED BY (year string, month string)
STORED AS PARQUET
LOCATION 's3://{RAW_BUCKET}/raw/employees/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.year.type'='integer',
    'projection.year.range'='2020,2030',
    'projection.month.type'='integer',
    'projection.month.range'='1,12',
    'projection.month.digits'='2',
    'storage.location.template'='s3://{RAW_BUCKET}/raw/employees/year=${year}/month=${month}'
);

CREATE
EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.stg_raw_suppliers (
    SupplierID int,
    CompanyName string,
    ContactName string,
    ContactTitle string,
    Address string,
    City string,
    Region string,
    PostalCode string,
    Country string,
    Phone string,
    Fax string,
    HomePage string,
    `_ingestion_timestamp` string
)
PARTITIONED BY (year string, month string)
STORED AS PARQUET
LOCATION 's3://{RAW_BUCKET}/raw/suppliers/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.year.type'='integer',
    'projection.year.range'='2020,2030',
    'projection.month.type'='integer',
    'projection.month.range'='1,12',
    'projection.month.digits'='2',
    'storage.location.template'='s3://{RAW_BUCKET}/raw/suppliers/year=${year}/month=${month}'
);

CREATE
EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.stg_raw_orders (
    OrderID int,
    ProductID int,
    CustomerID string,
    EmployeeID int,
    OrderDate string,
    RequiredDate string,
    ShippedDate string,
    ShipVia int,
    Freight double,
    ShipName string,
    ShipAddress string,
    ShipCity string,
    ShipRegion string,
    ShipPostalCode string,
    ShipCountry string,
    UnitPrice double,
    Quantity bigint,
    Discount double,
    `_ingestion_timestamp` string
)
PARTITIONED BY (year string, month string)
STORED AS PARQUET
LOCATION 's3://{RAW_BUCKET}/raw/orders/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.year.type'='integer',
    'projection.year.range'='2020,2030',
    'projection.month.type'='integer',
    'projection.month.range'='1,12',
    'projection.month.digits'='2',
    'storage.location.template'='s3://{RAW_BUCKET}/raw/orders/year=${year}/month=${month}'
);