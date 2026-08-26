MERGE INTO {DATABASE_NAME}.dim_customers target
    USING (
    SELECT * FROM (
    SELECT
    CustomerID AS customer_id,
    CompanyName AS company_name,
    ContactName AS contact_name,
    ContactTitle AS contact_title,
    Address AS address,
    City AS city,
    Region AS region,
    PostalCode AS postal_code,
    Country AS country,
    Phone AS phone,
    Fax AS fax,
    _ingestion_timestamp,
    ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY _ingestion_timestamp DESC) AS rn
    FROM {DATABASE_NAME}.stg_raw_customers
    WHERE CustomerID IS NOT NULL
    AND year = '{YEAR}' AND month = '{MONTH}'
    ) WHERE rn = 1
    ) source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN
UPDATE SET
    company_name = source.company_name,
    contact_name = source.contact_name,
    contact_title = source.contact_title,
    address = source.address,
    city = source.city,
    region = source.region,
    postal_code = source.postal_code,
    country = source.country,
    phone = source.phone,
    fax = source.fax,
    _ingestion_timestamp = source._ingestion_timestamp
    WHEN NOT MATCHED THEN
INSERT
(
customer_id
,
company_name
,
contact_name
,
contact_title
,
address
,
city
,
region
,
postal_code
,
country
,
phone
,
fax
,
_ingestion_timestamp
)
VALUES (
    source.customer_id, source.company_name, source.contact_name, source.contact_title, source.address, source.city, source.region, source.postal_code, source.country, source.phone, source.fax, source._ingestion_timestamp
    );