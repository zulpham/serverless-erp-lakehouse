MERGE INTO {DATABASE_NAME}.dim_suppliers target
    USING (
    SELECT * FROM (
    SELECT
    SupplierID AS supplier_id,
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
    HomePage AS home_page,
    _ingestion_timestamp,
    ROW_NUMBER() OVER (PARTITION BY SupplierID ORDER BY _ingestion_timestamp DESC) AS rn
    FROM {DATABASE_NAME}.stg_raw_suppliers
    WHERE SupplierID IS NOT NULL
    AND year = '{YEAR}' AND month = '{MONTH}'
    ) WHERE rn = 1
    ) source
    ON target.supplier_id = source.supplier_id
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
    home_page = source.home_page,
    _ingestion_timestamp = source._ingestion_timestamp
    WHEN NOT MATCHED THEN
INSERT
(
supplier_id
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
home_page
,
_ingestion_timestamp
)
VALUES (
    source.supplier_id, source.company_name, source.contact_name, source.contact_title, source.address, source.city, source.region, source.postal_code, source.country, source.phone, source.fax, source.home_page, source._ingestion_timestamp
    );