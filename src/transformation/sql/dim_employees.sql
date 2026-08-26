MERGE INTO {DATABASE_NAME}.dim_employees target
    USING (
    SELECT * FROM (
    SELECT
    EmployeeID AS employee_id,
    LastName AS last_name,
    FirstName AS first_name,
    Title AS title,
    TitleOfCourtesy AS title_of_courtesy,
    BirthDate AS birth_date,
    HireDate AS hire_date,
    Address AS address,
    City AS city,
    Region AS region,
    PostalCode AS postal_code,
    Country AS country,
    HomePhone AS home_phone,
    Extension AS extension,
    Notes AS notes,
    ReportsTo AS reports_to,
    _ingestion_timestamp,
    ROW_NUMBER() OVER (PARTITION BY EmployeeID ORDER BY _ingestion_timestamp DESC) AS rn
    FROM {DATABASE_NAME}.stg_raw_employees
    WHERE EmployeeID IS NOT NULL
    AND year = '{YEAR}' AND month = '{MONTH}'
    ) WHERE rn = 1
    ) source
    ON target.employee_id = source.employee_id
    WHEN MATCHED THEN
UPDATE SET
    last_name = source.last_name,
    first_name = source.first_name,
    title = source.title,
    title_of_courtesy = source.title_of_courtesy,
    birth_date = source.birth_date,
    hire_date = source.hire_date,
    address = source.address,
    city = source.city,
    region = source.region,
    postal_code = source.postal_code,
    country = source.country,
    home_phone = source.home_phone,
    extension = source.extension,
    notes = source.notes,
    reports_to = source.reports_to,
    _ingestion_timestamp = source._ingestion_timestamp
    WHEN NOT MATCHED THEN
INSERT
(
employee_id
,
last_name
,
first_name
,
title
,
title_of_courtesy
,
birth_date
,
hire_date
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
home_phone
,
extension
,
notes
,
reports_to
,
_ingestion_timestamp
)
VALUES (
    source.employee_id, source.last_name, source.first_name, source.title, source.title_of_courtesy, source.birth_date, source.hire_date, source.address, source.city, source.region, source.postal_code, source.country, source.home_phone, source.extension, source.notes, source.reports_to, source._ingestion_timestamp
    );