MERGE INTO {DATABASE_NAME}.fact_order_details target
    USING (
    SELECT * FROM (
    SELECT
    OrderID AS order_id,
    ProductID AS product_id,
    CustomerID AS customer_id,
    EmployeeID AS employee_id,
    from_iso8601_timestamp(OrderDate) AS order_date,
    RequiredDate AS required_date,
    ShippedDate AS shipped_date,
    ShipVia AS ship_via,
    Freight AS freight,
    ShipName AS ship_name,
    ShipAddress AS ship_address,
    ShipCity AS ship_city,
    ShipRegion AS ship_region,
    ShipPostalCode AS ship_postal_code,
    ShipCountry AS ship_country,
    UnitPrice AS unit_price,
    Quantity AS quantity,
    Discount AS discount,
    ROUND(UnitPrice * CAST (Quantity AS double) * (1.0 - Discount), 2) AS net_amount,
    _ingestion_timestamp,
    ROW_NUMBER() OVER (PARTITION BY OrderID, ProductID ORDER BY _ingestion_timestamp DESC) AS rn
    FROM {DATABASE_NAME}.stg_raw_orders
    WHERE OrderDate IS NOT NULL
    AND OrderID IS NOT NULL
    AND ProductID IS NOT NULL
    AND year = '{YEAR}' AND month = '{MONTH}'
    ) WHERE rn = 1
    ) source
    ON target.order_id = source.order_id AND target.product_id = source.product_id
    WHEN MATCHED THEN
UPDATE SET
    customer_id = source.customer_id,
    employee_id = source.employee_id,
    order_date = source.order_date,
    required_date = source.required_date,
    shipped_date = source.shipped_date,
    ship_via = source.ship_via,
    freight = source.freight,
    ship_name = source.ship_name,
    ship_address = source.ship_address,
    ship_city = source.ship_city,
    ship_region = source.ship_region,
    ship_postal_code = source.ship_postal_code,
    ship_country = source.ship_country,
    unit_price = source.unit_price,
    quantity = source.quantity,
    discount = source.discount,
    net_amount = source.net_amount,
    _ingestion_timestamp = source._ingestion_timestamp
    WHEN NOT MATCHED THEN
INSERT
(
order_id
,
product_id
,
customer_id
,
employee_id
,
order_date
,
required_date
,
shipped_date
,
ship_via
,
freight
,
ship_name
,
ship_address
,
ship_city
,
ship_region
,
ship_postal_code
,
ship_country
,
unit_price
,
quantity
,
discount
,
net_amount
,
_ingestion_timestamp
)
VALUES (
    source.order_id, source.product_id, source.customer_id, source.employee_id, source.order_date, source.required_date, source.shipped_date, source.ship_via, source.freight, source.ship_name, source.ship_address, source.ship_city, source.ship_region, source.ship_postal_code, source.ship_country, source.unit_price, source.quantity, source.discount, source.net_amount, source._ingestion_timestamp
    );