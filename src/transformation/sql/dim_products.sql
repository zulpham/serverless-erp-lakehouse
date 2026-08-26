MERGE INTO {DATABASE_NAME}.dim_products target
    USING (
    SELECT * FROM (
    SELECT
    ProductID AS product_id,
    ProductName AS product_name,
    SupplierID AS supplier_id,
    CategoryID AS category_id,
    QuantityPerUnit AS quantity_per_unit,
    UnitPrice AS unit_price,
    UnitsInStock AS units_in_stock,
    UnitsOnOrder AS units_on_order,
    ReorderLevel AS reorder_level,
    Discontinued AS discontinued,
    _ingestion_timestamp,
    ROW_NUMBER() OVER (PARTITION BY ProductID ORDER BY _ingestion_timestamp DESC) AS rn
    FROM {DATABASE_NAME}.stg_raw_products
    WHERE ProductID IS NOT NULL
    AND year = '{YEAR}' AND month = '{MONTH}'
    ) WHERE rn = 1
    ) source
    ON target.product_id = source.product_id
    WHEN MATCHED THEN
UPDATE SET
    product_name = source.product_name,
    supplier_id = source.supplier_id,
    category_id = source.category_id,
    quantity_per_unit = source.quantity_per_unit,
    unit_price = source.unit_price,
    units_in_stock = source.units_in_stock,
    units_on_order = source.units_on_order,
    reorder_level = source.reorder_level,
    discontinued = source.discontinued,
    _ingestion_timestamp = source._ingestion_timestamp
    WHEN NOT MATCHED THEN
INSERT
(
product_id
,
product_name
,
supplier_id
,
category_id
,
quantity_per_unit
,
unit_price
,
units_in_stock
,
units_on_order
,
reorder_level
,
discontinued
,
_ingestion_timestamp
)
VALUES (
    source.product_id, source.product_name, source.supplier_id, source.category_id, source.quantity_per_unit, source.unit_price, source.units_in_stock, source.units_on_order, source.reorder_level, source.discontinued, source._ingestion_timestamp
    );