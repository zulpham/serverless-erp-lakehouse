resource "aws_glue_catalog_database" "lakehouse_db" {
  name         = replace("${var.project_name}_db", "-", "_")
  description  = "Centralized metastore database for Apache Iceberg Lakehouse tables"
  location_uri = "s3://${aws_s3_bucket.iceberg_warehouse.id}/"
}
