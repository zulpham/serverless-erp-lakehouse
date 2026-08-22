output "raw_bucket_name" {
  description = "S3 Bucket Raw Zone name"
  value = aws_s3_bucket.raw_zone.id
}

output "iceberg_warehouse_bucket_name" {
  description = "S3 Bucket Apache Iceberg Warehouse name"
  value = aws_s3_bucket.iceberg_warehouse.id
}
