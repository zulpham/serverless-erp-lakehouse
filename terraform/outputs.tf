output "raw_bucket_name" {
  description = "S3 Bucket Raw Zone name"
  value       = aws_s3_bucket.raw_zone.id
}

output "iceberg_warehouse_bucket_name" {
  description = "S3 Bucket Apache Iceberg Warehouse name"
  value       = aws_s3_bucket.iceberg_warehouse.id
}

output "ingestion_lambda_function_name" {
  description = "Lambda Ingestion function Name"
  value       = aws_lambda_function.ingestion_lambda.function_name
}

output "ingestion_lambda_arn" {
  description = "ARN from Lambda Ingestion function"
  value       = aws_lambda_function.ingestion_lambda.arn
}
