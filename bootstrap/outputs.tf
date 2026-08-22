output "s3_state_bucket_name" {
  description = "S3 Bucket Remote State name"
  value       = aws_s3_bucket.terraform_state.id
}

output "dynamodb_table_name" {
  description = "DynamoDB Table State Lock name"
  value       = aws_dynamodb_table.terraform_locks.name
}
