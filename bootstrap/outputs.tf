# ==============================================================================
# TERRAFORM BOOTSTRAP - OUTPUT EXPORTS
# ==============================================================================
# Purpose: Exports the unique names of the provisioned S3 Bucket and DynamoDB
#          Table required to configure the backend block in the main infrastructure.
# ==============================================================================

output "s3_state_bucket_name" {
  description = "The globally unique name of the S3 Bucket hosting the remote state file"
  value       = aws_s3_bucket.terraform_state.id
}

output "dynamodb_table_name" {
  description = "The name of the DynamoDB Table utilized for Terraform distributed state locking"
  value       = aws_dynamodb_table.terraform_locks.name
}
