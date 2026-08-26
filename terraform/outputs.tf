# ==============================================================================
# MAIN INFRASTRUCTURE - OUTPUT EXPORTS REGISTRY
# ==============================================================================
# Architecture: Serverless ERP Data Lakehouse
# Purpose: Exposes critical resource identifiers, endpoints, and ARNs required
#          for automated CI/CD pipelines, downstream SRE alerting, and AI engine hooks.
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. STORAGE LAYER OUTPUTS (Amazon S3)
# ------------------------------------------------------------------------------

output "raw_bucket_name" {
  description = "The globally unique name of the Amazon S3 Bronze Raw Zone bucket for landing transient OData Parquet files"
  value       = aws_s3_bucket.raw_zone.id
}

output "iceberg_warehouse_bucket_name" {
  description = "The globally unique name of the Amazon S3 Silver/Gold Warehouse bucket storing Apache Iceberg tables and metadata"
  value       = aws_s3_bucket.iceberg_warehouse.id
}

# ------------------------------------------------------------------------------
# 2. INGESTION COMPUTE LAYER OUTPUTS (AWS Lambda)
# ------------------------------------------------------------------------------

output "ingestion_lambda_function_name" {
  description = "The physical name of the pure functional Lambda Ingestion Worker executing Polars extraction tasks"
  value       = aws_lambda_function.ingestion_lambda.function_name
}

output "ingestion_lambda_arn" {
  description = "The Amazon Resource Name (ARN) of the Lambda Ingestion Worker utilized by IAM policies and Step Functions"
  value       = aws_lambda_function.ingestion_lambda.arn
}

# ------------------------------------------------------------------------------
# 3. METASTORE & CATALOG LAYER OUTPUTS (AWS Glue Data Catalog)
# ------------------------------------------------------------------------------

output "glue_database_name" {
  description = "The centralized AWS Glue Data Catalog database name hosting the Apache Iceberg table schemas"
  value       = aws_glue_catalog_database.lakehouse_db.name
}

# ------------------------------------------------------------------------------
# 4. ORCHESTRATION & MONITORING OUTPUTS (AWS Step Functions & Amazon SNS)
# ------------------------------------------------------------------------------

output "step_functions_state_machine_arn" {
  description = "The Amazon Resource Name (ARN) of the Step Functions Lakehouse Ingestion & Transformation Orchestrator"
  value       = aws_sfn_state_machine.ingestion_orchestrator.arn
}

output "sns_alerts_topic_arn" {
  description = "The Amazon Resource Name (ARN) of the Amazon SNS Topic receiving dead-letter failure alerts for SRE triage"
  value       = aws_sns_topic.pipeline_alerts.arn
}

output "github_actions_oidc_role_arn" {
  description = "ARN dari Zero-Trust OIDC Role untuk GitHub Actions"
  value       = aws_iam_role.github_actions_oidc_role.arn
}
