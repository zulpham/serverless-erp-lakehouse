# ==============================================================================
# MAIN INFRASTRUCTURE - INPUT VARIABLES REGISTRY
# ==============================================================================
# Architecture: Serverless ERP Data Lakehouse
# Purpose: Centralizes all deployment parameters, geographical targets, project
#          namespaces, and GitOps repository identifiers for the entire stack.
# ==============================================================================

variable "aws_region" {
  description = "The target AWS Region where all serverless resources are deployed"
  type        = string
  default     = "ap-southeast-2" # Sydney Region (Native support for Bedrock & Glue Spark)
}

variable "project_name" {
  description = "Standardized project namespace prefix applied to all resources for cost allocation and isolation"
  type        = string
  default     = "serverless-erp-lakehouse"
}

variable "environment" {
  description = "The deployment environment stage (e.g., dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "raw_retention_days" {
  description = "FinOps retention threshold (in days) before raw staging Parquet files are auto-expired"
  type        = number
  default     = 30
}

variable "github_repository" {
  description = "The target GitHub repository slug in format 'owner/repo-name' for OIDC trust binding"
  type        = string
  default     = "zulpham/serverless-erp-lakehouse"
}
