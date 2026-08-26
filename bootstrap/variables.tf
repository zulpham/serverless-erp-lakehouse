# ==============================================================================
# TERRAFORM BOOTSTRAP - INPUT VARIABLES
# ==============================================================================
# Purpose: Defines project-wide naming conventions and geographical deployment
#          targets for provisioning the remote state backend infrastructure.
# ==============================================================================

variable "aws_region" {
  description = "Target AWS Region for provisioning the remote state S3 bucket and DynamoDB lock table"
  type        = string
  default     = "ap-southeast-2" # Sydney Region (Selected for full native Amazon Bedrock & Glue support)
}

variable "project_name" {
  description = "Standardized project prefix applied to all underlying AWS resources for cost allocation and namespace isolation"
  type        = string
  default     = "erp-lakehouse-portfolio"
}
