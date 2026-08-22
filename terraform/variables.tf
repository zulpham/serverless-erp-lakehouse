variable "aws_region" {
  description = "AWS Region deployment target"
  type        = string
  default     = "ap-southeast-2"
}

variable "project_name" {
  description = "Project prefix name"
  type        = string
  default     = "serverless-erp-lakehouse"
}

variable "environment" {
  description = "Environment deployment"
  type        = string
  default     = "dev"
}

variable "raw_retention_days" {
  description = "Retention days limit for raw data in the Raw Zone before it is automatically deleted"
  type        = number
  default     = 30
}
