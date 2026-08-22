variable "aws_region" {
  description = "AWS Region deployment target"
  type        = string
  default     = "ap-southeast-2" # Sydney Region
}

variable "project_name" {
  description = "Project name prefix for resource standardization"
  type        = string
  default     = "erp-lakehouse-portfolio"
}
