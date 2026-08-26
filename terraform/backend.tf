# ==============================================================================
# MAIN INFRASTRUCTURE - REMOTE STATE BACKEND & AWS PROVIDER CONFIGURATION
# ==============================================================================
# Architecture: Centralized State Locking & Global Tagging Enforcement
# Purpose: Connects the main Lakehouse infrastructure module to the pre-provisioned
#          Bootstrap S3 Remote State Bucket and DynamoDB Mutex Lock Table.
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Terraform Core & Provider Version Constraints
# ------------------------------------------------------------------------------
terraform {
  required_version = ">= 1.5.0"

  required_providers {
    # Official AWS Provider for managing cloud resources
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    # Archive Provider for local packaging of Lambda zip files
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7.0"
    }
  }

  # ----------------------------------------------------------------------------
  # 2. Remote State Backend (Connected to Bootstrap Resources)
  # ----------------------------------------------------------------------------
  # Centralizes the state file in S3 with server-side encryption and distributed
  # mutex locking via DynamoDB to prevent concurrent execution conflicts in CI/CD.
  backend "s3" {
    bucket         = "erp-lakehouse-portfolio-tfstate-ap-southeast-2"
    key            = "lakehouse/terraform.tfstate" # Object path inside the state bucket
    region         = "ap-southeast-2"
    dynamodb_table = "erp-lakehouse-portfolio-tflocks"
    encrypt        = true
  }
}

# ------------------------------------------------------------------------------
# 3. AWS Provider Configuration & Universal Tagging (FinOps Standard)
# ------------------------------------------------------------------------------
# Enforces mandatory Cost Allocation Tags on every provisioned resource across AWS.
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# ------------------------------------------------------------------------------
# 4. Caller Identity Data Source
# ------------------------------------------------------------------------------
# Dynamically queries the current AWS Account ID for resource name deduplication
# and granular IAM ARN policy interpolation without hardcoding secrets.
data "aws_caller_identity" "current" {}
