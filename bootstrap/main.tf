# ==============================================================================
# TERRAFORM BOOTSTRAP - MAIN INFRASTRUCTURE
# ==============================================================================
# Architecture: Terraform Remote State Storage & Concurrency Control Backend
# Pattern: Two-Phase Bootstrap Pattern (Local State -> Remote State Gateway)
# Security Standard: CIS AWS Foundations Benchmark (Encrypted, Private, Versioned)
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Terraform Core & AWS Provider Configuration
# ------------------------------------------------------------------------------
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ------------------------------------------------------------------------------
# 2. Remote State Storage Bucket (Amazon S3)
# ------------------------------------------------------------------------------
# Dedicated storage bucket holding the centralized 'terraform.tfstate' file.
# Lifecycle Guard: 'prevent_destroy = true' guarantees that accidental 'terraform destroy'
# commands will NEVER wipe out the backend state repository.
resource "aws_s3_bucket" "terraform_state" {
  bucket        = "${var.project_name}-tfstate-${var.aws_region}"
  force_destroy = false

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    Name        = "${var.project_name}-tfstate"
    Component   = "Terraform-Remote-State"
    Environment = "Management"
    ManagedBy   = "Terraform-Bootstrap"
  }
}

# ------------------------------------------------------------------------------
# 3. S3 Bucket Versioning (Disaster Recovery & State History)
# ------------------------------------------------------------------------------
# Retains past versions of the state file on every 'terraform apply'.
# Protects against state corruption, accidental overwrites, and human error.
resource "aws_s3_bucket_versioning" "terraform_state_versioning" {
  bucket = aws_s3_bucket.terraform_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

# ------------------------------------------------------------------------------
# 4. Server-Side Encryption (Data-at-Rest Protection)
# ------------------------------------------------------------------------------
# Enforces default AES-256 server-side encryption (SSE-S3) on all state objects,
# ensuring sensitive infrastructure secrets in plaintext state files are encrypted.
resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_state_crypto" {
  bucket = aws_s3_bucket.terraform_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ------------------------------------------------------------------------------
# 5. Public Access Block (Strict Perimeter Security)
# ------------------------------------------------------------------------------
# Blocks 100% of public ACLs and Bucket Policies to prevent unauthorized public
# exposure of infrastructure architecture and credentials.
resource "aws_s3_bucket_public_access_block" "terraform_state_block" {
  bucket                  = aws_s3_bucket.terraform_state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ------------------------------------------------------------------------------
# 6. Concurrency State Locking Table (Amazon DynamoDB)
# ------------------------------------------------------------------------------
# Provides distributed mutual exclusion (mutex lock) during Terraform executions.
# Hash Key: 'LockID' (String) is mandatory per HashiCorp AWS backend specification.
# Billing Mode: 'PAY_PER_REQUEST' ensures zero idle cost during inactive periods.
resource "aws_dynamodb_table" "terraform_locks" {
  name         = "${var.project_name}-tflocks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = {
    Name        = "${var.project_name}-tflocks"
    Component   = "Terraform-State-Locking"
    Environment = "Management"
    ManagedBy   = "Terraform-Bootstrap"
  }
}
