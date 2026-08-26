# ==============================================================================
# DATA LAKEHOUSE STORAGE LAYER (AMAZON S3)
# ==============================================================================
# Architecture: Medallion Multi-Tier Storage (Bronze Raw & Silver/Gold Iceberg)
# Security: 100% Private, Server-Side Encrypted (AES-256), Lifecycle Managed
# ==============================================================================

locals {
  # Extracts the trailing 6 digits of the AWS Account ID to ensure 100% globally
  # unique S3 bucket names worldwide, eliminating 'BucketAlreadyExists' deploy errors.
  account_suffix = substr(data.aws_caller_identity.current.account_id, -6, 6)
}

# ==============================================================================
# 1. S3 RAW ZONE (Bronze Layer - Transient Staging Buffer)
# ==============================================================================
# Landing storage where the Lambda Ingestion Worker streams partitioned raw Parquet.
# Lifecycle: Ephemeral staging area (auto-cleaned after 30 days).
resource "aws_s3_bucket" "raw_zone" {
  bucket        = "${var.project_name}-raw-${var.aws_region}-${local.account_suffix}"
  force_destroy = true # Enables clean environment teardown for sandbox and portfolio environments

  tags = {
    Layer       = "Bronze-Raw"
    Description = "Transient landing buffer for OData ingestion payloads"
  }
}

# Perimeter Security: Blocks all public access vectors (CIS AWS Benchmark Compliant)
resource "aws_s3_bucket_public_access_block" "raw_zone_block" {
  bucket                  = aws_s3_bucket.raw_zone.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Data-at-Rest Encryption: Enforces default Amazon S3 managed keys (SSE-S3 AES-256)
resource "aws_s3_bucket_server_side_encryption_configuration" "raw_zone_crypto" {
  bucket = aws_s3_bucket.raw_zone.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# FinOps Guardrail: Auto-expires raw bronze files after retention threshold (30 days)
# Prevents silent multi-year storage cost accumulation from old ingestion dumps.
resource "aws_s3_bucket_lifecycle_configuration" "raw_zone_lifecycle" {
  bucket = aws_s3_bucket.raw_zone.id

  rule {
    id     = "auto-delete-raw-staging-files"
    status = "Enabled"

    filter {
      prefix = "" # Applies to all objects in the raw bucket
    }

    expiration {
      days = var.raw_retention_days # Defaults to 30 days
    }
  }
}

# ==============================================================================
# 2. S3 ICEBERG WAREHOUSE (Silver/Gold Layer - ACID Table Storage)
# ==============================================================================
# Central analytical warehouse storing Apache Iceberg v2 metadata manifests,
# snapshot trees, and optimized ZSTD-compressed Parquet data files.
resource "aws_s3_bucket" "iceberg_warehouse" {
  bucket        = "${var.project_name}-iceberg-${var.aws_region}-${local.account_suffix}"
  force_destroy = true

  tags = {
    Layer       = "Silver-Gold-Iceberg"
    Description = "ACID analytical warehouse hosting Apache Iceberg tables and metadata"
  }
}

# Perimeter Security: 100% Private (No Public Access)
resource "aws_s3_bucket_public_access_block" "iceberg_warehouse_block" {
  bucket                  = aws_s3_bucket.iceberg_warehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Data-at-Rest Encryption: AES-256 Server-Side Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "iceberg_warehouse_crypto" {
  bucket = aws_s3_bucket.iceberg_warehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
