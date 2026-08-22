locals {
  # Get last 6 digit of Account ID
  account_suffix = substr(data.aws_caller_identity.current.account_id, -6, 6)
}

# S3 RAW ZONE
resource "aws_s3_bucket" "raw_zone" {
  bucket = "${var.project_name}-raw-${var.aws_region}-${local.account_suffix}"
  force_destroy = true

  tags = {
    Layer = "Raw-Zone"
  }
}

resource "aws_s3_bucket_public_access_block" "raw_zone_block" {
  bucket = aws_s3_bucket.raw_zone.id
  block_public_acls = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_zone_crypto" {
  bucket = aws_s3_bucket.raw_zone.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# FinOPS GuardRail
resource "aws_s3_bucket_lifecycle_configuration" "raw_zone_lifecycle" {
  bucket = aws_s3_bucket.raw_zone.id

  rule {
    id     = "auto-delete-raw-files"
    status = "Enabled"

    filter {
      prefix = "" # For all file on raw bucket
    }

    expiration {
      days = var.raw_retention_days
    }
  }
}

# ICEBERG WAREHOUSE
resource "aws_s3_bucket" "iceberg_warehouse" {
  bucket = "${var.project_name}-iceberg-${var.aws_region}-${local.account_suffix}"
  force_destroy = true

  tags = {
    Layer = "Iceberg-Warehouse"
  }
}

resource "aws_s3_bucket_public_access_block" "iceberg_warehouse_block" {
  bucket = aws_s3_bucket.iceberg_warehouse.id
  block_public_acls = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "iceberg_warehouse_crypto" {
  bucket = aws_s3_bucket.iceberg_warehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}