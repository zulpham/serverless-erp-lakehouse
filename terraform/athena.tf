# ==============================================================================
# ANALYTICS LAYER - AMAZON ATHENA WORKGROUP & FINOPS QUERY GUARDRAILS
# ==============================================================================

resource "aws_s3_bucket" "athena_results" {
  bucket        = "${var.project_name}-athena-results-${var.aws_region}-${local.account_suffix}"
  force_destroy = true

  tags = {
    Layer       = "Athena-Query-Results"
    Component   = "Analytics-Storage"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_s3_bucket_public_access_block" "athena_results_block" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results_crypto" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results_lifecycle" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "auto-delete-old-query-results"
    status = "Enabled"

    filter {
      prefix = ""
    }

    expiration {
      days = 7
    }
  }
}

resource "aws_athena_workgroup" "lakehouse_workgroup" {
  name        = "${var.project_name}-workgroup"
  description = "Isolated workgroup for AI Text-to-SQL with strict per-query data scan budget controls"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    # FINOPS RATIONALIZED CUTOFF: 10 GB per query limit
    # Prevents runaway queries while providing ample headroom for Iceberg metadata manifests.
    # Calculation: 10 GB = 10 * 1024 * 1024 * 1024 bytes = 10,737,418,240 bytes (Max cost: $0.05/query)
    bytes_scanned_cutoff_per_query = 10737418240

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.id}/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = {
    Component   = "AI-Analytics-Engine"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}
