# ==============================================================================
# AWS LAMBDA INGESTION INFRASTRUCTURE MODULE
# ==============================================================================
# Architecture: Pure Functional Serverless Micro-Task Worker
# Purpose: Packages Python source code and Linux binary wheels into an S3-backed
#          Lambda Layer and provisions the execution environment with strict
#          FinOps memory allocation and SRE timeout guardrails.
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Source Code Packaging (Automated Local ZIP Archive)
# ------------------------------------------------------------------------------
# Compresses the Python handler file into a deployable zip artifact.
# Calculates base64sha256 hash to trigger Terraform deployments only when code changes.
data "archive_file" "lambda_code_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/ingestion/lambda_function.py"
  output_path = "${path.module}/lambda_function_payload.zip"
}

# ------------------------------------------------------------------------------
# 2. Dependency Layer Packaging (Linux x86_64 Binary Wheels)
# ------------------------------------------------------------------------------
# Packages external libraries (Polars, Requests, Urllib3) installed in layers/polars_layer.
# Expected directory layout: layers/polars_layer/python/lib/python3.11/site-packages/...
data "archive_file" "lambda_layer_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../layers/polars_layer"
  output_path = "${path.module}/polars_layer_payload.zip"
}

# ------------------------------------------------------------------------------
# 3. Layer Staging to S3 (Bypassing the 50 MB Lambda Direct Upload API Limit)
# ------------------------------------------------------------------------------
# Why S3 Upload? AWS Lambda API rejects direct payload uploads larger than 50 MB.
# Compiled binary packages like Polars are ~35-40 MB zipped (dangerously close to the limit).
# Staging the zip artifact in S3 enables layer sizes up to 250 MB (unzipped) with 100% reliability.
resource "aws_s3_object" "polars_layer_zip_s3" {
  bucket      = aws_s3_bucket.raw_zone.id
  key         = "_build_artifacts/polars_layer_payload.zip"
  source      = data.archive_file.lambda_layer_zip.output_path
  source_hash = data.archive_file.lambda_layer_zip.output_base64sha256

  tags = {
    ArtifactType = "Lambda-Layer-Package"
    ManagedBy    = "Terraform"
  }
}

# ------------------------------------------------------------------------------
# 4. AWS Lambda Layer Version Registration
# ------------------------------------------------------------------------------
# Registers the S3-hosted artifact as an immutable, versioned Lambda Layer.
resource "aws_lambda_layer_version" "polars_layer" {
  layer_name          = "${var.project_name}-polars-layer"
  s3_bucket           = aws_s3_bucket.raw_zone.id
  s3_key              = aws_s3_object.polars_layer_zip_s3.key
  compatible_runtimes = ["python3.11", "python3.12"]
  source_code_hash    = data.archive_file.lambda_layer_zip.output_base64sha256
  description         = "Immutable runtime layer containing Polars, Requests, and Urllib3 (manylinux2014_x86_64)"
}

# ------------------------------------------------------------------------------
# 5. Core AWS Lambda Ingestion Function
# ------------------------------------------------------------------------------
# Provisions the serverless compute unit executing the pure functional ingestion worker.
resource "aws_lambda_function" "ingestion_lambda" {
  filename         = data.archive_file.lambda_code_zip.output_path
  function_name    = "${var.project_name}-ingestion"
  role             = aws_iam_role.lambda_ingestion_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.11"
  source_code_hash = data.archive_file.lambda_code_zip.output_base64sha256

  # FinOps Guardrails:
  # - timeout (300s / 5 min): Upper execution bound to prevent runaway cloud spend.
  # - memory_size (512 MB): Optimal price-performance ratio for Polars in-memory Arrow engine.
  timeout     = 300
  memory_size = 512

  # Attach the pre-built dependencies layer
  layers = [
    aws_lambda_layer_version.polars_layer.arn
  ]

  # Runtime Environment Variables
  environment {
    variables = {
      RAW_BUCKET_NAME = aws_s3_bucket.raw_zone.id
      ODATA_BASE_URL  = "https://services.odata.org/v4/northwind/northwind.svc"
      PROJECT_NAME    = var.project_name
    }
  }

  # Dependency Guard: Ensures CloudWatch Log Group with 14-day retention exists BEFORE
  # Lambda invokes, preventing AWS from auto-creating non-expiring log groups.
  depends_on = [
    aws_cloudwatch_log_group.lambda_ingestion_logs
  ]

  tags = {
    Component = "Ingestion-Worker"
    Layer     = "Bronze"
  }
}
