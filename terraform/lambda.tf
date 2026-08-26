# ==============================================================================
# INGESTION COMPUTE LAYER - AWS LAMBDA MODULE (SELF-CONTAINED)
# ==============================================================================
# Architecture: Pure Functional Serverless Micro-Task Worker
# Purpose: Packages Python source code and Linux binary wheels into an S3-backed
#          layer, provisions execution IAM roles, and defines the compute worker.
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. IAM Execution Role & Least-Privilege Policy
# ------------------------------------------------------------------------------
resource "aws_iam_role" "lambda_ingestion_role" {
  name = "${var.project_name}-lambda-ingestion-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Component = "Ingestion-IAM"
  }
}

# Dedicated CloudWatch Log Group with 14-day retention for SRE log inspection
resource "aws_cloudwatch_log_group" "lambda_ingestion_logs" {
  name              = "/aws/lambda/${var.project_name}-ingestion"
  retention_in_days = 14
}

# Scoped IAM Policy: Restricts Lambda to CloudWatch, S3 Raw Zone, and SSM Parameters
resource "aws_iam_policy" "lambda_ingestion_policy" {
  name        = "${var.project_name}-lambda-ingestion-policy"
  description = "Granular policy allowing Lambda to write S3 Raw Parquet, log to CloudWatch, and read SSM watermarks"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # CloudWatch Log Streaming
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.lambda_ingestion_logs.arn}:*"
      },
      # S3 Raw Zone Write Access
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:AbortMultipartUpload"
        ]
        Resource = "${aws_s3_bucket.raw_zone.arn}/*"
      },
      # AWS SSM Parameter Store Read Access (Fail-Fast Watermark Retrieval)
      {
        Effect = "Allow"
        Action = [
          "ssm:GetParameter",
          "ssm:PutParameter"
        ]
        Resource = "arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter/${var.project_name}/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_ingestion_attach" {
  role       = aws_iam_role.lambda_ingestion_role.name
  policy_arn = aws_iam_policy.lambda_ingestion_policy.arn
}

# ------------------------------------------------------------------------------
# 2. Source Packaging & S3-Backed Layer Staging
# ------------------------------------------------------------------------------
# Compresses the Python handler code into a deployable zip artifact
data "archive_file" "lambda_code_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/ingestion/lambda_function.py"
  output_path = "${path.module}/lambda_function_payload.zip"
}

# Compresses external dependency wheels (Polars, Requests, Urllib3)
data "archive_file" "lambda_layer_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../layers/polars_layer"
  output_path = "${path.module}/polars_layer_payload.zip"
}

# Uploads layer archive to S3 to bypass the Lambda 50 MB Direct Upload API limit
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

# Registers the immutable Lambda Layer Version pointing to S3
resource "aws_lambda_layer_version" "polars_layer" {
  layer_name          = "${var.project_name}-polars-layer"
  s3_bucket           = aws_s3_bucket.raw_zone.id
  s3_key              = aws_s3_object.polars_layer_zip_s3.key
  compatible_runtimes = ["python3.11", "python3.12"]
  source_code_hash    = data.archive_file.lambda_layer_zip.output_base64sha256
  description         = "Immutable runtime layer containing Polars, Requests, and Urllib3 (manylinux2014_x86_64)"
}

# ------------------------------------------------------------------------------
# 3. Core AWS Lambda Ingestion Function
# ------------------------------------------------------------------------------
resource "aws_lambda_function" "ingestion_lambda" {
  filename         = data.archive_file.lambda_code_zip.output_path
  function_name    = "${var.project_name}-ingestion"
  role             = aws_iam_role.lambda_ingestion_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.11"
  source_code_hash = data.archive_file.lambda_code_zip.output_base64sha256

  # FinOps Guardrails: 5-minute timeout and 512 MB RAM for optimal Polars Arrow execution
  timeout     = 300
  memory_size = 512

  layers = [
    aws_lambda_layer_version.polars_layer.arn
  ]

  environment {
    variables = {
      RAW_BUCKET_NAME = aws_s3_bucket.raw_zone.id
      ODATA_BASE_URL  = "https://services.odata.org/v4/northwind/northwind.svc"
      PROJECT_NAME    = var.project_name
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.lambda_ingestion_logs
  ]

  tags = {
    Component = "Ingestion-Worker"
    Layer     = "Bronze"
  }
}
