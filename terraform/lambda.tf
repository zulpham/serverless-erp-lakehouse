# ==============================================================================
# COMPUTE LAYER - AWS LAMBDA MODULE (INGESTION WORKER & SQL DISPATCHER)
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. IAM Role & Policy for Ingestion Worker (S3 Raw & SSM Only)
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

resource "aws_cloudwatch_log_group" "lambda_ingestion_logs" {
  name              = "/aws/lambda/${var.project_name}-ingestion"
  retention_in_days = 14
}

resource "aws_iam_policy" "lambda_ingestion_policy" {
  name        = "${var.project_name}-lambda-ingestion-policy"
  description = "Granular policy allowing Ingestion Lambda to write S3 Raw Parquet and read SSM watermarks"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.lambda_ingestion_logs.arn}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:AbortMultipartUpload"
        ]
        Resource = "${aws_s3_bucket.raw_zone.arn}/*"
      },
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
# 2. DEDICATED IAM Role & Policy for SQL Dispatcher (Athena & Glue Scoped)
# ------------------------------------------------------------------------------
resource "aws_iam_role" "lambda_dispatcher_role" {
  name = "${var.project_name}-lambda-dispatcher-role"

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
    Component = "Dispatcher-IAM"
  }
}

resource "aws_cloudwatch_log_group" "sql_dispatcher_logs" {
  name              = "/aws/lambda/${var.project_name}-sql-dispatcher"
  retention_in_days = 14
}

resource "aws_iam_policy" "lambda_dispatcher_policy" {
  name        = "${var.project_name}-lambda-dispatcher-policy"
  description = "Granular policy allowing SQL Dispatcher to log and read catalog metadata"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.sql_dispatcher_logs.arn}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetTables"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_dispatcher_attach" {
  role       = aws_iam_role.lambda_dispatcher_role.name
  policy_arn = aws_iam_policy.lambda_dispatcher_policy.arn
}

# ------------------------------------------------------------------------------
# 3. Packaging & Layer Versioning
# ------------------------------------------------------------------------------
data "archive_file" "lambda_code_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/lambda_code_payload.zip"
}

data "archive_file" "lambda_layer_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../layers/polars_layer"
  output_path = "${path.module}/polars_layer_payload.zip"
}

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

resource "aws_lambda_layer_version" "polars_layer" {
  layer_name          = "${var.project_name}-polars-layer"
  s3_bucket           = aws_s3_bucket.raw_zone.id
  s3_key              = aws_s3_object.polars_layer_zip_s3.key
  compatible_runtimes = ["python3.11", "python3.12"]
  source_code_hash    = data.archive_file.lambda_layer_zip.output_base64sha256
  description         = "Immutable runtime layer containing Polars, Requests, and Urllib3"
}

# ------------------------------------------------------------------------------
# 4. Ingestion Worker Function
# ------------------------------------------------------------------------------
resource "aws_lambda_function" "ingestion_lambda" {
  filename         = data.archive_file.lambda_code_zip.output_path
  function_name    = "${var.project_name}-ingestion"
  role             = aws_iam_role.lambda_ingestion_role.arn
  handler          = "ingestion/lambda_function.lambda_handler"
  runtime          = "python3.11"
  source_code_hash = data.archive_file.lambda_code_zip.output_base64sha256

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

# ------------------------------------------------------------------------------
# DEDICATED IAM Role & Policy for SQL Dispatcher (Pure In-Memory Renderer)
# ------------------------------------------------------------------------------
resource "aws_iam_role" "lambda_dispatcher_role" {
  name = "${var.project_name}-lambda-dispatcher-role"

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
    Component = "Dispatcher-IAM"
  }
}

resource "aws_cloudwatch_log_group" "sql_dispatcher_logs" {
  name              = "/aws/lambda/${var.project_name}-sql-dispatcher"
  retention_in_days = 14
}

# 100% PURE LEAST-PRIVILEGE: Zero AWS API permissions except CloudWatch Logging
resource "aws_iam_policy" "lambda_dispatcher_policy" {
  name        = "${var.project_name}-lambda-dispatcher-policy"
  description = "Minimalist policy granting SQL Dispatcher only CloudWatch logging rights"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.sql_dispatcher_logs.arn}:*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_dispatcher_attach" {
  role       = aws_iam_role.lambda_dispatcher_role.name
  policy_arn = aws_iam_policy.lambda_dispatcher_policy.arn
}
