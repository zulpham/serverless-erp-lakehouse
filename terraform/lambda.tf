# Python Source Code Zip Automation (src/ingestion/lambda_function.py)
data "archive_file" "lambda_code_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/ingestion/lambda_function.py"
  output_path = "${path.module}/lambda_function_payload.zip"
}

# Dependencies Layer Zip Automation (layers/polars_layer/)
data "archive_file" "lambda_layer_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../layers/polars_layer"
  output_path = "${path.module}/polars_layer_payload.zip"
}

# Upload Layer Zip to S3 (Avoiding Direct Upload API's 50 MB Limit)
resource "aws_s3_object" "polars_layer_zip_s3" {
  bucket      = aws_s3_bucket.raw_zone.id
  key         = "_build_artifacts/polars_layer_payload.zip"
  source      = data.archive_file.lambda_layer_zip.output_path
  source_hash = data.archive_file.lambda_layer_zip.output_base64sha256
}

# Create a Lambda Layer by Reading from S3 on AWS
resource "aws_lambda_layer_version" "polars_layer" {
  layer_name          = "${var.project_name}-polars-layer"
  s3_bucket           = aws_s3_bucket.raw_zone.id
  s3_key              = aws_s3_object.polars_layer_zip_s3.key
  compatible_runtimes = ["python3.11", "python3.12"]
  source_code_hash    = data.archive_file.lambda_layer_zip.output_base64sha256
  description         = "The layer contains Polars, Requests, and Urllib3 for serverless architectures"
}

# Creates AWS Lambda Function
resource "aws_lambda_function" "ingestion_lambda" {
  filename         = data.archive_file.lambda_code_zip.output_path
  function_name    = "${var.project_name}-ingestion"
  role             = aws_iam_role.lambda_ingestion_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.11"
  source_code_hash = data.archive_file.lambda_code_zip.output_base64sha256

  # Memory allocation & timeout limits
  timeout     = 300 # 5 Minutes
  memory_size = 512 # 512 MB RAM

  layers = [
    aws_lambda_layer_version.polars_layer.arn
  ]

  environment {
    variables = {
      RAW_BUCKET_NAME = aws_s3_bucket.raw_zone.id
      ODATA_BASE_URL  = "https://services.odata.org/v4/northwind/northwind.svc"
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.lambda_ingestion_logs
  ]
}
