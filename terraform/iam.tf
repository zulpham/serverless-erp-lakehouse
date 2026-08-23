# IAM Role for Lambda Service
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
}

# CloudWatch Log Group with Retention
resource "aws_cloudwatch_log_group" "lambda_ingestion_logs" {
  name              = "/aws/lambda/${var.project_name}-ingestion"
  retention_in_days = 14
}

# IAM Policy: Special Permission for S3 PutObject & CloudWatch Logs
resource "aws_iam_policy" "lambda_ingestion_policy" {
  name        = "${var.project_name}-lambda-ingestion-policy"
  description = "Permission for Lambda to write data to Raw S3 and create logs in CloudWatch"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      #Logging to CloudWatch
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.lambda_ingestion_logs.arn}:*"
      },
      # Write to S3 Raw Zone
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:AbortMultipartUpload"
        ]
        Resource = "${aws_s3_bucket.raw_zone.arn}/*"
      }
    ]
  })
}

# Connect Policy to Role
resource "aws_iam_role_policy_attachment" "lambda_ingestion_attach" {
  role       = aws_iam_role.lambda_ingestion_role.name
  policy_arn = aws_iam_policy.lambda_ingestion_policy.arn
}
