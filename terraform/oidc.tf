# ==============================================================================
# ZERO-TRUST CI/CD - AWS IAM OPENID CONNECT (OIDC) FEDERATION MODULE
# ==============================================================================
# Architecture: Passwordless Identity Federation (GitHub Actions -> AWS STS)
# Standards: Immutable Subject Claim Binding & Scoped Permissions Policy
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. AWS IAM OpenID Connect (OIDC) Identity Provider
# ------------------------------------------------------------------------------
# Registers GitHub Actions as a trusted identity provider within the AWS Account
resource "aws_iam_openid_connect_provider" "github_oidc" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  thumbprint_list = [
    "6938fd4d98bab03faadb97b34396831e3780aea1",
    "1c58a3a8518e8759bf075b76b750d4f2df264fcd",
    "15e78e0ab829e083fb8b9074092b7754308a3d46"
  ]

  tags = {
    Component = "OIDC-Authentication"
  }
}

# ------------------------------------------------------------------------------
# 2. IAM Role for GitHub Actions (Repo-Locked Trust Policy)
# ------------------------------------------------------------------------------
# Restricts role assumption strictly to workflows originating from your specific repository
resource "aws_iam_role" "github_actions_oidc_role" {
  name = "${var.project_name}-github-oidc-deploy-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = aws_iam_openid_connect_provider.github_oidc.arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          }
          # Anti Confused-Deputy: Matches both 2026 Immutable ID format and wildcard slugs
          StringLike = {
            "token.actions.githubusercontent.com:sub" = [
              "repo:${var.github_repository}:*",
              "repo:zulpham@172234337/serverless-erp-lakehouse@1346920952:*"
            ]
          }
        }
      }
    ]
  })

  tags = {
    Component = "Zero-Trust-CI-CD"
  }
}

# ------------------------------------------------------------------------------
# 3. Scoped Least-Privilege Deployment Policy
# ------------------------------------------------------------------------------
# Explicitly limits the CI/CD runner to resources bearing the project prefix
# Gantikan resource "aws_iam_policy" "github_oidc_scoped_policy" di terraform/oidc.tf dengan ini:
resource "aws_iam_policy" "github_oidc_scoped_policy" {
  name        = "${var.project_name}-github-oidc-deploy-policy"
  description = "Scoped policy limiting CI/CD runner to project-specific infrastructure and bootstrap backend"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Project S3 Buckets & Remote State Backend
      {
        Effect = "Allow"
        Action = ["s3:*"]
        Resource = [
          "arn:aws:s3:::${var.project_name}-*",
          "arn:aws:s3:::${var.project_name}-*/*",
          "arn:aws:s3:::erp-lakehouse-portfolio-*",
          "arn:aws:s3:::erp-lakehouse-portfolio-*/*"
        ]
      },
      # Project Lambda Functions & Lambda Layers
      {
        Effect = "Allow"
        Action = ["lambda:*"]
        Resource = [
          "arn:aws:lambda:${var.aws_region}:${data.aws_caller_identity.current.account_id}:function:${var.project_name}-*",
          "arn:aws:lambda:${var.aws_region}:${data.aws_caller_identity.current.account_id}:layer:${var.project_name}-*"
        ]
      },
      # Project Step Functions State Machines & Validation (Wildcard Resource Required for ASL Validation)
      {
        Effect   = "Allow"
        Action   = ["states:*"]
        Resource = "*"
      },
      # AWS Glue Catalog & Database
      {
        Effect   = "Allow"
        Action   = ["glue:*"]
        Resource = "*"
      },
      # Scoped IAM Roles, Policies, & OIDC Provider
      {
        Effect = "Allow"
        Action = ["iam:*"]
        Resource = [
          "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.project_name}-*",
          "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/${var.project_name}-*",
          "arn:aws:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/*"
        ]
      },
      # DynamoDB State Lock Tables
      {
        Effect = "Allow"
        Action = ["dynamodb:*"]
        Resource = [
          "arn:aws:dynamodb:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${var.project_name}-*",
          "arn:aws:dynamodb:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/erp-lakehouse-portfolio-*"
        ]
      },
      # CloudWatch Logs, Athena, SNS, & SSM
      {
        Effect   = "Allow"
        Action   = ["logs:*", "athena:*", "sns:*", "ssm:*"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "github_oidc_scoped_attach" {
  role       = aws_iam_role.github_actions_oidc_role.name
  policy_arn = aws_iam_policy.github_oidc_scoped_policy.arn
}
