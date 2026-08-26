variable "github_repository" {
  description = "Format: 'username/repo-name'"
  type        = string
  default     = "zulpham/serverless-erp-lakehouse"
}

# 1. AWS IAM OpenID Connect (OIDC) Provider dengan Thumbprint Resmi Terlengkap
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

# 2. IAM Role dengan Trust Policy Terbuka untuk Seluruh Event di Repositori Anda
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
          StringLike = {
            "token.actions.githubusercontent.com:sub" = "repo:${var.github_repository}:*"
          }
        }
      }
    ]
  })

  tags = {
    Component = "Zero-Trust-CI-CD"
  }
}

# 3. Izin Deployment
resource "aws_iam_policy" "github_oidc_scoped_policy" {
  name        = "${var.project_name}-github-oidc-deploy-policy"
  description = "Scoped policy limiting CI/CD runner to project-specific infrastructure only"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:*"]
        Resource = [
          "arn:aws:s3:::${var.project_name}-*",
          "arn:aws:s3:::${var.project_name}-*/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["lambda:*"]
        Resource = "arn:aws:lambda:${var.aws_region}:${data.aws_caller_identity.current.account_id}:function:${var.project_name}-*"
      },
      {
        Effect   = "Allow"
        Action   = ["states:*"]
        Resource = "arn:aws:states:${var.aws_region}:${data.aws_caller_identity.current.account_id}:stateMachine:${var.project_name}-*"
      },
      {
        Effect   = "Allow"
        Action   = ["glue:*"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["iam:*"]
        Resource = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.project_name}-*"
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:*"]
        Resource = "arn:aws:dynamodb:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${var.project_name}-*"
      },
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
