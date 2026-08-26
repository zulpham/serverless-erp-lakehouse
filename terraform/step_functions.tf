# ==============================================================================
# ORCHESTRATION LAYER - AWS STEP FUNCTIONS STATE MACHINE & DLQ
# ==============================================================================
# Architecture: Resilient Distributed State Machine with SNS Dead-Letter Alerting
# Standards: Injected Initial State, Bounded Concurrency, & Fail-Safe Catching
# Purpose: Orchestrates multi-entity Lambda extractions concurrently, manages
#          stateful pagination resume loops, and atomically commits SSM watermarks.
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Amazon SNS Topic for SRE Dead-Letter Alerts (DLQ)
# ------------------------------------------------------------------------------
# Dedicated notification channel receiving fatal failure payloads when a task
# exhausts all retry attempts.
resource "aws_sns_topic" "pipeline_alerts" {
  name = "${var.project_name}-pipeline-alerts"

  tags = {
    Component = "Monitoring-DLQ"
  }
}

# ------------------------------------------------------------------------------
# 2. CloudWatch Log Group for State Machine Execution Tracing
# ------------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "step_functions_logs" {
  name              = "/aws/vendedlogs/states/${var.project_name}-ingestion-orchestrator"
  retention_in_days = 14
}

# ------------------------------------------------------------------------------
# 3. IAM Execution Role & Granular Policy for Step Functions
# ------------------------------------------------------------------------------
resource "aws_iam_role" "step_functions_role" {
  name = "${var.project_name}-step-functions-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "step_functions_policy" {
  name        = "${var.project_name}-step-functions-policy"
  description = "Granular policy allowing Step Functions to invoke Lambda, commit SSM watermarks, and publish SNS alerts"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Invoke Ingestion Lambda
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = [
          aws_lambda_function.ingestion_lambda.arn,
          "${aws_lambda_function.ingestion_lambda.arn}:*"
        ]
      },
      # Native AWS SDK SSM Parameter Store Integration
      {
        Effect = "Allow"
        Action = [
          "ssm:PutParameter",
          "ssm:GetParameter"
        ]
        Resource = "arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter/${var.project_name}/*"
      },
      # Amazon SNS Notification Publishing
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = aws_sns_topic.pipeline_alerts.arn
      },
      # CloudWatch Logging Permissions
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "step_functions_attach" {
  role       = aws_iam_role.step_functions_role.name
  policy_arn = aws_iam_policy.step_functions_policy.arn
}

# ------------------------------------------------------------------------------
# 4. State Machine Definition (Amazon States Language - ASL)
# ------------------------------------------------------------------------------
resource "aws_sfn_state_machine" "ingestion_orchestrator" {
  name     = "${var.project_name}-ingestion-orchestrator"
  role_arn = aws_iam_role.step_functions_role.arn

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_functions_logs.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  definition = jsonencode({
    Comment = "Serverless ERP Lakehouse - Enterprise Ingestion & DLQ Orchestrator"
    StartAt = "InjectDefaultState"
    States = {
      # ------------------------------------------------------------------------
      # STEP 0: INJECT DEFAULT STATE (Anti First-Run JSONPath Missing Crash)
      # ------------------------------------------------------------------------
      InjectDefaultState = {
        Type = "Pass"
        Result = {
          dimensions = [
            { entity_name = "Customers", is_fact = false },
            { entity_name = "Products", is_fact = false },
            { entity_name = "Employees", is_fact = false },
            { entity_name = "Suppliers", is_fact = false }
          ]
          orders_state = {
            start_chunk_index = 0
            resume_url        = null
            watermark_in      = null
          }
        }
        ResultPath = "$"
        Next       = "ParallelIngestion"
      }

      # ------------------------------------------------------------------------
      # STEP 1: PARALLEL INGESTION (Dimensions Map + Orders Looping Flow)
      # ------------------------------------------------------------------------
      ParallelIngestion = {
        Type = "Parallel"
        Next = "ExtractOrdersWatermark"
        Branches = [
          # --------------------------------------------------------------------
          # BRANCH 0: DIMENSION ENTITIES MAP STATE (Bounded Concurrency: 4)
          # --------------------------------------------------------------------
          {
            StartAt = "IngestDimensionsMap"
            States = {
              IngestDimensionsMap = {
                Type           = "Map"
                MaxConcurrency = 4
                ItemsPath      = "$.dimensions"
                End            = true
                ItemProcessor = {
                  ProcessorConfig = {
                    Mode = "INLINE"
                  }
                  StartAt = "InvokeDimensionLambda"
                  States = {
                    InvokeDimensionLambda = {
                      Type     = "Task"
                      Resource = "arn:aws:states:::lambda:invoke"
                      Parameters = {
                        FunctionName = aws_lambda_function.ingestion_lambda.function_name
                        "Payload.$"  = "$"
                      }
                      Retry = [
                        {
                          ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException"]
                          IntervalSeconds = 2
                          MaxAttempts     = 3
                          BackoffRate     = 2.0
                        }
                      ]
                      Catch = [
                        {
                          ErrorEquals = ["States.ALL"]
                          ResultPath  = "$.error_info"
                          Next        = "NotifyDimensionFailureSNS"
                        }
                      ]
                      End = true
                    }

                    NotifyDimensionFailureSNS = {
                      Type     = "Task"
                      Resource = "arn:aws:states:::sns:publish"
                      Parameters = {
                        TopicArn    = aws_sns_topic.pipeline_alerts.arn
                        Subject     = "ALARM: Dimension Ingestion Failed"
                        "Message.$" = "States.JsonToString($)"
                      }
                      Next = "DimensionTaskFailed"
                    }

                    DimensionTaskFailed = {
                      Type  = "Fail"
                      Cause = "Dimension Ingestion Lambda Failed"
                    }
                  }
                }
              }
            }
          },

          # --------------------------------------------------------------------
          # BRANCH 1: ORDERS FACT INGESTION FLOW WITH STATEFUL CHOICE LOOP
          # --------------------------------------------------------------------
          {
            StartAt = "InvokeOrdersLambda"
            States = {
              InvokeOrdersLambda = {
                Type     = "Task"
                Resource = "arn:aws:states:::lambda:invoke"
                Parameters = {
                  FunctionName = aws_lambda_function.ingestion_lambda.function_name
                  Payload = {
                    entity_name      = "Orders"
                    expand_column    = "Order_Details"
                    watermark_column = "OrderDate"
                    primary_key      = "OrderID"
                    is_fact          = true
                    schema_overrides = {
                      UnitPrice = "Float64"
                      Quantity  = "Int64"
                      Discount  = "Float64"
                      Freight   = "Float64"
                    }
                    "execution_id.$"      = "$$.Execution.Id"
                    "start_chunk_index.$" = "$.orders_state.start_chunk_index"
                    "resume_url.$"        = "$.orders_state.resume_url"
                    "watermark_in.$"      = "$.orders_state.watermark_in"
                  }
                }
                ResultPath = "$.orders_response"
                Retry = [
                  {
                    ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException"]
                    IntervalSeconds = 2
                    MaxAttempts     = 3
                    BackoffRate     = 2.0
                  }
                ]
                Catch = [
                  {
                    ErrorEquals = ["States.ALL"]
                    ResultPath  = "$.error_info"
                    Next        = "NotifyOrdersFailureSNS"
                  }
                ]
                Next = "EvaluateOrdersPartial"
              }

              EvaluateOrdersPartial = {
                Type = "Choice"
                Choices = [
                  {
                    Variable      = "$.orders_response.Payload.is_partial"
                    BooleanEquals = true
                    Next          = "PrepareOrdersResumePayload"
                  }
                ]
                Default = "OrdersIngestionComplete"
              }

              PrepareOrdersResumePayload = {
                Type = "Pass"
                Parameters = {
                  orders_state = {
                    "start_chunk_index.$" = "$.orders_response.Payload.next_chunk_index"
                    "resume_url.$"        = "$.orders_response.Payload.resume_url"
                    "watermark_in.$"      = "$.orders_response.Payload.watermark_out"
                  }
                }
                Next = "InvokeOrdersLambda"
              }

              OrdersIngestionComplete = {
                Type = "Pass"
                Parameters = {
                  "final_watermark.$" = "$.orders_response.Payload.watermark_out"
                  "rows_ingested.$"   = "$.orders_response.Payload.rows_ingested"
                  "partition_path.$"  = "$.orders_response.Payload.partition_path"
                }
                End = true
              }

              NotifyOrdersFailureSNS = {
                Type     = "Task"
                Resource = "arn:aws:states:::sns:publish"
                Parameters = {
                  TopicArn    = aws_sns_topic.pipeline_alerts.arn
                  Subject     = "ALARM: Orders Ingestion Failed"
                  "Message.$" = "States.JsonToString($)"
                }
                Next = "OrdersTaskFailed"
              }

              OrdersTaskFailed = {
                Type  = "Fail"
                Cause = "Orders Ingestion Lambda Failed"
              }
            }
          }
        ]
      }

      # ------------------------------------------------------------------------
      # STEP 2: EXTRACT ORDERS WATERMARK FROM ARRAY INDEX 1 ($)
      # ------------------------------------------------------------------------
      ExtractOrdersWatermark = {
        Type = "Pass"
        Parameters = {
          "orders_final_watermark.$" = "$.final_watermark"
          "orders_partition_path.$"  = "$.partition_path"
        }
        Next = "CheckIfWatermarkExists"
      }

      CheckIfWatermarkExists = {
        Type = "Choice"
        Choices = [
          {
            Variable  = "$.orders_final_watermark"
            IsPresent = true
            Next      = "CommitOrdersSSMWatermark"
          }
        ]
        Default = "IngestionWorkflowSuccess"
      }

      # ------------------------------------------------------------------------
      # STEP 3: ATOMIC COMMIT SSM WATERMARK (Native AWS SDK Integration)
      # ------------------------------------------------------------------------
      CommitOrdersSSMWatermark = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:ssm:putParameter"
        Parameters = {
          Name        = "/${var.project_name}/watermarks/orders"
          "Value.$"   = "$.orders_final_watermark"
          Type        = "String"
          Overwrite   = true
          Description = "Atomic Event-Time Watermark committed by Step Functions Ingestion Orchestrator"
        }
        Next = "IngestionWorkflowSuccess"
      }

      # ------------------------------------------------------------------------
      # STEP 4: WORKFLOW COMPLETION
      # ------------------------------------------------------------------------
      IngestionWorkflowSuccess = {
        Type = "Succeed"
      }
    }
  })
}
