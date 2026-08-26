# ==============================================================================
# ORCHESTRATION LAYER - AWS STEP FUNCTIONS STATE MACHINE & DLQ
# ==============================================================================

resource "aws_sns_topic" "pipeline_alerts" {
  name = "${var.project_name}-pipeline-alerts"

  tags = {
    Component = "Monitoring-DLQ"
  }
}

resource "aws_cloudwatch_log_group" "step_functions_logs" {
  name              = "/aws/vendedlogs/states/${var.project_name}-lakehouse-orchestrator"
  retention_in_days = 14
}

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
  description = "Granular policy allowing Step Functions to invoke Lambda, commit SSM, execute Athena Sync, and publish SNS alerts"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = [
          aws_lambda_function.ingestion_lambda.arn,
          "${aws_lambda_function.ingestion_lambda.arn}:*",
          aws_lambda_function.sql_dispatcher.arn,
          "${aws_lambda_function.sql_dispatcher.arn}:*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "ssm:PutParameter",
          "ssm:GetParameter"
        ]
        Resource = "arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter/${var.project_name}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup"
        ]
        Resource = [
          aws_athena_workgroup.lakehouse_workgroup.arn,
          "arn:aws:athena:${var.aws_region}:${data.aws_caller_identity.current.account_id}:workgroup/${var.project_name}-workgroup"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartitions",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:BatchCreatePartition"
        ]
        Resource = "*"
      },
      # S3 ACCESS: Includes s3:DeleteObject for Apache Iceberg ACID Vacuuming & Snapshot Mutation
      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload",
          "s3:CreateBucket",
          "s3:PutObject",
          "s3:DeleteObject" # <-- MANDATORY FOR ICEBERG ACID MERGE COMPACTION
        ]
        Resource = [
          aws_s3_bucket.raw_zone.arn,
          "${aws_s3_bucket.raw_zone.arn}/*",
          aws_s3_bucket.iceberg_warehouse.arn,
          "${aws_s3_bucket.iceberg_warehouse.arn}/*",
          aws_s3_bucket.athena_results.arn,
          "${aws_s3_bucket.athena_results.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = aws_sns_topic.pipeline_alerts.arn
      },
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

resource "aws_sfn_state_machine" "ingestion_orchestrator" {
  name     = "${var.project_name}-lakehouse-orchestrator"
  role_arn = aws_iam_role.step_functions_role.arn

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_functions_logs.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  definition = jsonencode({
    Comment = "Serverless ERP Lakehouse - End-to-End Ingestion & Athena Iceberg Orchestrator"
    StartAt = "BootstrapIcebergSchema"
    States = {
      # ------------------------------------------------------------------------
      # STEP 0: BOOTSTRAP ICEBERG & STAGING SCHEMA (Solves Chicken-and-Egg DDL)
      # ------------------------------------------------------------------------
      BootstrapIcebergSchema = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.sql_dispatcher.function_name
          Payload = {
            "action" = "bootstrap_ddl"
          }
        }
        ResultPath = "$.bootstrap_result"
        Retry = [
          {
            ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"]
            IntervalSeconds = 2
            MaxAttempts     = 3
            BackoffRate     = 2.0
          }
        ]
        Next = "InjectDefaultState"
      }

      # ------------------------------------------------------------------------
      # STEP 1: INJECT DEFAULT STATE
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
      # STEP 2: PARALLEL BRONZE INGESTION
      # ------------------------------------------------------------------------
      ParallelIngestion = {
        Type = "Parallel"
        Next = "ExtractOrdersWatermark"
        Branches = [
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
      # STEP 3: EXTRACT ORDERS WATERMARK & PARTITION FROM ARRAY INDEX 1 ($)
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
        Default = "PrepareSilverPayload"
      }

      # ------------------------------------------------------------------------
      # STEP 4: ATOMIC COMMIT SSM WATERMARK
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
        Next = "PrepareSilverPayload"
      }

      # ------------------------------------------------------------------------
      # STEP 5: PREPARE DETERMINISTIC SILVER PAYLOAD
      # ------------------------------------------------------------------------
      PrepareSilverPayload = {
        Type = "Pass"
        Parameters = {
          "silver_context" = {
            "partition_path.$" = "$.orders_partition_path"
            "entities" = [
              "dim_customers",
              "dim_products",
              "dim_employees",
              "dim_suppliers",
              "fact_order_details"
            ]
          }
        }
        Next = "SilverTransformationMap"
      }

      # ------------------------------------------------------------------------
      # STEP 6: SILVER TRANSFORMATION MAP (Zero-Idle Synchronous Athena Execution)
      # ------------------------------------------------------------------------
      SilverTransformationMap = {
        Type           = "Map"
        MaxConcurrency = 3
        ItemsPath      = "$.silver_context.entities"
        ItemSelector = {
          "entity_name.$"    = "$$.Map.Item.Value"
          "partition_path.$" = "$.silver_context.partition_path"
        }
        End = true
        ItemProcessor = {
          ProcessorConfig = { Mode = "INLINE" }
          StartAt         = "RenderSQLEntity"
          States = {
            # 1. Micro-Dispatcher: Render .sql Template (< 5ms)
            RenderSQLEntity = {
              Type     = "Task"
              Resource = "arn:aws:states:::lambda:invoke"
              Parameters = {
                FunctionName = aws_lambda_function.sql_dispatcher.function_name
                Payload = {
                  "action"           = "render_sql"
                  "entity_name.$"    = "$.entity_name"
                  "partition_path.$" = "$.partition_path"
                }
              }
              ResultPath = "$.sql_render_result"
              Next       = "ExecuteAthenaMerge"
            }

            # 2. Athena Synchronous MERGE Execution (Zero-Idle Cost)
            ExecuteAthenaMerge = {
              Type     = "Task"
              Resource = "arn:aws:states:::athena:startQueryExecution.sync"
              Parameters = {
                "QueryString.$" = "$.sql_render_result.Payload.query_string"
                WorkGroup       = aws_athena_workgroup.lakehouse_workgroup.name
                ResultConfiguration = {
                  OutputLocation = "s3://${aws_s3_bucket.athena_results.id}/"
                }
              }
              Retry = [
                {
                  ErrorEquals     = ["Athena.TooManyRequestsException", "Athena.ResourceLimitExceededException"]
                  IntervalSeconds = 5
                  MaxAttempts     = 3
                  BackoffRate     = 2.0
                }
              ]
              Catch = [
                {
                  ErrorEquals = ["States.ALL"]
                  ResultPath  = "$.error_info"
                  Next        = "NotifyAthenaFailureSNS"
                }
              ]
              End = true
            }

            # 3. SRE Dead-Letter Alert on Fatal Failure
            NotifyAthenaFailureSNS = {
              Type     = "Task"
              Resource = "arn:aws:states:::sns:publish"
              Parameters = {
                TopicArn    = aws_sns_topic.pipeline_alerts.arn
                Subject     = "ALARM: Athena Silver Transformation Failed"
                "Message.$" = "States.JsonToString($)"
              }
              Next = "AthenaTaskFailed"
            }

            AthenaTaskFailed = {
              Type  = "Fail"
              Cause = "Athena MERGE INTO Query Execution Failed"
            }
          }
        }
      }
    }
  })
}
