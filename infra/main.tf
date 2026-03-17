terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ─────────────────────────────────────────────
# S3 Data Lake Bucket
# ─────────────────────────────────────────────
resource "aws_s3_bucket" "datalake" {
  bucket = var.s3_bucket_name
  tags   = var.tags
}

resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket                  = aws_s3_bucket.datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    id     = "expire-bronze-30d"
    status = "Enabled"
    filter { prefix = "bronze/" }
    expiration { days = 30 }
  }

  rule {
    id     = "expire-silver-90d"
    status = "Enabled"
    filter { prefix = "silver/" }
    expiration { days = 90 }
  }

  rule {
    id     = "expire-checkpoints-7d"
    status = "Enabled"
    filter { prefix = "_checkpoints/" }
    expiration { days = 7 }
  }
}

# ─────────────────────────────────────────────
# Kinesis Data Stream
# ─────────────────────────────────────────────
resource "aws_kinesis_stream" "flights" {
  name             = var.kinesis_stream_name
  shard_count      = var.kinesis_shard_count
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = var.tags
}

# ─────────────────────────────────────────────
# IAM Role for Lambda
# ─────────────────────────────────────────────
resource "aws_iam_role" "lambda_role" {
  name = "${var.project_name}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.project_name}-lambda-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:PutRecord",
          "kinesis:PutRecords",
          "kinesis:DescribeStream",
          "kinesis:ListShards"
        ]
        Resource = aws_kinesis_stream.flights.arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# ─────────────────────────────────────────────
# Lambda Function (OpenSky Poller)
# ─────────────────────────────────────────────
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../ingestion/lambda_producer/handler.py"
  output_path = "${path.module}/lambda_function.zip"
}

resource "aws_lambda_function" "opensky_poller" {
  function_name    = "${var.project_name}-opensky-poller"
  role             = aws_iam_role.lambda_role.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.11"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 75 # 6 polls × 10s + overhead
  memory_size      = 256

  environment {
    variables = {
      KINESIS_STREAM_NAME   = aws_kinesis_stream.flights.name
      POLL_INTERVAL_SECONDS = tostring(var.poll_interval_seconds)
      POLL_COUNT            = tostring(var.poll_count)
      OPENSKY_USERNAME      = var.opensky_username
      OPENSKY_PASSWORD      = var.opensky_password
      AWS_REGION_NAME       = var.aws_region
      BBOX_LAMIN            = tostring(var.bbox_lamin)
      BBOX_LOMIN            = tostring(var.bbox_lomin)
      BBOX_LAMAX            = tostring(var.bbox_lamax)
      BBOX_LOMAX            = tostring(var.bbox_lomax)
    }
  }

  tags = var.tags
}

resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${aws_lambda_function.opensky_poller.function_name}"
  retention_in_days = 7
  tags              = var.tags
}

# ─────────────────────────────────────────────
# EventBridge Rule — Trigger Lambda Every Minute
# ─────────────────────────────────────────────
resource "aws_cloudwatch_event_rule" "every_minute" {
  name                = "${var.project_name}-poll-every-minute"
  description         = "Triggers OpenSky poller Lambda every 1 minute"
  schedule_expression = "rate(1 minute)"
  tags                = var.tags
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.every_minute.name
  target_id = "OpenskyPollerLambda"
  arn       = aws_lambda_function.opensky_poller.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.opensky_poller.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.every_minute.arn
}

# ─────────────────────────────────────────────
# IAM Role for Kinesis Firehose
# ─────────────────────────────────────────────
resource "aws_iam_role" "firehose_role" {
  name = "${var.project_name}-firehose-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "firehose.amazonaws.com" }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "firehose_policy" {
  name = "${var.project_name}-firehose-policy"
  role = aws_iam_role.firehose_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetBucketLocation",
          "s3:ListBucket",
          "s3:GetObject"
        ]
        Resource = [
          aws_s3_bucket.datalake.arn,
          "${aws_s3_bucket.datalake.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream",
          "kinesis:ListShards",
          "kinesis:SubscribeToShard"
        ]
        Resource = aws_kinesis_stream.flights.arn
      },
      {
        Effect   = "Allow"
        Action   = ["logs:PutLogEvents"]
        Resource = "*"
      }
    ]
  })
}

# ─────────────────────────────────────────────
# Kinesis Firehose → S3 Bronze
# ─────────────────────────────────────────────
resource "aws_cloudwatch_log_group" "firehose_logs" {
  name              = "/aws/kinesisfirehose/${var.project_name}-flights-to-s3"
  retention_in_days = 7
  tags              = var.tags
}

resource "aws_cloudwatch_log_stream" "firehose_log_stream" {
  name           = "DestinationDelivery"
  log_group_name = aws_cloudwatch_log_group.firehose_logs.name
}

resource "aws_kinesis_firehose_delivery_stream" "flights_to_s3" {
  name        = "${var.project_name}-flights-to-s3"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.flights.arn
    role_arn           = aws_iam_role.firehose_role.arn
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose_role.arn
    bucket_arn = aws_s3_bucket.datalake.arn

    prefix              = "bronze/raw_states/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "bronze/errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"

    buffering_size     = 64  # MB — Auto Loader reads entire files; larger = fewer files
    buffering_interval = 60  # seconds (minimum allowed)
    compression_format = "GZIP"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose_logs.name
      log_stream_name = aws_cloudwatch_log_stream.firehose_log_stream.name
    }
  }

  tags = var.tags
}

# ─────────────────────────────────────────────
# IAM Role for Databricks (cross-account access to S3 + Kinesis)
# ─────────────────────────────────────────────
resource "aws_iam_role" "databricks_role" {
  name = "${var.project_name}-databricks-role"

  assume_role_policy = var.databricks_external_id != "" ? jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${var.databricks_aws_account_id}:root" }
      Condition = { StringEquals = { "sts:ExternalId" = var.databricks_external_id } }
    }]
  }) : jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${var.databricks_aws_account_id}:root" }
    }]
  })

  tags = var.tags
}

resource "aws_iam_instance_profile" "databricks_instance_profile" {
  name = "${var.project_name}-databricks-role"
  role = aws_iam_role.databricks_role.name
}

resource "aws_iam_role_policy" "databricks_policy" {
  name = "${var.project_name}-databricks-policy"
  role = aws_iam_role.databricks_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetBucketNotification",
          "s3:PutBucketNotification"
        ]
        Resource = [
          aws_s3_bucket.datalake.arn,
          "${aws_s3_bucket.datalake.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream",
          "kinesis:ListShards",
          "kinesis:SubscribeToShard",
          "kinesis:ListStreams"
        ]
        Resource = aws_kinesis_stream.flights.arn
      }
    ]
  })
}
