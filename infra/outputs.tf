output "s3_bucket_name" {
  description = "S3 data lake bucket name"
  value       = aws_s3_bucket.datalake.bucket
}

output "s3_bucket_arn" {
  description = "S3 data lake bucket ARN"
  value       = aws_s3_bucket.datalake.arn
}

output "kinesis_stream_name" {
  description = "Kinesis Data Stream name"
  value       = aws_kinesis_stream.flights.name
}

output "kinesis_stream_arn" {
  description = "Kinesis Data Stream ARN"
  value       = aws_kinesis_stream.flights.arn
}

output "lambda_function_name" {
  description = "Lambda function name"
  value       = aws_lambda_function.opensky_poller.function_name
}

output "lambda_function_arn" {
  description = "Lambda function ARN"
  value       = aws_lambda_function.opensky_poller.arn
}

output "firehose_stream_name" {
  description = "Kinesis Firehose delivery stream name"
  value       = aws_kinesis_firehose_delivery_stream.flights_to_s3.name
}

output "databricks_iam_role_arn" {
  description = "IAM role ARN for Databricks — use this in Databricks workspace AWS settings"
  value       = aws_iam_role.databricks_role.arn
}

output "bronze_s3_path" {
  description = "S3 path for Bronze raw data (use in Databricks Auto Loader)"
  value       = "s3://${aws_s3_bucket.datalake.bucket}/bronze/raw_states/"
}
