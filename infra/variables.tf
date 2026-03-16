variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name prefix used for all resource names"
  type        = string
  default     = "skystream"
}

variable "s3_bucket_name" {
  description = "Name of the S3 data lake bucket (must be globally unique)"
  type        = string
  default     = "skystream-datalake-dev"
}

variable "kinesis_stream_name" {
  description = "Name of the Kinesis Data Stream"
  type        = string
  default     = "flights-stream"
}

variable "kinesis_shard_count" {
  description = "Number of shards for the Kinesis stream (1 shard = ~1,000 records/sec)"
  type        = number
  default     = 2
}

variable "opensky_username" {
  description = "OpenSky Network username (optional — increases rate limit from 400 to 4,000 req/day)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "opensky_password" {
  description = "OpenSky Network password"
  type        = string
  default     = ""
  sensitive   = true
}

variable "poll_interval_seconds" {
  description = "Seconds between each OpenSky API poll within one Lambda invocation"
  type        = number
  default     = 10
}

variable "poll_count" {
  description = "Number of polls per Lambda invocation (invocation runs every 1 minute)"
  type        = number
  default     = 6
}

# Bounding box for region filtering (default = whole world)
variable "bbox_lamin" {
  description = "Bounding box: minimum latitude"
  type        = number
  default     = -90
}

variable "bbox_lomin" {
  description = "Bounding box: minimum longitude"
  type        = number
  default     = -180
}

variable "bbox_lamax" {
  description = "Bounding box: maximum latitude"
  type        = number
  default     = 90
}

variable "bbox_lomax" {
  description = "Bounding box: maximum longitude"
  type        = number
  default     = 180
}

variable "databricks_aws_account_id" {
  description = "Databricks production AWS account ID (for cross-account role trust)"
  type        = string
  default     = "414351767826"
}

variable "databricks_external_id" {
  description = "Databricks External ID for IAM role (from Databricks workspace > Settings > AWS)"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Tags applied to all AWS resources"
  type        = map(string)
  default = {
    Project     = "SkyStream Analytics"
    Environment = "dev"
    ManagedBy   = "terraform"
  }
}
