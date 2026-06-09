variable "organization_id" {
  description = "ClickHouse Cloud organization ID"
  type        = string
}

variable "token_key" {
  description = "ClickHouse Cloud API key ID"
  type        = string
  sensitive   = true
}

variable "token_secret" {
  description = "ClickHouse Cloud API key secret"
  type        = string
  sensitive   = true
}

variable "service_id" {
  description = "ClickHouse Cloud service ID that will receive the data"
  type        = string
}

variable "redpanda_brokers" {
  description = "Redpanda Serverless bootstrap server, host:port"
  type        = string
}

variable "redpanda_topic" {
  description = "Source topic"
  type        = string
  default     = "clickstream_events"
}

variable "redpanda_username" {
  description = "Redpanda SASL username"
  type        = string
  sensitive   = true
}

variable "redpanda_password" {
  description = "Redpanda SASL password"
  type        = string
  sensitive   = true
}
