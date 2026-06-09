terraform {
  required_providers {
    clickhouse = {
      source  = "ClickHouse/clickhouse"
      version = ">= 3.14.0"
    }
  }
}

provider "clickhouse" {
  organization_id = var.organization_id
  token_key       = var.token_key
  token_secret    = var.token_secret
}

# Creates a managed Kafka ClickPipe that reads the Redpanda topic and writes
# into the existing `default.clickstream_events` table (created via
# ../../clickhouse/01_schema.sql). managed_table = false => use existing table.
resource "clickhouse_clickpipe" "redpanda_clickstream" {
  name       = "redpanda-clickstream-demo"
  service_id = var.service_id

  scaling {
    replicas = 1
  }

  source {
    kafka {
      type           = "redpanda"
      format         = "JSONEachRow"
      brokers        = var.redpanda_brokers
      topics         = var.redpanda_topic
      consumer_group = "clickpipes-redpanda-clickstream-demo"
      authentication = "SCRAM-SHA-256"

      credentials {
        username = var.redpanda_username
        password = var.redpanda_password
      }

      offset {
        strategy = "from_beginning"
      }
    }
  }

  destination {
    database      = "default"
    table         = "clickstream_events"
    managed_table = false
  }
}
