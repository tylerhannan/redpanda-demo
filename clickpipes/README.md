# Configure the Redpanda → ClickHouse ClickPipe

ClickPipes is ClickHouse Cloud's built-in managed ingestion service. It runs a
managed Kafka consumer that reads from your Redpanda topic and inserts into your
ClickHouse table, with no connector to host yourself.

You can set it up three ways. **The UI is recommended for a first demo.**

---

## Option A — ClickHouse Cloud UI (recommended)

1. Open your ClickHouse Cloud service → left sidebar → **Data sources** →
   **Add new** (a.k.a. **ClickPipes**) → **Apache Kafka** → choose **Redpanda**.

2. **Connection**
   - **Broker(s):** your Redpanda Serverless bootstrap server, e.g.
     `seed-xxxxxxxx.xxx.byoc.prd.cloud.redpanda.com:9092`
   - **Authentication:** `SCRAM-SHA-256`
   - **Username / Password:** the Redpanda SASL user you created
     (Cluster → **Security** → **Users**).
   - TLS is on by default for Redpanda Serverless (the broker uses a public CA,
     so no custom certificate is needed).
   - Click **Next**, and ClickPipes will list your topics.

3. **Topic & format**
   - **Topic:** `clickstream_events`
   - **Offset / consumer start:** *From beginning* (to ingest data already in the
     topic) or *Latest* (only new messages).
   - **Format:** `JSONEachRow`

4. **Schema / destination table**
   - Choose **Use an existing table** and select `default.clickstream_events`
     (created from [`../clickhouse/01_schema.sql`](../clickhouse/01_schema.sql)).
   - ClickPipes maps JSON keys to columns by name. Since the producer emits keys
     that match the column names exactly, the mapping is automatic.
   - *(Alternatively, let ClickPipes create the table for you, but the explicit
     DDL gives you the tuned `ORDER BY`, partitioning, and TTL.)*

5. **Permissions:** pick the default ClickPipes user/role, then **Create the
   ClickPipe**.

6. Watch the pipe go **Running**. Start the producer (see the root `README.md`),
   then run [`../clickhouse/02_verify.sql`](../clickhouse/02_verify.sql).

---

## Option B — `clickhousectl` (CLI)

Requires `clickhousectl` v0.2.0+ and a ClickHouse Cloud API key.

```bash
clickhousectl cloud clickpipe create kafka <SERVICE_ID> \
  --name redpanda-clickstream-demo \
  --kafka-type redpanda \
  --brokers "$REDPANDA_BROKERS" \
  --topics clickstream_events \
  --format JSONEachRow \
  --auth SCRAM-SHA-256 \
  --username "$REDPANDA_USERNAME" \
  --password "$REDPANDA_PASSWORD" \
  --database default \
  --table clickstream_events
```

---

## Option C — Terraform

See [`terraform/`](terraform/). Requires the `ClickHouse/clickhouse` provider
v3.14.0+ and a ClickHouse Cloud API key/secret.

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # fill in values
terraform init
terraform apply
```
