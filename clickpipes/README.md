# Configure the Redpanda → ClickHouse ClickPipe

ClickPipes is ClickHouse Cloud's built-in managed ingestion service. It runs a
managed Kafka consumer that reads from your Redpanda topic and inserts into your
ClickHouse table, with no connector to host yourself.

You can set it up three ways. **The UI is recommended for a first demo.**

---

## Option A: ClickHouse Cloud UI (recommended)

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

## Option B: `clickhousectl` (CLI)

Requires `clickhousectl` v0.2.0+ and a ClickHouse Cloud API key. Log in with API
key auth first (OAuth login is read-only and cannot create pipes):

```bash
clickhousectl cloud auth login --api-key <KEY_ID> --api-secret <KEY_SECRET>
```

Then create the pipe. The `--column name:type` mappings are **required**, even
when the destination table already exists (the API rejects the request without
them). Source the Redpanda credentials from `.env` so the password is not typed
on the command line:

```bash
set -a && source ../.env && set +a

clickhousectl cloud clickpipe create kafka <SERVICE_ID> \
  --name redpanda-clickstream-demo \
  --kafka-type redpanda \
  --brokers "$REDPANDA_BROKERS" \
  --topics "$REDPANDA_TOPIC" \
  --format JSONEachRow \
  --auth SCRAM-SHA-256 \
  --username "$REDPANDA_USERNAME" \
  --password "$REDPANDA_PASSWORD" \
  --offset from_beginning \
  --consumer-group clickpipes-redpanda-clickstream-demo \
  --database default \
  --table clickstream_events \
  --column "event_id:UUID" \
  --column "event_time:DateTime64(3)" \
  --column "event_type:LowCardinality(String)" \
  --column "user_id:UInt64" \
  --column "session_id:UUID" \
  --column "url:String" \
  --column "referrer:String" \
  --column "device:LowCardinality(String)" \
  --column "browser:LowCardinality(String)" \
  --column "country:LowCardinality(String)" \
  --column "price:Decimal(10, 2)"
```

Get `<SERVICE_ID>` from `clickhousectl cloud service list`.

---

## Option C: Terraform

See [`terraform/`](terraform/). Requires the `ClickHouse/clickhouse` provider
v3.14.0+ and a ClickHouse Cloud API key/secret.

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # fill in values
terraform init
terraform apply
```
