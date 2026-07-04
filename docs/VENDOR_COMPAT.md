# Vendor Compatibility Matrix

> **Last reviewed:** 2026-07
>
> This document describes which Kafka-compatible platforms KafkaMCP has been
> exercised against, and provides configuration guidance (not certification) for
> managed services.
>
> **A row is only added after a version has been exercised against the local
> integration harness. Unverified claims are not listed.**

## Compatibility matrix

| Platform | Version tested | Core-read | Ops-write | Schema Registry | SASL/TLS | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| **Apache Kafka** | `confluentinc/cp-kafka:7.6.0` | ✓ | ✓* | not exercised | PLAINTEXT locally | Contract suite passed 2026-07-26 |
| **Redpanda** | `redpandadata/redpanda:v24.1.3` | ✓ | ✓* | not exercised | PLAINTEXT locally | Contract suite passed 2026-07-26 |
| **Amazon MSK** | _profile only_ | — | — | — | — | Configuration guidance provided; not certified |
| **Confluent Cloud** | _profile only_ | — | — | — | — | Configuration guidance provided; not certified |

`✓*` covers topic lifecycle/config, produce/consume, headers, metadata, and
consumer-group basics; it does not certify Kafka Connect or ACL mutation.

> **Important:** rows marked "_profile only_" have not been
> exercised against a live cluster in this repository's CI. Do not infer that
> they work; verify in your environment.

## Running the compatibility harness

The local integration harness is the reference point for adding a verified row.

```bash
# Apache Kafka
make test-integration-setup
KAFKA_PROVIDER=kafka KAFKA_BROKERS=localhost:9092 \
  go test -race -tags=integration -run TestKafkaProviderContract ./internal/kafka
docker compose -f examples/docker-compose.yaml down -v

# Redpanda
make incident-demo-up-redpanda
KAFKA_PROVIDER=redpanda KAFKA_BROKERS=localhost:9092 \
  go test -race -tags=integration -run TestKafkaProviderContract ./internal/kafka
make incident-demo-down-redpanda
```

When you exercise a new platform version:

1. Run the harness and note any failures.
2. Open a PR adding the verified row with your platform version and any known
   limitations.
3. Include the output of `make test` or the specific integration test suite.

## Configuration guidance (not certification)

The following sections provide YAML configuration templates for managed services.
These are derived from vendor documentation at the date noted and have not been
certified by the vendor or verified in KafkaMCP's CI.

### Amazon MSK (SASL/SCRAM)

_Guidance derived from AWS MSK public documentation, 2026-07. Not certified._

```yaml
clusters:
  - name: msk-production
    bootstrap_servers: "${MSK_BOOTSTRAP_SERVERS}"  # e.g. b-1.xxx.kafka.us-east-1.amazonaws.com:9096
    security_protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA-512
    sasl_username: "${MSK_USERNAME}"
    sasl_password: "${MSK_PASSWORD}"
    ssl_ca_location: "/etc/ssl/certs/ca-certificates.crt"  # system CA bundle
    default: true
```

**Notes:**
- MSK does not provide a managed Schema Registry; use Glue Schema Registry or a
  self-managed Confluent Schema Registry.
- IAM authentication (`SASL_IAM`) is not currently supported by KafkaMCP's SASL
  implementation.
- Verify that your MSK cluster security group allows inbound traffic on port 9096
  (SASL_SSL) from your KafkaMCP deployment.

### Confluent Cloud (SASL/PLAIN)

_Guidance derived from Confluent Cloud public documentation, 2026-07. Not certified._

```yaml
clusters:
  - name: confluent-cloud
    bootstrap_servers: "${CONFLUENT_BOOTSTRAP_SERVERS}"  # e.g. pkc-xxx.us-east1.gcp.confluent.cloud:9092
    security_protocol: SASL_SSL
    sasl_mechanism: PLAIN
    sasl_username: "${CONFLUENT_API_KEY}"
    sasl_password: "${CONFLUENT_API_SECRET}"
    schema_registry:
      url: "${CONFLUENT_SR_URL}"
      auth:
        username: "${CONFLUENT_SR_API_KEY}"
        password: "${CONFLUENT_SR_API_SECRET}"
    default: true
```

**Notes:**
- Use Confluent Cloud API keys for both the broker and Schema Registry.
- Do **not** hardcode API keys in `kafkamcp.yaml`; use `${ENV_VAR}` expansion.
- Confluent Cloud's Schema Registry URL follows the pattern
  `https://psrc-xxx.us-east-2.aws.confluent.cloud`.
- Rate limits on Confluent Cloud Schema Registry may affect schema-intensive
  workloads; set `schema_cache.ttl_seconds` to reduce API calls.

### Redpanda (SASL/SCRAM)

The self-hosted profile was verified with the local provider contract on
2026-07-26. Redpanda Cloud remains configuration guidance only.

```yaml
clusters:
  - name: redpanda
    bootstrap_servers: "${REDPANDA_BOOTSTRAP_SERVERS}"
    security_protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA-256
    sasl_username: "${REDPANDA_USERNAME}"
    sasl_password: "${REDPANDA_PASSWORD}"
    schema_registry:
      url: "${REDPANDA_SR_URL}"
    default: true
```

**Notes:**
- The verified contract covers the core Kafka protocol operations listed above;
  future deviations will be documented here.
- Redpanda Cloud uses SASL/SCRAM-SHA-256; Redpanda self-hosted can be configured
  with SASL or mTLS.

### Aiven for Apache Kafka

_Guidance derived from Aiven public documentation, 2026-07. Not certified._

```yaml
clusters:
  - name: aiven-kafka
    bootstrap_servers: "${AIVEN_SERVICE_HOST}:${AIVEN_SERVICE_PORT}"
    security_protocol: SSL
    ssl_ca_location: "${AIVEN_CA_CERT_PATH}"     # path to ca.pem from Aiven console
    # If client cert auth is required:
    # ssl_cert_location: "${AIVEN_CLIENT_CERT_PATH}"
    # ssl_key_location: "${AIVEN_CLIENT_KEY_PATH}"
    default: true
```

**Notes:**
- Aiven uses mTLS by default; download the CA and client certificates from the
  Aiven Console.
- Aiven's Karapace schema registry is Confluent-compatible; use its URL in
  `schema_registry.url`.

## What "profile only" means

A "_profile only_" entry means:

- A plausible configuration template has been written based on vendor
  documentation.
- It has **not** been tested end-to-end in this repository.
- It may work, partially work, or fail depending on your specific cluster
  configuration.
- Treat it as a starting point, not a tested recipe.

## Adding a verified row

To upgrade a "_profile only_" row to a verified row:

1. Set up a cluster with the target platform.
2. Run `make docker-dev` or point your `kafkamcp.yaml` at the cluster.
3. Run the full test suite or at minimum the core-read integration tests.
4. Document the platform version, any known limitations, and the test command
   used.
5. Open a PR to update this file with the verified row.

See [docs/COMMUNITY.md](COMMUNITY.md) for labeling conventions (`area:compatibility`).
