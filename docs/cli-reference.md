# CLI Reference

The `eventlens` command line tool provides subcommands for stream tailing, consumer lag inspection, dead letter queue triage, traffic recording and replay, distributed event tracing, structural diffing, producing test events, and launching interactive TUI and Web dashboards.

## Global Options

- `--broker, -b TEXT`: Comma-separated Kafka broker bootstrap servers. Default: `localhost:9092`.
- `--config, -c PATH`: Path to optional YAML or JSON configuration file.
- `--verbose, -v`: Enable detailed debug logging.
- `--help`: Show command options and usage descriptions.

## Subcommands

### `eventlens tail`

Streams events from a Kafka topic to the terminal in real time.

```bash
eventlens tail <TOPIC> [OPTIONS]
```

**Options:**
- `--from-beginning`: Read messages starting from offset 0 instead of the latest offset.
- `--tail INTEGER`: Read only the last N messages then continue streaming new events.
- `--partition INTEGER`: Restrict consumption to a specific partition ID.
- `--filter TEXT`: Apply a JMESPath filter expression against decoded message payloads.
- `--regex TEXT`: Filter messages matching a regular expression.
- `--proto PATH`: Path to a `.proto` file for dynamic Protobuf deserialization.
- `--message-type TEXT`: Protobuf message name defined in the provided `.proto` file.
- `--schema-registry TEXT`: URL to Confluent Schema Registry (e.g. `http://localhost:8081`).
- `--format [table|json|raw]`: Output format for records. Default: `table`.
- `--max-messages INTEGER`: Stop streaming after receiving N matching messages.

**Examples:**
```bash
# Stream all new events from order.events
eventlens tail order.events

# Stream the last 20 messages from partition 0
eventlens tail order.events --tail 20 --partition 0

# Filter failed orders using JMESPath
eventlens tail order.events --filter "status == 'FAILED'"
```

---

### `eventlens lag`

Checks consumer group offsets and partition lag against log-end watermarks.

```bash
eventlens lag <TOPIC> [OPTIONS]
```

**Options:**
- `--group, -g TEXT`: Consumer group ID to query.
- `--refresh-interval FLOAT`: Poll interval in seconds for continuous lag monitoring. Default: 3.0.
- `--watch`: Keep polling and updating lag metrics in place.

**Examples:**
```bash
# Check lag for a consumer group once
eventlens lag order.events --group order-worker-group

# Continuously watch consumer group lag
eventlens lag order.events --group order-worker-group --watch
```

---

### `eventlens dlq`

Tools for inspecting, analyzing, and redriving messages in dead letter queues.

#### `eventlens dlq inspect`

Lists messages in a DLQ topic with extracted error metadata.

```bash
eventlens dlq inspect <DLQ_TOPIC> [OPTIONS]
```

**Options:**
- `--limit INTEGER`: Number of poisoned messages to inspect. Default: 50.
- `--from-beginning`: Scan from offset 0. Default: true.
- `--diagnose`: Run the heuristic diagnostic engine on each error stack trace.

#### `eventlens dlq redrive`

Republishes an edited or original poisoned message back to a target topic.

```bash
eventlens dlq redrive <DLQ_TOPIC> --offset INTEGER --target <TARGET_TOPIC> [OPTIONS]
```

**Options:**
- `--offset INTEGER`: Partition offset of the message to redrive (required).
- `--partition INTEGER`: Partition of the source message. Default: 0.
- `--target TEXT`: Destination topic name (required).
- `--patch-file PATH`: Optional JSON file containing modified payload data.
- `--strip-error-headers`: Remove error headers before republishing. Default: true.

#### `eventlens dlq redrive-batch`

Batch redrive dead-letter records with failure category filtering and dry-run preview.

```bash
eventlens dlq redrive-batch <DLQ_TOPIC> [OPTIONS]
```

**Options:**
- `--target, -t TEXT`: Destination topic (defaults to `x-original-topic` header).
- `--category, -c TEXT`: Filter by failure category keyword (e.g. `Outage`, `Syntax`, `Validation`).
- `--filter, -f TEXT`: JMESPath filter expression.
- `--dry-run`: Preview matching messages and destinations without publishing.
- `--limit, -l INTEGER`: Maximum number of records to redrive. Default: 50.
- `--demo`: Use simulated DLQ records.

**Examples:**
```bash
# Dry run preview of transient outage failures
eventlens dlq redrive-batch order.events.dlq --category Outage --dry-run

# Live redrive up to 100 messages to the orders topic
eventlens dlq redrive-batch order.events.dlq --category Outage --target order.events --limit 100
```

---

### `eventlens record`

Captures live traffic from a Kafka topic into a portable `.lens` session file (NDJSON).

```bash
eventlens record <TOPIC> -o <SESSION_FILE> [OPTIONS]
```

**Options:**
- `--output, -o PATH`: Destination `.lens` session file (required).
- `--max INTEGER`: Maximum number of records to capture.
- `--duration FLOAT`: Maximum recording duration in seconds.
- `--demo`: Simulate traffic recording.

**Examples:**
```bash
# Record 500 records into a session file
eventlens record order.events -o order_traffic.lens --max 500

# Record for 60 seconds
eventlens record order.events -o order_traffic.lens --duration 60.0
```

---

### `eventlens replay`

Replays a recorded `.lens` session file into a target topic with rate scaling.

```bash
eventlens replay <SESSION_FILE> [OPTIONS]
```

**Options:**
- `--target, -t TEXT`: Target topic to publish into (defaults to original recorded topic).
- `--speed, -s FLOAT`: Speed multiplier (e.g. `2.0` for 2x speed, `0.5` for half speed). Default: 1.0.
- `--loop`: Continuously replay the session in an infinite loop.
- `--dry-run`: Output replayed records without publishing to Kafka.

**Examples:**
```bash
# Replay recording into a shadow topic at 2x speed
eventlens replay order_traffic.lens --target shadow.orders --speed 2.0

# Dry-run inspect replayed stream
eventlens replay order_traffic.lens --dry-run
```

---

### `eventlens trace`

Traces the end-to-end distributed lifecycle of an event across multiple Kafka topics.

```bash
eventlens trace <CORRELATION_ID> [OPTIONS]
```

**Options:**
- `--topics TEXT`: Comma-separated list of topics to scan.
- `--demo`: Run in demo mode with a pre-recorded distributed transaction.

**Examples:**
```bash
# Trace an order ID across default topics
eventlens trace ord_9011

# Run demo trace
eventlens trace ord_9011 --demo
```

---

### `eventlens diff`

Compares two message payloads, JSON files, or topic offsets side-by-side.

```bash
eventlens diff <TARGET_A> <TARGET_B> [OPTIONS]
```

**Options:**
- `--demo`: Compare sample payloads showing additions, deletions, and modifications.

**Examples:**
```bash
# Compare two JSON strings
eventlens diff '{"status": "PENDING", "amount": 100}' '{"status": "CONFIRMED", "amount": 100}'

# Compare two local JSON files
eventlens diff baseline_order.json rejected_order.json

# Demo comparison
eventlens diff dummy dummy --demo
```

---

### `eventlens produce`

Publishes custom messages directly into a Kafka topic for testing and validation.

```bash
eventlens produce <TOPIC> [OPTIONS]
```

**Options:**
- `--key, -k TEXT`: Message key.
- `--json, -j TEXT`: JSON payload string.
- `--file, -f PATH`: Path to a JSON or text payload file.
- `--headers, -H TEXT`: Headers in `key:value,key:value` format.
- `--count, -n INTEGER`: Number of messages to publish. Default: 1.
- `--rate, -r FLOAT`: Publishing rate in messages per second. Default: 10.0.
- `--demo`: Simulate publishing without a live broker.

**Examples:**
```bash
# Publish a single JSON event
eventlens produce order.events --key ord_101 --json '{"order_id": "ord_101", "total": 99.0}'

# Publish 10 messages from a payload file
eventlens produce order.events --file payload.json --count 10 --rate 5.0
```

---

### `eventlens tui`

Launches the interactive full-screen terminal user interface.

```bash
eventlens tui [OPTIONS]
```

**Options:**
- `--topic TEXT`: Initial topic to subscribe to.
- `--demo`: Start the interface in offline demo mode with simulated traffic.

---

### `eventlens web`

Starts the local web dashboard and WebSocket bridge.

```bash
eventlens web [OPTIONS]
```

**Options:**
- `--host TEXT`: Host binding. Default: `127.0.0.1`.
- `--port INTEGER`: Port number. Default: `8080`.
- `--topic TEXT`: Initial Kafka topic to bridge.
- `--demo`: Run in demo mode with synthetic data generation.

---

### `eventlens demo`

Streams simulated events directly to stdout to demonstrate filtering and formatting capabilities without a live Kafka broker.

```bash
eventlens demo [OPTIONS]
```

**Options:**
- `--rate FLOAT`: Event production rate in messages per second. Default: 2.0.
- `--count INTEGER`: Total number of events to produce. Default: 20.
- `--filter TEXT`: JMESPath filter expression to test.
