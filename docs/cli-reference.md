# CLI Reference

The `eventlens` command line tool provides subcommands for stream tailing, consumer lag inspection, dead letter queue triage, terminal UI, and web dashboard hosting.

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
