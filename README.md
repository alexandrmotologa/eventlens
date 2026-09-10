# EventLens

EventLens is a terminal and web debugger for Apache Kafka event streams and dead letter queues. It provides real-time stream tailing, JMESPath filtering, schema decoding for JSON, Protobuf, and Confluent Avro, consumer group lag tracking, time-travel traffic recording, distributed event tracing, structural payload diffing, and an interactive DLQ Studio for batch redriving failed messages.

## Key Capabilities

- Live stream tailing with syntax highlighting and formatted record previews.
- JMESPath and regular expression filtering across payload properties, headers, and keys.
- Dynamic schema decoders for JSON, Google Protocol Buffers (Proto3), and Confluent Avro wire formats.
- Confluent Schema Registry client with in-memory and disk caching.
- Partition lag tracking and consumer group watermark diagnostics.
- Dead Letter Queue studio with automated failure categorization (syntax errors, validation rejections, downstream outages, idempotency conflicts) and in-place redrive dispatch.
- Batch DLQ redrive with dry-run verification and category filtering.
- Time-travel debugger: record Kafka streams into portable `.lens` session files and replay them at variable speeds.
- Distributed event tracing across multiple topics by correlation ID or entity key with latency delta calculations.
- Structural payload diff tool for comparing messages or files side-by-side with colored terminal and web tables.
- Message producer CLI for injecting test payloads and simulated traffic directly into Kafka.
- Interactive terminal UI built with Textual, featuring collapsible payload trees and keyboard shortcuts.
- Local web dashboard powered by FastAPI and WebSockets, including audio alerts, trace graph visualization, diff tool, and multi-format export (JSON, NDJSON, CSV).
- Offline simulation mode (`eventlens demo`) to test every feature without a live Kafka broker.

## Installation

Install via pip or uv:

```bash
pip install eventlens
# or with uv
uv tool install eventlens
```

To run from source:

```bash
git clone https://github.com/alexandrmotologa/eventlens.git
cd eventlens
uv sync --all-extras --dev
```

## Quick Start

### 1. Tail an active Kafka topic

Stream messages in real time:

```bash
eventlens tail order.events --broker localhost:9092
```

Tail from the beginning of the topic:

```bash
eventlens tail order.events --broker localhost:9092 --from-beginning
```

Filter payloads using JMESPath or regular expressions:

```bash
eventlens tail order.events --filter "total_amount > `100`"
eventlens tail order.events --regex "FAIL|ERROR"
```

### 2. Decode Protobuf and Avro payloads

Pass a `.proto` file to decode binary Proto3 payloads dynamically:

```bash
eventlens tail order.events --proto contracts/order_events.proto --message-type OrderEventEnvelopeProto
```

For Confluent Schema Registry integration:

```bash
eventlens tail order.events --schema-registry http://localhost:8081
```

### 3. Trace events across topics

Track an order or correlation ID across multiple topics to inspect routing and hop latencies:

```bash
eventlens trace ord_9011 --broker localhost:9092
# Or try the built-in demo trace:
eventlens trace ord_9011 --demo
```

### 4. Compare message payloads (Diff Tool)

Compare two JSON files, literal JSON strings, or message payloads side-by-side:

```bash
eventlens diff '{"status": "PENDING", "amount": 100}' '{"status": "COMPLETED", "amount": 100}'
# Demo comparison:
eventlens diff dummy dummy --demo
```

### 5. Record and replay traffic (Time-Travel)

Record 1,000 live events to a `.lens` file:

```bash
eventlens record order.events -o recording.lens --max 1000
```

Replay recorded traffic into a staging or shadow topic at 2x speed:

```bash
eventlens replay recording.lens --target shadow.order.events --speed 2.0
```

### 6. Batch redrive DLQ records

Triage poisoned messages, filter by category (e.g. Outage), and preview the redrive using dry-run mode:

```bash
# Dry-run preview
eventlens dlq redrive-batch order.events.dlq --category Outage --dry-run

# Execute live batch redrive
eventlens dlq redrive-batch order.events.dlq --category Outage --target order.events --limit 100
```

### 7. Publish test events

Inject test payloads into a topic:

```bash
eventlens produce order.events --key ord_123 --json '{"order_id": "ord_123", "amount": 49.99}'
```

### 8. Interactive Terminal UI (TUI)

Launch the full-screen terminal interface:

```bash
eventlens tui --topic order.events
# Offline demo:
eventlens tui --demo
```

Keyboard shortcuts in TUI:
- `Space`: Pause or resume stream ingestion.
- `/`: Focus search filter bar.
- `Tab`: Switch between views (Stream, DLQ Studio, Lag Monitor).
- `j` / `k`: Navigate record list.
- `c`: Copy selected payload JSON to clipboard.
- `Ctrl+R`: Open redrive confirmation modal on DLQ view.
- `q`: Quit application.

### 9. Local Web Dashboard

Launch the web studio on port 8080:

```bash
eventlens web --demo --port 8080
```

Open `http://localhost:8080` in your browser to access:
- Live streaming table with detail inspector and search.
- DLQ Studio with diagnostic root causes, stack trace details, and JSON patch redrive.
- Partition lag gauges and consumer group health indicators.
- Distributed Event Trace explorer.
- Visual payload diff comparison.
- Test message publishing modal.
- Audio and visual alerts for incoming poison pills.
- Data export in JSON, NDJSON, and CSV formats.

## Documentation

- [Architecture Overview](docs/architecture.md)
- [CLI Reference](docs/cli-reference.md)
- [Schema Decoders](docs/decoders.md)
- [Dead Letter Queue Studio](docs/dlq-studio.md)
- [Web Studio Guide](docs/web-studio.md)

## Development and Testing

Run test suite:

```bash
uv run pytest tests/unit -v
```

Run linter and formatter:

```bash
uv run ruff check .
uv run ruff format .
```

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
