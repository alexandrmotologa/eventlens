# EventLens

EventLens is a terminal and web debugger for Apache Kafka event streams and dead letter queues. It provides live stream tailing, JMESPath filtering, schema decoding for JSON and Protobuf, consumer group lag inspection, and an interactive DLQ editor for patching and redriving failed events.

## Features

- Live tailing with syntax highlighting and color formatted records.
- JSONPath and JMESPath expression filtering on incoming payloads.
- Dynamic schema decoding for JSON, Google Protocol Buffers (Proto3), and Confluent Avro wire frames without precompiling stubs.
- Partition lag tracking and consumer group offset monitoring.
- Interactive terminal UI built with Textual, featuring collapsible payload trees and keyboard navigation.
- Built-in Dead Letter Queue studio with failure heuristic diagnostics and in-place redrive dispatch.
- Zero-dependency local web dashboard powered by FastAPI and WebSockets.
- Offline simulation mode (`eventlens demo`) to explore and test features without an external Kafka cluster.

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

Stream messages in real time with formatted output:

```bash
eventlens tail order.events --broker localhost:9092
```

Tail from the beginning of the topic:

```bash
eventlens tail order.events --broker localhost:9092 --from-beginning
```

### 2. Filter incoming messages

Filter payloads using JMESPath expressions:

```bash
eventlens tail order.events --filter "currency == 'USD' && total_amount > '100'"
```

Filter with regular expressions:

```bash
eventlens tail order.events --regex "FAIL|ERROR"
```

### 3. Decode Protobuf payloads dynamically

Pass a `.proto` contract file to decode binary Proto3 payloads on the fly:

```bash
eventlens tail order.events --proto contracts/order_events.proto --message-type OrderEventEnvelopeProto
```

### 4. Interactive Terminal UI

Launch the full-screen terminal interface:

```bash
eventlens tui --broker localhost:9092 --topic order.events
```

Keyboard shortcuts in TUI:
- `Space`: Pause or resume stream ingestion.
- `/`: Open search filter bar.
- `Tab`: Switch between panels (Streams, DLQ Studio, Lag Monitor).
- `j` / `k`: Scroll up and down.
- `c`: Copy selected event JSON to system clipboard.
- `Ctrl+R`: Open redrive confirmation modal in DLQ screen.
- `q`: Quit application.

### 5. Inspect consumer group lag

Check partition offsets, log-end watermarks, and committed lag:

```bash
eventlens lag order.events --group order-processing-group --broker localhost:9092
```

### 6. Dead Letter Queue triage and redrive

Inspect poisoned messages and headers on a DLQ topic:

```bash
eventlens dlq inspect order.events.dlq --broker localhost:9092
```

Redrive an edited payload back to its original topic:

```bash
eventlens dlq redrive order.events.dlq --offset 142 --target order.events --broker localhost:9092
```

### 7. Run without Kafka (Demo Mode)

If you do not have a Kafka cluster running locally, use demo mode to generate simulated order events and DLQ poison pills:

```bash
eventlens demo
```

To run the interactive TUI in demo mode:

```bash
eventlens tui --demo
```

To launch the web dashboard in demo mode:

```bash
eventlens web --demo --port 8080
```

## Documentation

- [Architecture Overview](docs/architecture.md)
- [CLI Reference](docs/cli-reference.md)
- [Schema Decoders](docs/decoders.md)
- [Dead Letter Queue Studio](docs/dlq-studio.md)

## Development and Testing

Run test suite:

```bash
uv run pytest tests/unit -v
```

Run linter:

```bash
uv run ruff check .
```

Run formatted check:

```bash
uv run ruff format --check .
```

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
