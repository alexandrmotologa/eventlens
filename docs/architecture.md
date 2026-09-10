# Architecture Overview

EventLens connects to Apache Kafka clusters, consumes event streams asynchronously, decodes binary payloads according to registered or inferred schemas, and exposes interactive debugging interfaces in both terminal and web browser environments.

## Component Layout

```
                  ┌─────────────────────────────────────┐
                  │            Apache Kafka             │
                  │   Topics / Partitions / DLQ / Lag   │
                  └──────────────────┬──────────────────┘
                                     │
                                     ▼
                  ┌─────────────────────────────────────┐
                  │      eventlens.engine.consumer      │
                  │   AsyncKafkaConsumer / Batch Queue  │
                  └──────────────────┬──────────────────┘
                                     │
                                     ▼
                  ┌─────────────────────────────────────┐
                  │     eventlens.decoders.registry     │
                  │  Auto-Detect / Proto / Avro / JSON  │
                  └──────────────────┬──────────────────┘
                                     │
                                     ▼
                  ┌─────────────────────────────────────┐
                  │       eventlens.engine.filter       │
                  │       JMESPath / Regex Engine       │
                  └──────────┬────────────────┬─────────┘
                             │                │
             ┌───────────────┴────┐     ┌─────┴──────────────┐
             │                    │     │                    │
             ▼                    ▼     ▼                    ▼
     ┌──────────────┐      ┌──────────────┐           ┌──────────────┐
     │  CLI output  │      │ Textual TUI  │           │ Web UI (WS)  │
     │ Rich tables  │      │ Screens/Tree │           │ FastAPI + JS │
     └──────────────┘      └──────────────┘           └──────────────┘
```

## Core Modules

### 1. Consumer Engine (`eventlens.engine.consumer`)
Wraps `aiokafka.AIOKafkaConsumer` to handle connection lifecycles, partition assignment, manual offset management, and graceful shutdown. It supports tailing specific partition offsets or subscribing to full topics with configurable batch size and polling intervals.

### 2. Decoder Registry (`eventlens.decoders.registry`)
Inspects raw payload bytes before forwarding records to the presentation layer:
- Confluent Avro wire frames: Checks for magic byte 0x00 and extracts the 4-byte schema ID.
- Protocol Buffers: Uses the dynamic descriptor pool to parse proto bytes from provided `.proto` files without requiring ahead-of-time code generation.
- JSON: Validates UTF-8 encoding and parses structured dictionary objects.
- Binary fallback: Formats unstructured binary data into hex dumps or ASCII strings.

### 3. Filter Engine (`eventlens.engine.filter`)
Applies client-side predicates to records before rendering:
- JMESPath queries: Evaluates expressions against decoded JSON or dictionary objects.
- Regular expressions: Matches patterns across payload strings and header values.

### 4. DLQ Studio (`eventlens.engine.dlq_engine`)
Inspects records in dead letter queues, extracts error metadata from headers such as `x-exception-message`, `x-original-topic`, and `x-death`, analyzes root causes using heuristic pattern matching, and republishes corrected payloads to destination topics.

### 5. Lag Tracker (`eventlens.engine.lag_tracker`)
Uses Kafka admin APIs to query high watermarks across all topic partitions, matches them with committed consumer group offsets, and calculates per-partition lag numbers and distribution percentages.

### 6. Presentation Layers
- **CLI (`eventlens.cli`)**: Fast single-purpose commands for tailing, lag queries, and batch inspection.
- **TUI (`eventlens.tui`)**: Interactive terminal interface built with Textual, featuring reactive screens, keyboard navigation, collapsible JSON tree views, and an in-terminal editor.
- **Web UI (`eventlens.web`)**: Lightweight FastAPI server that streams incoming messages to browser clients over WebSockets.
