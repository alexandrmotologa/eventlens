# Web Studio Guide

EventLens includes a local web studio that connects directly to Kafka topics or runs in synthetic demo mode. It serves as a visual companion to the CLI and TUI, offering real-time streaming, Dead Letter Queue diagnosis, lag telemetry, event tracing, payload diffing, and export functionality.

## Launching the Web Studio

Start the web dashboard on port 8080:

```bash
# Connect to a live Kafka cluster
eventlens web --broker localhost:9092 --topic order.events --port 8080

# Or launch in simulated offline demo mode
eventlens web --demo --port 8080
```

Once started, open `http://localhost:8080` in your web browser.

## Core Panels

### 1. Live Stream View

The primary panel streams events arriving from the broker over an active WebSocket bridge (`/ws/events`).
- **Live Stream Table**: Displays message status (OK or DLQ), partition, offset, key, format (JSON, Proto, Avro), a truncated payload preview, and local timestamp.
- **Search & Filter**: Type in any substring or field name to filter rows in real time without dropping buffered events.
- **Detail Drawer**: Click on any record row to inspect headers and pretty-printed JSON payloads. Use the copy button to transfer formatted JSON directly to your system clipboard.
- **Feed Pause**: Toggle feed consumption to inspect fast-moving topics without rows jumping.

### 2. DLQ Studio

Dedicated triage environment for dead-letter topics.
- **Poison Pill Sidebar**: Lists failed messages with offset, partition, and preview error indicators.
- **Diagnostic Panel**: Automatically categorizes the root cause using failure heuristics:
  - Deserialization Syntax Error (malformed JSON)
  - Schema Validation Failure (constraint violations)
  - Protobuf Deserialization Failure (wire type or tag mismatches)
  - Downstream Service Outage (timeouts, 503s)
  - Idempotency / Conflict Rejection (duplicates)
- **JSON Patch Editor**: Modify the failed payload directly in the web editor to fix invalid values or missing fields.
- **Redrive Dispatcher**: Specify the target topic and click **Redrive Message**. The backend republishes the message with provenance tracking headers (`x-redriven-by: eventlens-web`) and updates the status bar with partition and offset confirmation.

### 3. Lag Monitor

Visualizes consumer group health and partition watermarks.
- **Summary Cards**: Displays active consumer group ID, total committed lag across all partitions, and total partition count.
- **Partition Table**: Lists each partition with its log-end offset (high watermark), current committed offset, absolute lag, and dynamic color-coded progress bars (green for healthy, yellow for moderate, red for high lag).

### 4. Distributed Event Trace

Track a multi-topic lifecycle using correlation IDs.
- Enter an order ID, trace ID, or transaction identifier (e.g. `ord_9011`).
- The engine searches across cluster topics to reconstruct the execution hops in chronological sequence.
- Shows hop order, topic, partition/offset, action, hop latency delta (in milliseconds), and execution status (SUCCESS or POISON DLQ).

### 5. Payload Diff Tool

Side-by-side structural comparison of two JSON payloads.
- Paste baseline payload into Payload A and current or rejected payload into Payload B.
- Click **Compare Payloads** to execute structural comparison.
- Generates a summary of added, removed, and modified properties, followed by a difference table showing exact property paths, change types, and values.

## Additional Features

- **Publish Event Modal**: Click `+ Produce Event` in the header to publish custom test events directly into any topic.
- **Audio Alerts**: Click the bell icon (`🔔`) in the header to enable Web Audio alerts when poison pills are detected on the stream.
- **Global Alert Banner**: Visual notification banner alerting operators to newly ingested DLQ records.
- **Multi-Format Export**: Download currently captured stream or DLQ events in JSON, NDJSON, or CSV formats via the `Export ▾` dropdown menu.
