# Dead Letter Queue Studio

Dead letter queues capture messages that fail processing in downstream consumers. Without specialized tools, debugging these poison pills requires writing throwaway scripts to fetch records, parse stack traces from custom headers, edit payloads, and publish them back to active topics.

EventLens provides a dedicated workflow for inspecting, analyzing, and redriving failed events.

## Error Header Extraction

When event-driven frameworks such as Spring Kafka or custom consumers reject a record, they write failure metadata into Kafka record headers. EventLens parses the following common header keys:

- `x-exception-message`: Brief explanation of the error.
- `x-exception-stacktrace`: Full trace of the unhandled exception.
- `x-original-topic`: Topic where the message was initially published.
- `x-original-partition`: Partition ID where the failure occurred.
- `x-original-offset`: Original message offset.
- `deadletter.reason`: Business rule or validation rejection reason.

## Heuristic Diagnostics

The diagnostic engine checks error signatures against known failure classes:

| Error Signature | Detected Category | Suggested Fix |
|---|---|---|
| `JsonParseException`, `Unexpected character` | Malformed JSON | Correct corrupted syntax in payload |
| `ValidationException`, `ConstraintViolation` | Business Schema Rejection | Update rejected property values |
| `UnknownFieldException`, `UnrecognizedProperty` | Contract Incompatibility | Synchronize schema version with consumer |
| `TimeoutException`, `ConnectException` | Upstream Infrastructure Outage | Redrive without altering payload |

## In-Terminal Patching and Redrive

In the Textual TUI (`eventlens tui`), switch to the DLQ tab:

1. Select a poisoned message from the table to view its parsed headers and payload.
2. Review the error diagnostics and stack trace in the inspector panel.
3. Open the payload editor, make corrections, and press `Ctrl+R`.
4. EventLens republishes the corrected event to the destination topic, preserving the original key and stripping out dead letter error headers.
