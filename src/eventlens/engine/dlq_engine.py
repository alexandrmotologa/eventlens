"""Dead Letter Queue (DLQ) diagnostic and redrive engine."""

from __future__ import annotations

import re
from typing import Any

from eventlens.config import EventLensConfig
from eventlens.engine.producer import AsyncProducer
from eventlens.models import DLQDiagnostic, KafkaRecord, RedriveResult

FAILURE_RULES: list[tuple[str, str, str, str]] = [
    (
        r"(JsonParse|Unexpected character|Expecting value|JSONDecodeError|invalid json)",
        "Deserialization Syntax Error",
        "Corrupted or incomplete JSON syntax in message payload.",
        "Fix malformed JSON brackets, quotes, or characters in the payload.",
    ),
    (
        r"(ValidationException|ConstraintViolation|missing required|pattern mismatch|invalid field)",
        "Schema Validation Failure",
        "Payload fails structural or constraint validation in consumer.",
        "Update rejected properties to satisfy contract constraints.",
    ),
    (
        r"(DecodeError|Truncated message|wire type|invalid tag|protobuf)",
        "Protobuf Deserialization Failure",
        "Binary payload does not match expected Protobuf schema definition.",
        "Verify schema contract version or synchronize message definition.",
    ),
    (
        r"(TimeoutException|ConnectException|SocketTimeout|503 Service Unavailable|504 Gateway)",
        "Downstream Service Outage",
        "Processing failed due to temporary network or dependency unavailability.",
        "Safe to redrive directly without altering payload once service recovers.",
    ),
    (
        r"(Duplicate|AlreadyExists|ConflictException|OptimisticLock)",
        "Idempotency or Conflict Rejection",
        "Message with this identifier was already processed or caused state conflict.",
        "Inspect entity state before redriving to avoid unintended duplicates.",
    ),
    (
        r"(NullPointerException|AttributeError|KeyError|IndexError)",
        "Uncaught Null / Key Exception",
        "Consumer crashed expecting a field that was not present in the payload.",
        "Supply the missing property in the payload editor.",
    ),
]


class DLQEngine:
    """Provides failure diagnostics, header extraction, and redrive execution for DLQ records."""

    def __init__(self, config: EventLensConfig | None = None) -> None:
        self.config = config or EventLensConfig()
        self.producer = AsyncProducer(config=self.config)

    @classmethod
    def diagnose_record(cls, record: KafkaRecord) -> DLQDiagnostic:
        """Analyze headers and stack traces to categorize failure root cause."""
        headers = record.headers
        error_msg = headers.get("x-exception-message") or headers.get("deadletter.reason") or record.error_message or ""
        stack_trace = headers.get("x-exception-stacktrace") or ""
        original_topic = headers.get("x-original-topic")
        orig_part_str = headers.get("x-original-partition")
        orig_off_str = headers.get("x-original-offset")

        orig_partition = int(orig_part_str) if orig_part_str and orig_part_str.isdigit() else None
        orig_offset = int(orig_off_str) if orig_off_str and orig_off_str.isdigit() else None

        combined_text = f"{error_msg}\n{stack_trace}"

        matched_category = "Unclassified Error"
        matched_cause = error_msg or "Unknown error occurred during processing."
        matched_action = "Review stack trace and inspect payload fields."

        for pattern, cat, cause, action in FAILURE_RULES:
            if re.search(pattern, combined_text, re.IGNORECASE):
                matched_category = cat
                matched_cause = cause
                matched_action = action
                break

        # Extract exception class name if possible
        exception_type: str | None = None
        ex_match = re.search(r"([\w\.]+(?:Exception|Error))", combined_text)
        if ex_match:
            exception_type = ex_match.group(1).split(".")[-1]

        return DLQDiagnostic(
            failure_category=matched_category,
            root_cause=matched_cause,
            suggested_action=matched_action,
            original_topic=original_topic,
            original_partition=orig_partition,
            original_offset=orig_offset,
            exception_type=exception_type,
            exception_message=error_msg if error_msg else None,
            stack_trace=stack_trace if stack_trace else None,
        )

    async def redrive(
        self,
        record: KafkaRecord,
        target_topic: str | None = None,
        patched_payload: Any | None = None,
        strip_dlq_headers: bool = True,
    ) -> RedriveResult:
        """Redrive a DLQ message to its target topic."""
        dest = target_topic or record.headers.get("x-original-topic")
        if not dest:
            return RedriveResult(
                success=False,
                source_topic=record.topic,
                target_topic="",
                source_offset=record.offset,
                error="Target topic unspecified and 'x-original-topic' header missing.",
            )

        return await self.producer.redrive_record(
            record=record,
            target_topic=dest,
            patched_payload=patched_payload,
            strip_dlq_headers=strip_dlq_headers,
        )
