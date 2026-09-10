"""Event filtering using JMESPath expressions and regular expressions."""

from __future__ import annotations

import json
import re

import jmespath

from eventlens.models import KafkaRecord


class EventFilter:
    """Filters Kafka records based on JMESPath or regular expressions."""

    def __init__(
        self,
        jmespath_query: str | None = None,
        regex_pattern: str | None = None,
    ) -> None:
        self.jmespath_query = jmespath_query
        self.compiled_jmespath = jmespath.compile(jmespath_query) if jmespath_query else None
        self.regex_pattern = regex_pattern
        self.compiled_regex = re.compile(regex_pattern, re.IGNORECASE) if regex_pattern else None

    def is_active(self) -> bool:
        """Returns True if any filter rule is configured."""
        return bool(self.compiled_jmespath or self.compiled_regex)

    def matches(self, record: KafkaRecord) -> bool:
        """Check if a record matches all configured filter criteria."""
        if not self.is_active():
            return True

        payload = record.decoded_payload
        raw_text = record.value or ""

        # JMESPath Evaluation
        if self.compiled_jmespath:
            if not isinstance(payload, (dict, list)):
                # If decoded payload is not a dict/list, try parsing raw text
                if raw_text:
                    try:
                        payload = json.loads(raw_text)
                    except Exception:
                        return False
                else:
                    return False

            try:
                result = self.compiled_jmespath.search(payload)
                if not result and isinstance(payload, (dict, list)):
                    context = {"payload": payload, "headers": record.headers, "key": record.key}
                    result = self.compiled_jmespath.search(context)

                # Truthy evaluation in Python
                if not result:
                    return False
            except Exception:
                return False

        # Regex Evaluation
        if self.compiled_regex:
            matched = False
            # Check payload text
            target_str = json.dumps(payload, default=str) if isinstance(payload, (dict, list)) else str(raw_text or "")
            if self.compiled_regex.search(target_str):
                matched = True

            # Check key
            if not matched and record.key and self.compiled_regex.search(record.key):
                matched = True

            # Check headers
            if not matched:
                for k, v in record.headers.items():
                    if self.compiled_regex.search(f"{k}:{v}"):
                        matched = True
                        break

            if not matched:
                return False

        return True
