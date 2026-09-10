"""Unit tests for JMESPath and Regex event filtering."""

from __future__ import annotations

from eventlens.engine.filter import EventFilter
from eventlens.models import KafkaRecord


def create_record(payload_dict: dict, key: str = "ord_1", headers: dict | None = None) -> KafkaRecord:
    import json

    val_str = json.dumps(payload_dict)
    return KafkaRecord(
        topic="order.events",
        partition=0,
        offset=100,
        key=key,
        value=val_str,
        decoded_payload=payload_dict,
        payload_format="json",
        headers=headers or {},
    )


def test_filter_no_rules_matches_everything() -> None:
    f = EventFilter()
    assert f.is_active() is False
    rec = create_record({"foo": "bar"})
    assert f.matches(rec) is True


def test_jmespath_filter_matching() -> None:
    f = EventFilter(jmespath_query="currency == 'USD' && total_amount > `100`")
    assert f.is_active() is True

    # Matching record
    rec1 = create_record({"currency": "USD", "total_amount": 150})
    assert f.matches(rec1) is True

    # Non-matching currency
    rec2 = create_record({"currency": "EUR", "total_amount": 150})
    assert f.matches(rec2) is False

    # Non-matching amount
    rec3 = create_record({"currency": "USD", "total_amount": 50})
    assert f.matches(rec3) is False


def test_regex_filter_matching() -> None:
    f = EventFilter(regex_pattern="FAIL|CANCELLED")

    rec1 = create_record({"order_id": "ord_1", "status": "CANCELLED"})
    assert f.matches(rec1) is True

    rec2 = create_record({"order_id": "ord_2", "status": "COMPLETED"})
    assert f.matches(rec2) is False

    # Matches in key
    rec3 = create_record({"status": "OK"}, key="ord_FAIL_99")
    assert f.matches(rec3) is True

    # Matches in headers
    rec4 = create_record({"status": "OK"}, headers={"x-error": "FAILED_VALIDATION"})
    assert f.matches(rec4) is True


def test_combined_filter() -> None:
    f = EventFilter(jmespath_query="currency == 'EUR'", regex_pattern="ord_special")

    # Meets both
    rec1 = create_record({"currency": "EUR"}, key="ord_special_1")
    assert f.matches(rec1) is True

    # Meets JMESPath but not regex
    rec2 = create_record({"currency": "EUR"}, key="ord_regular_2")
    assert f.matches(rec2) is False

    # Meets regex but not JMESPath
    rec3 = create_record({"currency": "USD"}, key="ord_special_3")
    assert f.matches(rec3) is False
