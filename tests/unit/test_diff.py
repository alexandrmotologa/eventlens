"""Unit tests for payload structural diff engine."""

from __future__ import annotations

import json

from eventlens.engine.diff import compare_payloads


def test_compare_identical_payloads() -> None:
    payload = {"order_id": "ord_100", "total": 45.5, "items": [1, 2, 3]}
    res = compare_payloads(payload, payload.copy())
    assert res.identical is True
    assert res.added_count == 0
    assert res.removed_count == 0
    assert res.modified_count == 0
    assert len(res.items) == 0


def test_compare_added_and_removed_fields() -> None:
    payload_a = {"order_id": "ord_100", "obsolete_flag": True}
    payload_b = {"order_id": "ord_100", "new_feature_flag": False}

    res = compare_payloads(payload_a, payload_b)
    assert res.identical is False
    assert res.added_count == 1
    assert "new_feature_flag" in res.added
    assert res.removed_count == 1
    assert "obsolete_flag" in res.removed
    assert res.modified_count == 0


def test_compare_modified_nested_fields() -> None:
    payload_a = {
        "order_id": "ord_100",
        "customer": {
            "name": "Alice",
            "tier": "STANDARD",
        },
    }
    payload_b = {
        "order_id": "ord_100",
        "customer": {
            "name": "Alice",
            "tier": "VIP",
        },
    }

    res = compare_payloads(payload_a, payload_b)
    assert res.identical is False
    assert res.modified_count == 1
    assert "customer.tier" in res.changed
    assert res.changed["customer.tier"] == ("STANDARD", "VIP")

    # Verify computed items list for web serialization
    assert len(res.items) == 1
    item = res.items[0]
    assert item["path"] == "customer.tier"
    assert item["diff_type"] == "modified"
    assert item["old_value"] == "STANDARD"
    assert item["new_value"] == "VIP"


def test_compare_json_strings() -> None:
    str_a = json.dumps({"currency": "USD", "amount": 100})
    str_b = json.dumps({"currency": "EUR", "amount": 100})

    res = compare_payloads(str_a, str_b)
    assert res.identical is False
    assert res.changed["currency"] == ("USD", "EUR")
