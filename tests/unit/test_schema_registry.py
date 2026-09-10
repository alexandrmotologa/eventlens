"""Unit tests for Schema Registry client and caching mechanisms."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from eventlens.decoders.schema_registry import SchemaRegistryClient

SAMPLE_AVRO_SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "UserOrder",
        "namespace": "com.eventlens",
        "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "amount", "type": "double"},
        ],
    }
)


def test_schema_registry_mock_registration() -> None:
    client = SchemaRegistryClient(base_url="http://mock-registry:8081")
    client.register_mock_schema(schema_id=42, schema_text=SAMPLE_AVRO_SCHEMA)

    cached = client.get_cached_schema(schema_id=42)
    assert cached is not None
    assert cached["id"] == 42
    assert cached["schemaType"] == "AVRO"
    assert "UserOrder" in cached["schema"]


def test_schema_registry_disk_caching(tmp_path: Path) -> None:
    cache_dir = tmp_path / "schema_cache"
    client1 = SchemaRegistryClient(base_url="http://mock-registry:8081", cache_dir=cache_dir)
    client1.register_mock_schema(schema_id=101, schema_text=SAMPLE_AVRO_SCHEMA)

    # Save to disk
    schema_data = client1.get_cached_schema(101)
    disk_file = cache_dir / "schema_101.json"
    disk_file.write_text(json.dumps(schema_data), encoding="utf-8")

    # Second client without memory cache should read from disk
    client2 = SchemaRegistryClient(base_url="http://mock-registry:8081", cache_dir=cache_dir)
    assert 101 not in client2._memory_cache

    cached_disk = client2.get_cached_schema(101)
    assert cached_disk is not None
    assert cached_disk["id"] == 101
    assert 101 in client2._memory_cache


@pytest.mark.asyncio
async def test_schema_registry_http_fetch_mock(monkeypatch: pytest.MonkeyPatch) -> None:
    client = SchemaRegistryClient(base_url="http://test-registry:8081")

    async def mock_get(self, url: str, *args, **kwargs):
        req = httpx.Request("GET", url)
        if "123" in url:
            content = json.dumps({"id": 123, "schema": SAMPLE_AVRO_SCHEMA, "schemaType": "AVRO"}).encode()
            return httpx.Response(status_code=200, content=content, request=req)
        return httpx.Response(status_code=404, content=b"Not found", request=req)

    monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)

    res = await client.get_schema_by_id(123)
    assert res is not None
    assert res["id"] == 123
    assert res["schemaType"] == "AVRO"

    # Subsequent fetch should be answered from memory cache
    cached_sync = client.get_cached_schema(123)
    assert cached_sync is not None
