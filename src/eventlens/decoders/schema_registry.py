"""Confluent Schema Registry client with caching for dynamic Avro and Protobuf schemas."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class SchemaRegistryClient:
    """Client for querying schemas from Confluent Schema Registry or Karapace."""

    def __init__(
        self,
        base_url: str = "http://localhost:8081",
        username: str | None = None,
        password: str | None = None,
        cache_dir: str | Path | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth = (username, password) if username and password else None
        self._memory_cache: dict[int, dict[str, Any]] = {}
        self.cache_dir = Path(cache_dir) if cache_dir else None
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    async def get_schema_by_id(self, schema_id: int) -> dict[str, Any] | None:
        """Fetch schema definition by ID with in-memory and disk caching."""
        # 1. Memory cache
        if schema_id in self._memory_cache:
            return self._memory_cache[schema_id]

        # 2. Disk cache
        if self.cache_dir:
            disk_file = self.cache_dir / f"schema_{schema_id}.json"
            if disk_file.exists():
                try:
                    data = json.loads(disk_file.read_text(encoding="utf-8"))
                    self._memory_cache[schema_id] = data
                    return data
                except Exception:
                    pass

        # 3. HTTP Request
        url = f"{self.base_url}/schemas/ids/{schema_id}"
        try:
            async with httpx.AsyncClient(auth=self.auth, timeout=5.0) as client:
                res = await client.get(url)
                if res.status_code == 200:
                    schema_data = res.json()
                    self._memory_cache[schema_id] = schema_data

                    if self.cache_dir:
                        disk_file = self.cache_dir / f"schema_{schema_id}.json"
                        disk_file.write_text(json.dumps(schema_data), encoding="utf-8")

                    return schema_data
                else:
                    logger.warning(f"Schema Registry returned {res.status_code} for schema {schema_id}")
                    return None
        except Exception as err:
            logger.error(f"Error contacting Schema Registry at {url}: {err}")
            return None

    def get_cached_schema(self, schema_id: int) -> dict[str, Any] | None:
        """Synchronously get cached schema without making network calls."""
        if schema_id in self._memory_cache:
            return self._memory_cache[schema_id]
        if self.cache_dir:
            disk_file = self.cache_dir / f"schema_{schema_id}.json"
            if disk_file.exists():
                try:
                    data = json.loads(disk_file.read_text(encoding="utf-8"))
                    self._memory_cache[schema_id] = data
                    return data
                except Exception:
                    pass
        return None

    def register_mock_schema(self, schema_id: int, schema_text: str, schema_type: str = "AVRO") -> None:
        """Register an in-memory schema for testing or offline environments."""
        self._memory_cache[schema_id] = {
            "schema": schema_text,
            "schemaType": schema_type,
            "id": schema_id,
        }
