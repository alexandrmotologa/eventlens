"""Unit tests for FastAPI Web Studio endpoints."""

from __future__ import annotations

from starlette.testclient import TestClient

from eventlens.web.server import create_app


def test_web_api_status() -> None:
    app = create_app(topic="order.events", demo=True)
    with TestClient(app) as client:
        res = client.get("/api/status")
        assert res.status_code == 200
        data = res.json()
        assert data["topic"] == "order.events"
        assert data["demo"] is True


def test_web_api_lag() -> None:
    app = create_app(topic="order.events", demo=True)
    with TestClient(app) as client:
        res = client.get("/api/lag")
        assert res.status_code == 200
        data = res.json()
        assert "group_id" in data
        assert "total_lag" in data
        assert len(data["partitions"]) > 0


def test_web_api_produce() -> None:
    app = create_app(topic="order.events", demo=True)
    with TestClient(app) as client:
        res = client.post(
            "/api/produce",
            json={
                "topic": "order.events",
                "key": "ord_test_99",
                "payload": {"order_id": "ord_test_99", "amount": 100.0},
            },
        )
        assert res.status_code == 200
        data = res.json()
        assert data["success"] is True
        assert data["demo"] is True


def test_web_api_trace() -> None:
    app = create_app(topic="order.events", demo=True)
    with TestClient(app) as client:
        res = client.get("/api/trace?correlation_id=ord_demo_100")
        assert res.status_code == 200
        data = res.json()
        assert data["correlation_id"] == "ord_demo_100"
        assert len(data["hops"]) > 0


def test_web_api_diff() -> None:
    app = create_app(topic="order.events", demo=True)
    with TestClient(app) as client:
        res = client.post(
            "/api/diff",
            json={
                "payload_a": {"status": "PENDING", "amount": 50},
                "payload_b": {"status": "SETTLED", "amount": 50},
            },
        )
        assert res.status_code == 200
        data = res.json()
        assert data["identical"] is False
        assert data["modified_count"] == 1
        assert len(data["items"]) == 1


def test_web_api_export_formats() -> None:
    app = create_app(topic="order.events", demo=True)
    with TestClient(app) as client:
        # JSON
        res_json = client.get("/api/export?format=json")
        assert res_json.status_code == 200
        assert "application/json" in res_json.headers["content-type"]

        # CSV
        res_csv = client.get("/api/export?format=csv")
        assert res_csv.status_code == 200
        assert "text/csv" in res_csv.headers["content-type"]
        assert "Topic,Partition,Offset" in res_csv.text

        # NDJSON
        res_nd = client.get("/api/export?format=ndjson")
        assert res_nd.status_code == 200
        assert "application/x-ndjson" in res_nd.headers["content-type"]


def test_web_api_serves_html() -> None:
    app = create_app(topic="order.events", demo=True)
    with TestClient(app) as client:
        res = client.get("/")
        assert res.status_code == 200
        assert "EventLens" in res.text
