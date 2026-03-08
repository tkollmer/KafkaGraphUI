"""Tests for API routes with mocked Kafka admin."""

import pytest
from unittest.mock import MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient
from api_routes import router


def create_app(admin=None, sampler=None):
    app = FastAPI()
    app.include_router(router)
    app.state.kafka_admin = admin
    app.state.message_sampler = sampler
    return app


class TestTopicRoutes:
    def test_list_topics(self):
        admin = MagicMock()
        admin.list_topics.return_value = [
            {"name": "orders", "partitions": 3, "messages": 1000}
        ]
        client = TestClient(create_app(admin=admin))
        resp = client.get("/api/topics")
        assert resp.status_code == 200
        assert len(resp.json()) == 1
        assert resp.json()[0]["name"] == "orders"

    def test_get_topic_detail(self):
        admin = MagicMock()
        admin.get_topic_detail.return_value = {
            "name": "orders", "partitions": [], "config": {}
        }
        client = TestClient(create_app(admin=admin))
        resp = client.get("/api/topics/orders")
        assert resp.status_code == 200
        assert resp.json()["name"] == "orders"

    def test_create_topic(self):
        admin = MagicMock()
        admin.create_topic.return_value = {"success": True}
        client = TestClient(create_app(admin=admin))
        resp = client.post("/api/topics", json={"name": "new-topic", "partitions": 3})
        assert resp.status_code == 200
        admin.create_topic.assert_called_once()

    def test_create_topic_missing_name(self):
        admin = MagicMock()
        client = TestClient(create_app(admin=admin))
        resp = client.post("/api/topics", json={})
        assert resp.status_code == 400

    def test_create_topic_failure(self):
        admin = MagicMock()
        admin.create_topic.return_value = {"success": False, "error": "Already exists"}
        client = TestClient(create_app(admin=admin))
        resp = client.post("/api/topics", json={"name": "orders"})
        assert resp.status_code == 400

    def test_delete_topic(self):
        admin = MagicMock()
        admin.delete_topic.return_value = {"success": True}
        client = TestClient(create_app(admin=admin))
        resp = client.delete("/api/topics/orders")
        assert resp.status_code == 200

    def test_delete_topic_failure(self):
        admin = MagicMock()
        admin.delete_topic.return_value = {"success": False, "error": "Not found"}
        client = TestClient(create_app(admin=admin))
        resp = client.delete("/api/topics/orders")
        assert resp.status_code == 400

    def test_produce_message(self):
        admin = MagicMock()
        admin.produce_message.return_value = {"success": True, "partition": 0, "offset": 42}
        client = TestClient(create_app(admin=admin))
        resp = client.post("/api/topics/orders/produce", json={"value": "test msg", "key": "k1"})
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_produce_message_failure(self):
        admin = MagicMock()
        admin.produce_message.return_value = {"success": False, "error": "Timeout"}
        client = TestClient(create_app(admin=admin))
        resp = client.post("/api/topics/orders/produce", json={"value": "test"})
        assert resp.status_code == 500


class TestConsumerGroupRoutes:
    def test_list_consumer_groups(self):
        admin = MagicMock()
        admin.list_consumer_groups.return_value = [
            {"groupId": "my-group", "state": "Stable", "members": 2}
        ]
        client = TestClient(create_app(admin=admin))
        resp = client.get("/api/consumer-groups")
        assert resp.status_code == 200
        assert resp.json()[0]["groupId"] == "my-group"

    def test_get_consumer_group_detail(self):
        admin = MagicMock()
        admin.get_consumer_group_detail.return_value = {
            "groupId": "my-group", "state": "Stable", "members": [], "offsets": []
        }
        client = TestClient(create_app(admin=admin))
        resp = client.get("/api/consumer-groups/my-group")
        assert resp.status_code == 200

    def test_reset_offsets(self):
        admin = MagicMock()
        admin.reset_offsets.return_value = {"success": True}
        client = TestClient(create_app(admin=admin))
        resp = client.post("/api/consumer-groups/my-group/reset-offsets", json={"strategy": "earliest"})
        assert resp.status_code == 200

    def test_reset_offsets_failure(self):
        admin = MagicMock()
        admin.reset_offsets.return_value = {"success": False, "error": "Group is active"}
        client = TestClient(create_app(admin=admin))
        resp = client.post("/api/consumer-groups/my-group/reset-offsets", json={"strategy": "latest"})
        assert resp.status_code == 400


class TestBrokerRoutes:
    def test_list_brokers(self):
        admin = MagicMock()
        admin.list_brokers.return_value = [
            {"id": 1, "host": "broker-1", "port": 9092}
        ]
        client = TestClient(create_app(admin=admin))
        resp = client.get("/api/brokers")
        assert resp.status_code == 200

    def test_get_cluster_info(self):
        admin = MagicMock()
        admin.get_cluster_info.return_value = {
            "clusterId": "abc123", "controller": 1, "brokerCount": 3
        }
        client = TestClient(create_app(admin=admin))
        resp = client.get("/api/cluster")
        assert resp.status_code == 200
        assert resp.json()["brokerCount"] == 3


class TestNoAdmin:
    def test_returns_503_without_admin(self):
        client = TestClient(create_app(admin=None))
        resp = client.get("/api/topics")
        assert resp.status_code == 503
