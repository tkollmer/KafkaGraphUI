"""Kafka Connect REST client — proxies to Kafka Connect cluster."""

import logging
import requests

logger = logging.getLogger(__name__)


class ConnectClient:
    """Lightweight proxy for the Kafka Connect REST API."""

    def __init__(self, url: str):
        self.url = url.rstrip("/")
        self.session = requests.Session()

    def _get(self, path: str) -> dict | list:
        resp = self.session.get(f"{self.url}{path}", timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, data: dict) -> dict:
        resp = self.session.post(f"{self.url}{path}", json=data, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _put(self, path: str, data: dict) -> dict:
        resp = self.session.put(f"{self.url}{path}", json=data, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _delete(self, path: str):
        resp = self.session.delete(f"{self.url}{path}", timeout=10)
        resp.raise_for_status()

    def list_connectors(self) -> list[str]:
        try:
            return self._get("/connectors")
        except Exception as e:
            logger.error(f"Failed to list connectors: {e}")
            return []

    def get_connector(self, name: str) -> dict:
        try:
            return self._get(f"/connectors/{name}")
        except Exception as e:
            logger.error(f"Failed to get connector {name}: {e}")
            return {}

    def get_connector_status(self, name: str) -> dict:
        try:
            return self._get(f"/connectors/{name}/status")
        except Exception as e:
            logger.error(f"Failed to get connector status {name}: {e}")
            return {}

    def create_connector(self, name: str, config: dict) -> dict:
        try:
            return self._post("/connectors", {"name": name, "config": config})
        except Exception as e:
            logger.error(f"Failed to create connector {name}: {e}")
            return {"error": str(e)}

    def update_connector(self, name: str, config: dict) -> dict:
        try:
            return self._put(f"/connectors/{name}/config", config)
        except Exception as e:
            logger.error(f"Failed to update connector {name}: {e}")
            return {"error": str(e)}

    def delete_connector(self, name: str) -> bool:
        try:
            self._delete(f"/connectors/{name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete connector {name}: {e}")
            return False

    def pause_connector(self, name: str) -> bool:
        try:
            self.session.put(f"{self.url}/connectors/{name}/pause", timeout=10)
            return True
        except Exception as e:
            logger.error(f"Failed to pause connector {name}: {e}")
            return False

    def resume_connector(self, name: str) -> bool:
        try:
            self.session.put(f"{self.url}/connectors/{name}/resume", timeout=10)
            return True
        except Exception as e:
            logger.error(f"Failed to resume connector {name}: {e}")
            return False

    def restart_connector(self, name: str) -> bool:
        try:
            self.session.post(f"{self.url}/connectors/{name}/restart", timeout=10)
            return True
        except Exception as e:
            logger.error(f"Failed to restart connector {name}: {e}")
            return False

    def restart_task(self, name: str, task_id: int) -> bool:
        try:
            self.session.post(f"{self.url}/connectors/{name}/tasks/{task_id}/restart", timeout=10)
            return True
        except Exception as e:
            logger.error(f"Failed to restart task {task_id} of connector {name}: {e}")
            return False

    def get_connector_plugins(self) -> list[dict]:
        try:
            return self._get("/connector-plugins")
        except Exception as e:
            logger.error(f"Failed to get connector plugins: {e}")
            return []
