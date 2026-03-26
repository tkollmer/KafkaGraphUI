"""Schema Registry client — proxies requests to a Confluent Schema Registry."""

import logging
import requests

logger = logging.getLogger(__name__)


class SchemaRegistryClient:
    """Lightweight proxy for the Confluent Schema Registry REST API."""

    def __init__(self, url: str, auth: tuple[str, str] | None = None):
        self.url = url.rstrip("/")
        self.auth = auth
        self.session = requests.Session()
        if auth:
            self.session.auth = auth
        self.session.headers["Accept"] = "application/vnd.schemaregistry.v1+json"

    def _get(self, path: str) -> dict | list:
        resp = self.session.get(f"{self.url}{path}", timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, data: dict) -> dict:
        resp = self.session.post(
            f"{self.url}{path}",
            json=data,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()

    def _delete(self, path: str) -> dict | list:
        resp = self.session.delete(f"{self.url}{path}", timeout=10)
        resp.raise_for_status()
        return resp.json()

    def list_subjects(self) -> list[str]:
        try:
            return self._get("/subjects")
        except Exception as e:
            logger.error(f"Failed to list subjects: {e}")
            return []

    def get_versions(self, subject: str) -> list[int]:
        try:
            return self._get(f"/subjects/{subject}/versions")
        except Exception as e:
            logger.error(f"Failed to get versions for {subject}: {e}")
            return []

    def get_schema(self, subject: str, version: int | str = "latest") -> dict:
        try:
            return self._get(f"/subjects/{subject}/versions/{version}")
        except Exception as e:
            logger.error(f"Failed to get schema {subject}@{version}: {e}")
            return {}

    def get_compatibility(self, subject: str | None = None) -> str:
        try:
            if subject:
                data = self._get(f"/config/{subject}")
            else:
                data = self._get("/config")
            return data.get("compatibilityLevel", "UNKNOWN")
        except Exception:
            return "UNKNOWN"

    def register_schema(self, subject: str, schema: str, schema_type: str = "AVRO") -> dict:
        try:
            body: dict = {"schema": schema}
            if schema_type != "AVRO":
                body["schemaType"] = schema_type
            return self._post(f"/subjects/{subject}/versions", body)
        except Exception as e:
            logger.error(f"Failed to register schema for {subject}: {e}")
            return {"error": str(e)}

    def delete_subject(self, subject: str) -> list[int]:
        try:
            return self._delete(f"/subjects/{subject}")
        except Exception as e:
            logger.error(f"Failed to delete subject {subject}: {e}")
            return []

    def get_schema_by_id(self, schema_id: int) -> dict:
        try:
            return self._get(f"/schemas/ids/{schema_id}")
        except Exception as e:
            logger.error(f"Failed to get schema by id {schema_id}: {e}")
            return {}
