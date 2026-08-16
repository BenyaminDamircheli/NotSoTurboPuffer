"""Minimal HTTP client for the NotSoTurboPuffer server."""

from __future__ import annotations

from typing import Any

import requests


class PufferError(RuntimeError):
    """Server returned a non-success status. Carries the server message."""


class PufferClient:
    def __init__(self, base_url: str, timeout_secs: float = 60.0):
        self.base_url = base_url.rstrip("/")
        self.timeout_secs = timeout_secs
        self.session = requests.Session()

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self.session.request(
            method, f"{self.base_url}{path}", timeout=self.timeout_secs, **kwargs
        )
        if response.status_code >= 400:
            try:
                message = response.json().get("error", response.text)
            except ValueError:
                message = response.text
            raise PufferError(f"{method} {path} -> {response.status_code}: {message}")
        return response.json() if response.content else None

    def health(self) -> None:
        response = self.session.get(f"{self.base_url}/health", timeout=self.timeout_secs)
        response.raise_for_status()

    def create_namespace(self, namespace: str) -> dict:
        return self._request("PUT", f"/v1/namespaces/{namespace}")

    def metadata(self, namespace: str) -> dict:
        return self._request("GET", f"/v1/namespaces/{namespace}")

    def upsert(self, namespace: str, rows: list[dict]) -> dict:
        return self._request(
            "POST", f"/v1/namespaces/{namespace}/upsert", json={"rows": rows}
        )

    def query(
        self,
        namespace: str,
        vector: list[float],
        top_k: int,
        filters: dict[str, str] | None = None,
    ) -> dict:
        body: dict[str, Any] = {"vector": vector, "top_k": top_k}
        if filters:
            body["filters"] = filters
        return self._request("POST", f"/v1/namespaces/{namespace}/query", json=body)

    def delete(self, namespace: str, ids: list) -> dict:
        return self._request(
            "POST", f"/v1/namespaces/{namespace}/delete", json={"ids": ids}
        )

    def patch(self, namespace: str, rows: list[dict]) -> dict:
        return self._request(
            "POST", f"/v1/namespaces/{namespace}/patch", json={"rows": rows}
        )

    def compact(self, namespace: str, force: bool = False) -> dict:
        params = {"force": "true"} if force else {}
        return self._request(
            "POST", f"/v1/namespaces/{namespace}/compact", params=params
        )
