from __future__ import annotations

from typing import Any

import requests


class ApiError(RuntimeError):
    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        *,
        request_id: str | None = None,
        response_body: str | None = None,
        response_headers: dict[str, str] | None = None,
        path: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.request_id = request_id
        self.response_body = response_body
        self.response_headers = response_headers or {}
        self.path = path

    def to_dict(self) -> dict[str, object]:
        return {
            "message": str(self),
            "status_code": self.status_code,
            "request_id": self.request_id,
            "path": self.path,
            "response_body": self.response_body,
            "response_headers": self.response_headers,
        }


class ReceiptApiClient:
    def __init__(self, base_url: str, product_id: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.product_id = product_id

    def _request(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=json_body,
                timeout=30,
            )
        except requests.RequestException as exc:
            raise ApiError("ارتباط با API برقرار نشد.", path=path) from exc

        if response.status_code != 200:
            error_text = response.text.strip() or f"HTTP {response.status_code}"
            raise ApiError(
                error_text,
                status_code=response.status_code,
                request_id=response.headers.get("X-Request-Id"),
                response_body=response.text[:1000],
                response_headers=dict(response.headers),
                path=path,
            )

        content_type = response.headers.get("Content-Type", "").lower()
        if "application/json" in content_type:
            try:
                return response.json()
            except ValueError as exc:
                raise ApiError(
                    "پاسخ JSON از API معتبر نبود.",
                    request_id=response.headers.get("X-Request-Id"),
                    response_body=response.text[:1000],
                    response_headers=dict(response.headers),
                    path=path,
                ) from exc

        return response.text.strip()

    def check_activation_code(self, code: str) -> dict[str, Any]:
        payload = self._request(
            "POST",
            "/cdks/public/check",
            headers={
                "Content-Type": "application/json",
                "X-Product-ID": self.product_id,
            },
            json_body={"code": code},
        )
        if not isinstance(payload, dict):
            raise ApiError("پاسخ بررسی کد فعالسازی قابل خواندن نبود.")
        return payload

    def create_outstock_order(self, code: str, user_session: str) -> str:
        payload = self._request(
            "POST",
            "/stocks/public/outstock",
            headers={
                "Content-Type": "application/json",
                "X-Product-ID": self.product_id,
                "X-Device-Id": "web",
            },
            json_body={"cdk": code, "user": user_session},
        )
        if not isinstance(payload, str) or not payload.strip():
            raise ApiError("task_id از API دریافت نشد.")
        return payload.strip()

    def get_outstock_status(self, task_id: str) -> dict[str, Any]:
        payload = self._request(
            "GET",
            f"/stocks/public/outstock/{task_id}",
        )
        if not isinstance(payload, dict):
            raise ApiError("پاسخ وضعیت سفارش قابل خواندن نبود.")
        return payload
