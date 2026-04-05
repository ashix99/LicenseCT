from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


class SessionValidationError(ValueError):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_datetime(value: str) -> datetime:
    normalized = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def deep_get(source: dict[str, Any], *path: str) -> Any:
    current: Any = source
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def build_outstock_user_candidates(raw_text: str) -> list[tuple[str, str]]:
    normalized = SessionData._normalize_text(raw_text)
    if not normalized:
        return []
    return [("raw_session", normalized)]


def extract_outstock_user(raw_text: str) -> str:
    return SessionData._normalize_text(raw_text)


def extract_email(payload: dict[str, Any]) -> str:
    email = str(deep_get(payload, "user", "email") or "").strip()
    if email:
        return email

    access_token = str(payload.get("accessToken") or "").strip()
    if not access_token:
        return ""

    token_parts = access_token.split(".")
    if len(token_parts) < 2:
        return ""

    padded = token_parts[1] + "=" * (-len(token_parts[1]) % 4)
    try:
        decoded = base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8")
        token_payload = json.loads(decoded)
    except (ValueError, json.JSONDecodeError):
        return ""

    profile = token_payload.get("https://api.openai.com/profile")
    if not isinstance(profile, dict):
        return ""
    return str(profile.get("email") or "").strip()


@dataclass(slots=True)
class SessionData:
    raw_text: str
    payload: dict[str, Any]
    email: str
    plan_type: str
    expires_at: datetime

    @property
    def has_active_subscription(self) -> bool:
        return self.plan_type.strip().lower() != "free"

    @classmethod
    def parse(cls, raw_text: str) -> "SessionData":
        normalized = cls._normalize_text(raw_text)
        try:
            payload = json.loads(normalized)
        except json.JSONDecodeError as exc:
            raise SessionValidationError("ساختار سشن اکانت معتبر نیست.") from exc

        if isinstance(payload, list):
            if payload and all(isinstance(item, dict) for item in payload):
                cookie_names = {
                    str(item.get("name") or "").strip()
                    for item in payload
                    if isinstance(item, dict)
                }
                if "__Secure-next-auth.session-token" in cookie_names:
                    raise SessionValidationError(
                        "این داده خروجی کوکی مرورگر است، نه سشن JSON کامل. لطفا سشن اکانت کامل را ارسال کنید."
                    )
            raise SessionValidationError(
                "فرمت سشن اکانت اشتباه است. لطفا سشن JSON کامل را ارسال کنید."
            )

        if not isinstance(payload, dict):
            raise SessionValidationError(
                "فرمت سشن اکانت اشتباه است. لطفا سشن JSON کامل را ارسال کنید."
            )

        email = extract_email(payload)

        expires_raw = str(payload.get("expires") or "").strip()
        if not expires_raw:
            raise SessionValidationError("زمان انقضای سشن اکانت پیدا نشد.")

        try:
            expires_at = parse_iso_datetime(expires_raw)
        except ValueError as exc:
            raise SessionValidationError("زمان انقضای سشن اکانت معتبر نیست.") from exc

        if expires_at <= utc_now():
            raise SessionValidationError("سشن اکانت منقضی شده است.")

        access_token = str(payload.get("accessToken") or "").strip()
        if not access_token:
            raise SessionValidationError("accessToken داخل سشن اکانت پیدا نشد.")

        session_token = str(payload.get("sessionToken") or "").strip()
        if not session_token:
            raise SessionValidationError("sessionToken داخل سشن اکانت پیدا نشد.")

        plan_type = str(deep_get(payload, "account", "planType") or "unknown").strip()
        if not plan_type:
            plan_type = "unknown"

        return cls(
            raw_text=normalized,
            payload=payload,
            email=email,
            plan_type=plan_type,
            expires_at=expires_at,
        )

    @staticmethod
    def _normalize_text(raw_text: str) -> str:
        stripped = raw_text.strip()
        if not stripped:
            raise SessionValidationError("سشن اکانت خالی است.")

        if stripped.startswith("```") and stripped.endswith("```"):
            lines = stripped.splitlines()
            if len(lines) >= 3:
                body = lines[1:-1]
                if body and body[0].strip().lower() == "json":
                    body = body[1:]
                stripped = "\n".join(body).strip()

        is_object = stripped.startswith("{") and stripped.endswith("}")
        is_array = stripped.startswith("[") and stripped.endswith("]")
        if not is_object and not is_array:
            raise SessionValidationError("فرمت JSON سشن اکانت کامل نیست.")

        return stripped
