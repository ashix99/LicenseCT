from __future__ import annotations

import base64
import json
from itertools import permutations
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


def combine_session_fragments(fragments: list[str]) -> str:
    cleaned = [str(fragment or "").strip() for fragment in fragments if str(fragment or "").strip()]
    if not cleaned:
        raise SessionValidationError("سشن اکانت خالی است.")

    candidates: list[str] = []
    seen: set[str] = set()

    def add_candidate(parts: list[str] | tuple[str, ...]) -> None:
        joined = "".join(parts).strip()
        if joined and joined not in seen:
            seen.add(joined)
            candidates.append(joined)

    add_candidate(cleaned)

    heuristic_parts = _heuristic_fragment_order(cleaned)
    add_candidate(heuristic_parts)

    if len(cleaned) <= 4:
        ordered_permutations = sorted(
            permutations(cleaned),
            key=_fragment_order_score,
            reverse=True,
        )
        for permuted in ordered_permutations:
            add_candidate(permuted)

    for candidate in candidates:
        if _looks_like_json_payload(candidate):
            return candidate

    for candidate in candidates:
        if _looks_like_complete_json(candidate):
            return candidate

    return candidates[0]


def _heuristic_fragment_order(fragments: list[str]) -> list[str]:
    if len(fragments) <= 1:
        return list(fragments)

    first_index = next(
        (index for index, fragment in enumerate(fragments) if fragment.lstrip().startswith(("{", "["))),
        None,
    )
    last_index = next(
        (
            index
            for index in range(len(fragments) - 1, -1, -1)
            if fragments[index].rstrip().endswith(("}", "]"))
        ),
        None,
    )

    ordered: list[str] = []
    used: set[int] = set()
    if first_index is not None:
        ordered.append(fragments[first_index])
        used.add(first_index)

    for index, fragment in enumerate(fragments):
        if index in used:
            continue
        if last_index is not None and index == last_index:
            continue
        ordered.append(fragment)
        used.add(index)

    if last_index is not None and last_index not in used:
        ordered.append(fragments[last_index])
        used.add(last_index)

    for index, fragment in enumerate(fragments):
        if index not in used:
            ordered.append(fragment)

    return ordered


def _looks_like_complete_json(value: str) -> bool:
    stripped = str(value or "").strip()
    return (
        stripped.startswith("{")
        and stripped.endswith("}")
    ) or (
        stripped.startswith("[")
        and stripped.endswith("]")
    )


def _looks_like_json_payload(value: str) -> bool:
    if not _looks_like_complete_json(value):
        return False
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return False
    return isinstance(payload, (dict, list))


def _fragment_order_score(parts: tuple[str, ...]) -> tuple[int, int, int]:
    if not parts:
        return (0, 0, 0)
    joined = "".join(parts).strip()
    first_score = 1 if parts[0].lstrip().startswith(("{", "[")) else 0
    last_score = 1 if parts[-1].rstrip().endswith(("}", "]")) else 0
    json_score = 1 if _looks_like_json_payload(joined) else 0
    return (json_score, first_score, last_score)


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
