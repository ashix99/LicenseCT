from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class BotStorage:
    STATE_COLUMNS = (
        "user_id",
        "language",
        "state",
        "activation_code",
        "activation_app_name",
        "activation_product_name",
        "activation_payload",
        "session_fragments",
        "session_started_at",
        "session_email",
        "session_plan_type",
        "raw_session",
        "order_id",
        "updated_at",
    )

    ORDER_UPDATE_COLUMNS = {
        "task_id",
        "task_result",
        "status",
        "updated_at",
    }

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self._initialize()

    def _initialize(self) -> None:
        with self.lock:
            self.connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS chat_state (
                    user_id INTEGER PRIMARY KEY,
                    language TEXT NOT NULL DEFAULT '',
                    state TEXT NOT NULL,
                    activation_code TEXT,
                    activation_app_name TEXT,
                    activation_product_name TEXT,
                    activation_payload TEXT,
                    session_fragments TEXT NOT NULL DEFAULT '[]',
                    session_started_at TEXT,
                    session_email TEXT,
                    session_plan_type TEXT,
                    raw_session TEXT,
                    order_id INTEGER,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    activation_code TEXT,
                    app_name TEXT,
                    product_name TEXT,
                    email TEXT,
                    plan_type TEXT,
                    raw_session TEXT,
                    task_id TEXT,
                    task_result TEXT,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS event_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    event_type TEXT NOT NULL,
                    details TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
                CREATE INDEX IF NOT EXISTS idx_orders_task_id ON orders(task_id);
                CREATE INDEX IF NOT EXISTS idx_event_logs_user_id ON event_logs(user_id);
                """
            )
            columns = {
                row["name"]
                for row in self.connection.execute("PRAGMA table_info(chat_state)")
            }
            if "language" not in columns:
                self.connection.execute(
                    "ALTER TABLE chat_state ADD COLUMN language TEXT NOT NULL DEFAULT ''"
                )
            self.connection.commit()

    def _default_state(self, user_id: int) -> dict[str, Any]:
        return {
            "user_id": user_id,
            "language": "",
            "state": "idle",
            "activation_code": "",
            "activation_app_name": "",
            "activation_product_name": "",
            "activation_payload": "",
            "session_fragments": [],
            "session_started_at": "",
            "session_email": "",
            "session_plan_type": "",
            "raw_session": "",
            "order_id": None,
            "updated_at": utc_now_iso(),
        }

    def get_state(self, user_id: int) -> dict[str, Any]:
        with self.lock:
            row = self.connection.execute(
                "SELECT * FROM chat_state WHERE user_id = ?",
                (user_id,),
            ).fetchone()

        if row is None:
            return self._default_state(user_id)

        state = dict(row)
        state["session_fragments"] = json.loads(state.get("session_fragments") or "[]")
        return state

    def save_state(self, user_id: int, **updates: Any) -> dict[str, Any]:
        state = self.get_state(user_id)
        state.update(updates)
        state["user_id"] = user_id
        state["updated_at"] = utc_now_iso()

        payload = dict(state)
        payload["session_fragments"] = json.dumps(
            state.get("session_fragments", []), ensure_ascii=False
        )

        placeholders = ", ".join("?" for _ in self.STATE_COLUMNS)
        assignments = ", ".join(
            f"{column} = excluded.{column}"
            for column in self.STATE_COLUMNS
            if column != "user_id"
        )
        values = [payload[column] for column in self.STATE_COLUMNS]

        with self.lock:
            self.connection.execute(
                f"""
                INSERT INTO chat_state ({", ".join(self.STATE_COLUMNS)})
                VALUES ({placeholders})
                ON CONFLICT(user_id) DO UPDATE SET {assignments}
                """,
                values,
            )
            self.connection.commit()

        return state

    def reset_state(self, user_id: int) -> None:
        state = self._default_state(user_id)
        current = self.get_state(user_id)
        state["language"] = current.get("language", "")
        payload = dict(state)
        payload["session_fragments"] = json.dumps([], ensure_ascii=False)
        placeholders = ", ".join("?" for _ in self.STATE_COLUMNS)
        assignments = ", ".join(
            f"{column} = excluded.{column}"
            for column in self.STATE_COLUMNS
            if column != "user_id"
        )
        values = [payload[column] for column in self.STATE_COLUMNS]

        with self.lock:
            self.connection.execute(
                f"""
                INSERT INTO chat_state ({", ".join(self.STATE_COLUMNS)})
                VALUES ({placeholders})
                ON CONFLICT(user_id) DO UPDATE SET {assignments}
                """,
                values,
            )
            self.connection.commit()

    def create_order(
        self,
        *,
        user_id: int,
        activation_code: str,
        app_name: str,
        product_name: str,
        email: str,
        plan_type: str,
        raw_session: str,
        status: str,
    ) -> int:
        now = utc_now_iso()
        with self.lock:
            cursor = self.connection.execute(
                """
                INSERT INTO orders (
                    user_id,
                    activation_code,
                    app_name,
                    product_name,
                    email,
                    plan_type,
                    raw_session,
                    status,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    activation_code,
                    app_name,
                    product_name,
                    email,
                    plan_type,
                    raw_session,
                    status,
                    now,
                    now,
                ),
            )
            self.connection.commit()
            return int(cursor.lastrowid)

    def update_order(self, order_id: int, **updates: Any) -> None:
        if not updates:
            return

        invalid = set(updates).difference(self.ORDER_UPDATE_COLUMNS)
        if invalid:
            raise ValueError(f"Unsupported order fields: {', '.join(sorted(invalid))}")

        updates["updated_at"] = utc_now_iso()
        assignments = ", ".join(f"{column} = ?" for column in updates)
        values = list(updates.values()) + [order_id]

        with self.lock:
            self.connection.execute(
                f"UPDATE orders SET {assignments} WHERE id = ?",
                values,
            )
            self.connection.commit()

    def log_event(
        self,
        *,
        user_id: int | None,
        event_type: str,
        details: Any,
    ) -> None:
        if isinstance(details, (dict, list)):
            serialized = json.dumps(details, ensure_ascii=False)
        else:
            serialized = str(details)

        with self.lock:
            self.connection.execute(
                """
                INSERT INTO event_logs (user_id, event_type, details, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (user_id, event_type, serialized, utc_now_iso()),
            )
            self.connection.commit()
