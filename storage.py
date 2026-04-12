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

    DEFAULT_NOTIFICATION_SETTINGS = {
        "notify_new_user": True,
        "notify_activation_success": True,
        "notify_activation_failed": True,
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

                CREATE TABLE IF NOT EXISTS chat_users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    display_name TEXT,
                    is_admin INTEGER NOT NULL DEFAULT 0,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS bot_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
                CREATE INDEX IF NOT EXISTS idx_orders_task_id ON orders(task_id);
                CREATE INDEX IF NOT EXISTS idx_event_logs_user_id ON event_logs(user_id);
                CREATE INDEX IF NOT EXISTS idx_chat_users_last_seen ON chat_users(last_seen_at);
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
            self._ensure_default_settings()
            self.connection.commit()

    def _ensure_default_settings(self) -> None:
        now = utc_now_iso()
        for key, enabled in self.DEFAULT_NOTIFICATION_SETTINGS.items():
            self.connection.execute(
                """
                INSERT INTO bot_settings (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO NOTHING
                """,
                (key, "1" if enabled else "0", now),
            )

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

    def upsert_user(
        self,
        *,
        user_id: int,
        username: str,
        first_name: str,
        last_name: str,
        display_name: str,
        is_admin: bool,
    ) -> bool:
        now = utc_now_iso()
        with self.lock:
            existing = self.connection.execute(
                "SELECT 1 FROM chat_users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            is_new = existing is None
            if is_new:
                self.connection.execute(
                    """
                    INSERT INTO chat_users (
                        user_id,
                        username,
                        first_name,
                        last_name,
                        display_name,
                        is_admin,
                        first_seen_at,
                        last_seen_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        user_id,
                        username,
                        first_name,
                        last_name,
                        display_name,
                        1 if is_admin else 0,
                        now,
                        now,
                    ),
                )
            else:
                self.connection.execute(
                    """
                    UPDATE chat_users
                    SET username = ?,
                        first_name = ?,
                        last_name = ?,
                        display_name = ?,
                        is_admin = ?,
                        last_seen_at = ?
                    WHERE user_id = ?
                    """,
                    (
                        username,
                        first_name,
                        last_name,
                        display_name,
                        1 if is_admin else 0,
                        now,
                        user_id,
                    ),
                )
            self.connection.commit()
            return is_new

    def get_user(self, user_id: int) -> dict[str, Any] | None:
        with self.lock:
            row = self.connection.execute(
                "SELECT * FROM chat_users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
        return dict(row) if row else None

    def list_admin_users(self) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.connection.execute(
                """
                SELECT user_id, username, first_name, last_name, display_name, is_admin,
                       first_seen_at, last_seen_at
                FROM chat_users
                WHERE is_admin = 1
                ORDER BY last_seen_at DESC, user_id DESC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def list_recent_users(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.connection.execute(
                """
                SELECT user_id, username, first_name, last_name, display_name, is_admin,
                       first_seen_at, last_seen_at
                FROM chat_users
                ORDER BY last_seen_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def _users_having_clause(self, filter_mode: str) -> str:
        if filter_mode == "with_orders":
            return "HAVING COUNT(orders.id) > 0"
        if filter_mode == "without_orders":
            return "HAVING COUNT(orders.id) = 0"
        return ""

    def _normalized_search_value(self, search_query: str) -> str:
        return f"%{str(search_query or '').strip().lower()}%"

    def _users_search_clause(self, search_query: str) -> tuple[str, list[Any]]:
        normalized = self._normalized_search_value(search_query)
        if normalized == "%%":
            return "", []
        return (
            """
            WHERE (
                LOWER(COALESCE(chat_users.username, '')) LIKE ?
                OR LOWER(COALESCE(chat_users.display_name, '')) LIKE ?
                OR LOWER(COALESCE(chat_users.first_name, '')) LIKE ?
                OR LOWER(COALESCE(chat_users.last_name, '')) LIKE ?
                OR LOWER(COALESCE(orders.email, '')) LIKE ?
                OR LOWER(COALESCE(orders.activation_code, '')) LIKE ?
            )
            """,
            [normalized] * 6,
        )

    def _users_sort_clause(self, sort_key: str) -> str:
        sort_map = {
            "joined_new": "chat_users.first_seen_at DESC, chat_users.user_id DESC",
            "joined_old": "chat_users.first_seen_at ASC, chat_users.user_id ASC",
            "last_seen": "chat_users.last_seen_at DESC, chat_users.user_id DESC",
            "last_order": "CASE WHEN MAX(orders.updated_at) IS NULL THEN 1 ELSE 0 END, MAX(orders.updated_at) DESC, chat_users.last_seen_at DESC",
            "name_az": "LOWER(COALESCE(chat_users.display_name, '')) ASC, chat_users.user_id ASC",
            "name_za": "LOWER(COALESCE(chat_users.display_name, '')) DESC, chat_users.user_id DESC",
        }
        return sort_map.get(sort_key, sort_map["last_seen"])

    def list_all_users(self) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.connection.execute(
                """
                SELECT user_id, username, first_name, last_name, display_name, is_admin,
                       first_seen_at, last_seen_at
                FROM chat_users
                ORDER BY last_seen_at DESC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def query_users(
        self,
        *,
        limit: int,
        offset: int,
        filter_mode: str,
        sort_key: str,
        search_query: str = "",
    ) -> list[dict[str, Any]]:
        having_clause = self._users_having_clause(filter_mode)
        order_clause = self._users_sort_clause(sort_key)
        where_clause, params = self._users_search_clause(search_query)
        with self.lock:
            rows = self.connection.execute(
                f"""
                SELECT
                    chat_users.user_id,
                    chat_users.username,
                    chat_users.first_name,
                    chat_users.last_name,
                    chat_users.display_name,
                    chat_users.is_admin,
                    chat_users.first_seen_at,
                    chat_users.last_seen_at,
                    COUNT(orders.id) AS total_orders,
                    MAX(orders.updated_at) AS last_order_at
                FROM chat_users
                LEFT JOIN orders
                    ON orders.user_id = chat_users.user_id
                    AND orders.status != 'processing'
                {where_clause}
                GROUP BY
                    chat_users.user_id,
                    chat_users.username,
                    chat_users.first_name,
                    chat_users.last_name,
                    chat_users.display_name,
                    chat_users.is_admin,
                    chat_users.first_seen_at,
                    chat_users.last_seen_at
                {having_clause}
                ORDER BY {order_clause}
                LIMIT ? OFFSET ?
                """,
                [*params, limit, offset],
            ).fetchall()
        return [dict(row) for row in rows]

    def count_users_filtered(self, filter_mode: str, search_query: str = "") -> int:
        having_clause = self._users_having_clause(filter_mode)
        where_clause, params = self._users_search_clause(search_query)
        with self.lock:
            row = self.connection.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM (
                    SELECT chat_users.user_id
                    FROM chat_users
                    LEFT JOIN orders
                        ON orders.user_id = chat_users.user_id
                        AND orders.status != 'processing'
                    {where_clause}
                    GROUP BY chat_users.user_id
                    {having_clause}
                ) AS filtered_users
                """,
                params,
            ).fetchone()
        return int(row["count"] if row else 0)

    def count_users(self) -> int:
        with self.lock:
            row = self.connection.execute(
                "SELECT COUNT(*) AS count FROM chat_users"
            ).fetchone()
        return int(row["count"] if row else 0)

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

    def get_order(self, order_id: int) -> dict[str, Any] | None:
        with self.lock:
            row = self.connection.execute(
                "SELECT * FROM orders WHERE id = ?",
                (order_id,),
            ).fetchone()
        return dict(row) if row else None

    def list_recent_completed_orders(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.connection.execute(
                """
                SELECT
                    orders.id,
                    orders.user_id,
                    orders.activation_code,
                    orders.app_name,
                    orders.product_name,
                    orders.email,
                    orders.plan_type,
                    orders.task_id,
                    orders.task_result,
                    orders.status,
                    orders.created_at,
                    orders.updated_at,
                    chat_users.username,
                    chat_users.first_name,
                    chat_users.last_name,
                    chat_users.display_name
                FROM orders
                LEFT JOIN chat_users ON chat_users.user_id = orders.user_id
                WHERE orders.status != 'processing'
                ORDER BY orders.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def _orders_where_clause(self, status_filter: str) -> tuple[str, list[Any]]:
        where_parts = ["orders.status != 'processing'"]
        params: list[Any] = []
        if status_filter == "success":
            where_parts.append("orders.status = ?")
            params.append("success")
        elif status_filter == "failed":
            where_parts.append("orders.status != ?")
            params.append("success")
        return " AND ".join(where_parts), params

    def _orders_search_clause(self, search_query: str) -> tuple[str, list[Any]]:
        normalized = self._normalized_search_value(search_query)
        if normalized == "%%":
            return "", []
        return (
            """
            AND (
                LOWER(COALESCE(orders.email, '')) LIKE ?
                OR LOWER(COALESCE(orders.activation_code, '')) LIKE ?
                OR LOWER(COALESCE(chat_users.username, '')) LIKE ?
                OR LOWER(COALESCE(chat_users.display_name, '')) LIKE ?
                OR LOWER(COALESCE(chat_users.first_name, '')) LIKE ?
                OR LOWER(COALESCE(chat_users.last_name, '')) LIKE ?
            )
            """,
            [normalized] * 6,
        )

    def _orders_sort_clause(self, sort_key: str) -> str:
        sort_map = {
            "newest": "orders.updated_at DESC, orders.id DESC",
            "oldest": "orders.updated_at ASC, orders.id ASC",
        }
        return sort_map.get(sort_key, sort_map["newest"])

    def _user_orders_where_clause(self, user_id: int, status_filter: str) -> tuple[str, list[Any]]:
        where_parts = [
            "orders.user_id = ?",
            "orders.status != 'processing'",
        ]
        params: list[Any] = [user_id]
        if status_filter == "success":
            where_parts.append("orders.status = ?")
            params.append("success")
        elif status_filter == "failed":
            where_parts.append("orders.status != ?")
            params.append("success")
        return " AND ".join(where_parts), params

    def list_all_completed_orders(self) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.connection.execute(
                """
                SELECT
                    orders.id,
                    orders.user_id,
                    orders.activation_code,
                    orders.app_name,
                    orders.product_name,
                    orders.email,
                    orders.plan_type,
                    orders.task_id,
                    orders.task_result,
                    orders.status,
                    orders.created_at,
                    orders.updated_at,
                    chat_users.username,
                    chat_users.first_name,
                    chat_users.last_name,
                    chat_users.display_name
                FROM orders
                LEFT JOIN chat_users ON chat_users.user_id = orders.user_id
                WHERE orders.status != 'processing'
                ORDER BY orders.id DESC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def query_completed_orders(
        self,
        *,
        limit: int,
        offset: int,
        status_filter: str,
        sort_key: str,
        search_query: str = "",
    ) -> list[dict[str, Any]]:
        where_clause, params = self._orders_where_clause(status_filter)
        search_clause, search_params = self._orders_search_clause(search_query)
        order_clause = self._orders_sort_clause(sort_key)
        with self.lock:
            rows = self.connection.execute(
                f"""
                SELECT
                    orders.id,
                    orders.user_id,
                    orders.activation_code,
                    orders.app_name,
                    orders.product_name,
                    orders.email,
                    orders.plan_type,
                    orders.task_id,
                    orders.task_result,
                    orders.status,
                    orders.created_at,
                    orders.updated_at,
                    chat_users.username,
                    chat_users.first_name,
                    chat_users.last_name,
                    chat_users.display_name
                FROM orders
                LEFT JOIN chat_users ON chat_users.user_id = orders.user_id
                WHERE {where_clause}
                {search_clause}
                ORDER BY {order_clause}
                LIMIT ? OFFSET ?
                """,
                [*params, *search_params, limit, offset],
            ).fetchall()
        return [dict(row) for row in rows]

    def count_completed_orders_filtered(
        self,
        status_filter: str,
        search_query: str = "",
    ) -> int:
        where_clause, params = self._orders_where_clause(status_filter)
        search_clause, search_params = self._orders_search_clause(search_query)
        with self.lock:
            row = self.connection.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM orders
                LEFT JOIN chat_users ON chat_users.user_id = orders.user_id
                WHERE {where_clause}
                {search_clause}
                """,
                [*params, *search_params],
            ).fetchone()
        return int(row["count"] if row else 0)

    def count_completed_orders(self) -> int:
        with self.lock:
            row = self.connection.execute(
                "SELECT COUNT(*) AS count FROM orders WHERE status != 'processing'"
            ).fetchone()
        return int(row["count"] if row else 0)

    def query_user_completed_orders(
        self,
        *,
        user_id: int,
        limit: int,
        offset: int,
        status_filter: str,
        sort_key: str,
    ) -> list[dict[str, Any]]:
        where_clause, params = self._user_orders_where_clause(user_id, status_filter)
        order_clause = self._orders_sort_clause(sort_key)
        with self.lock:
            rows = self.connection.execute(
                f"""
                SELECT
                    orders.id,
                    orders.user_id,
                    orders.activation_code,
                    orders.app_name,
                    orders.product_name,
                    orders.email,
                    orders.plan_type,
                    orders.task_id,
                    orders.task_result,
                    orders.status,
                    orders.created_at,
                    orders.updated_at
                FROM orders
                WHERE {where_clause}
                ORDER BY {order_clause}
                LIMIT ? OFFSET ?
                """,
                [*params, limit, offset],
            ).fetchall()
        return [dict(row) for row in rows]

    def count_user_completed_orders_filtered(
        self,
        *,
        user_id: int,
        status_filter: str,
    ) -> int:
        where_clause, params = self._user_orders_where_clause(user_id, status_filter)
        with self.lock:
            row = self.connection.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM orders
                WHERE {where_clause}
                """,
                params,
            ).fetchone()
        return int(row["count"] if row else 0)

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

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        with self.lock:
            row = self.connection.execute(
                "SELECT value FROM bot_settings WHERE key = ?",
                (key,),
            ).fetchone()
        if row is None:
            return default
        return str(row["value"])

    def set_setting(self, key: str, value: str) -> None:
        with self.lock:
            self.connection.execute(
                """
                INSERT INTO bot_settings (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (key, value, utc_now_iso()),
            )
            self.connection.commit()

    def get_setting_bool(self, key: str, default: bool) -> bool:
        value = self.get_setting(key)
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def set_setting_bool(self, key: str, enabled: bool) -> None:
        self.set_setting(key, "1" if enabled else "0")

    def get_admin_chat_id(self) -> int | None:
        value = self.get_setting("admin_chat_id")
        if not value:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def set_admin_chat_id(self, user_id: int) -> None:
        self.set_setting("admin_chat_id", str(user_id))

    def get_notification_settings(self) -> dict[str, bool]:
        return {
            key: self.get_setting_bool(key, default)
            for key, default in self.DEFAULT_NOTIFICATION_SETTINGS.items()
        }
