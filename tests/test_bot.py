import base64
import asyncio
import json
import unittest
import uuid
from datetime import timedelta
from pathlib import Path
from unittest import mock

import requests

from bot_app import ActivationBotApp
from receipt_api import ReceiptApiClient
from settings import Settings
from session_data import (
    SessionData,
    SessionValidationError,
    build_outstock_user_candidates,
    combine_session_fragments,
    extract_outstock_user,
    utc_now,
)
from storage import BotStorage


class SessionDataTests(unittest.TestCase):
    def build_payload(self, **overrides):
        payload = {
            "user": {"email": "gempeak@ashixweb.com"},
            "expires": (utc_now() + timedelta(days=10)).isoformat(),
            "account": {"planType": "free"},
            "accessToken": "access-token",
            "sessionToken": "session-token",
        }
        payload.update(overrides)
        return payload

    def to_json_text(self, payload):
        return json.dumps(payload)

    def build_access_token(self, token_payload):
        encoded = base64.urlsafe_b64encode(
            json.dumps(token_payload).encode("utf-8")
        ).decode("utf-8").rstrip("=")
        return f"header.{encoded}.signature"

    def test_parse_valid_session(self):
        session = SessionData.parse(self.to_json_text(self.build_payload()))
        self.assertEqual(session.email, "gempeak@ashixweb.com")
        self.assertEqual(session.plan_type, "free")
        self.assertFalse(session.has_active_subscription)

    def test_parse_rejects_expired_session(self):
        payload = self.build_payload(expires=(utc_now() - timedelta(days=1)).isoformat())
        with self.assertRaises(SessionValidationError):
            SessionData.parse(self.to_json_text(payload))

    def test_parse_rejects_missing_email(self):
        payload = self.build_payload(user={})
        session = SessionData.parse(self.to_json_text(payload))
        self.assertEqual(session.email, "")

    def test_parse_reads_email_from_access_token_profile(self):
        payload = self.build_payload(
            user={},
            accessToken=self.build_access_token(
                {"https://api.openai.com/profile": {"email": "fallback@example.com"}}
            ),
        )
        session = SessionData.parse(self.to_json_text(payload))
        self.assertEqual(session.email, "fallback@example.com")

    def test_parse_rejects_cookie_export_array(self):
        payload = [
            {
                "name": "__Secure-next-auth.session-token",
                "value": "session-cookie",
                "domain": ".chatgpt.com",
            }
        ]
        with self.assertRaises(SessionValidationError):
            SessionData.parse(self.to_json_text(payload))

    def test_extract_outstock_user_returns_raw_session_json(self):
        payload = self.build_payload(
            accessToken="access-token-value",
            sessionToken="session-cookie-value",
        )
        raw_session = self.to_json_text(payload)
        self.assertEqual(extract_outstock_user(raw_session), raw_session)

    def test_build_outstock_user_candidates_returns_raw_session_only(self):
        payload = self.build_payload(
            user={
                "id": "user-123",
                "email": "gempeak@ashixweb.com",
                "idp": "auth0",
                "iat": 1771184855,
                "mfa": True,
            },
            accessToken="access-token-value",
            sessionToken="session-cookie-value",
        )
        raw_session = self.to_json_text(payload)
        candidates = build_outstock_user_candidates(self.to_json_text(payload))
        self.assertEqual(candidates, [("raw_session", raw_session)])

    def test_combine_session_fragments_reorders_out_of_order_parts(self):
        raw_session = self.to_json_text(
            self.build_payload(
                accessToken="access-token-value",
                sessionToken="session-cookie-value",
            )
        )
        first = raw_session[:40]
        middle = raw_session[40:120]
        last = raw_session[120:]
        combined = combine_session_fragments([middle, last, first])
        self.assertEqual(combined, raw_session)
        parsed = SessionData.parse(combined)
        self.assertEqual(parsed.plan_type, "free")


class StorageTests(unittest.TestCase):
    def test_state_and_order_persistence(self):
        db_path = Path.cwd() / f"test_{uuid.uuid4().hex}.sqlite3"
        try:
            storage = BotStorage(db_path)
            storage.save_state(
                1001,
                language="en",
                state="waiting_session_fragments",
                activation_code="ABC123",
                session_fragments=["part-1", "part-2"],
            )
            state = storage.get_state(1001)
            self.assertEqual(state["language"], "en")
            self.assertEqual(state["state"], "waiting_session_fragments")
            self.assertEqual(state["session_fragments"], ["part-1", "part-2"])

            order_id = storage.create_order(
                user_id=1001,
                activation_code="ABC123",
                app_name="ChatGPT",
                product_name="Plus 1M",
                email="gempeak@ashixweb.com",
                plan_type="free",
                raw_session='{"ok": true}',
                status="processing",
            )
            storage.update_order(order_id, status="success", task_result="done")
            storage.log_event(user_id=1001, event_type="test", details={"ok": True})

            row = storage.connection.execute(
                "SELECT status, task_result FROM orders WHERE id = ?",
                (order_id,),
            ).fetchone()
            self.assertEqual(row["status"], "success")
            self.assertEqual(row["task_result"], "done")
            storage.connection.close()
        finally:
            if db_path.exists():
                db_path.unlink()

    def test_reset_state_preserves_language(self):
        db_path = Path.cwd() / f"test_{uuid.uuid4().hex}.sqlite3"
        try:
            storage = BotStorage(db_path)
            storage.save_state(1001, language="en", state="waiting_activation_code")
            storage.reset_state(1001)
            state = storage.get_state(1001)
            self.assertEqual(state["language"], "en")
            self.assertEqual(state["state"], "idle")
        finally:
            storage.connection.close()
            if db_path.exists():
                db_path.unlink()

    def test_user_registry_and_notification_settings(self):
        db_path = Path.cwd() / f"test_{uuid.uuid4().hex}.sqlite3"
        try:
            storage = BotStorage(db_path)
            is_new = storage.upsert_user(
                user_id=2002,
                username="DexAshkan",
                first_name="Ashkan",
                last_name="",
                display_name="Ashkan",
                is_admin=True,
            )
            self.assertTrue(is_new)
            self.assertEqual(len(storage.list_recent_users()), 1)
            self.assertEqual(storage.get_user(2002)["username"], "DexAshkan")
            self.assertTrue(storage.get_notification_settings()["notify_new_user"])
            storage.set_setting_bool("notify_new_user", False)
            self.assertFalse(storage.get_notification_settings()["notify_new_user"])
            storage.set_admin_chat_id(2002)
            self.assertEqual(storage.get_admin_chat_id(), 2002)
        finally:
            storage.connection.close()
            if db_path.exists():
                db_path.unlink()

    def test_paginated_queries_for_users_and_orders(self):
        db_path = Path.cwd() / f"test_{uuid.uuid4().hex}.sqlite3"
        try:
            storage = BotStorage(db_path)
            storage.upsert_user(
                user_id=1,
                username="alpha",
                first_name="Alpha",
                last_name="",
                display_name="Alpha",
                is_admin=False,
            )
            storage.upsert_user(
                user_id=2,
                username="beta",
                first_name="Beta",
                last_name="",
                display_name="Beta",
                is_admin=False,
            )
            success_id = storage.create_order(
                user_id=1,
                activation_code="A1",
                app_name="ChatGPT",
                product_name="Plus 1M",
                email="a@example.com",
                plan_type="free",
                raw_session="{}",
                status="success",
            )
            failed_id = storage.create_order(
                user_id=2,
                activation_code="B1",
                app_name="ChatGPT",
                product_name="Plus 1M",
                email="b@example.com",
                plan_type="free",
                raw_session="{}",
                status="failed",
            )
            self.assertIsNotNone(success_id)
            self.assertIsNotNone(failed_id)
            self.assertEqual(storage.count_completed_orders_filtered("all"), 2)
            self.assertEqual(storage.count_completed_orders_filtered("success"), 1)
            self.assertEqual(storage.count_completed_orders_filtered("failed"), 1)
            success_rows = storage.query_completed_orders(
                limit=10,
                offset=0,
                status_filter="success",
                sort_key="newest",
            )
            self.assertEqual(len(success_rows), 1)
            self.assertEqual(success_rows[0]["user_id"], 1)
            self.assertEqual(storage.count_users_filtered("with_orders"), 2)
            self.assertEqual(storage.count_users_filtered("without_orders"), 0)
            user_rows = storage.query_users(
                limit=10,
                offset=0,
                filter_mode="all",
                sort_key="name_az",
            )
            self.assertEqual(len(user_rows), 2)
            self.assertEqual(user_rows[0]["username"], "alpha")

            email_rows = storage.query_completed_orders(
                limit=10,
                offset=0,
                status_filter="all",
                sort_key="newest",
                search_query="b@example.com",
            )
            self.assertEqual(len(email_rows), 1)
            self.assertEqual(email_rows[0]["user_id"], 2)

            username_rows = storage.query_completed_orders(
                limit=10,
                offset=0,
                status_filter="all",
                sort_key="newest",
                search_query="alpha",
            )
            self.assertEqual(len(username_rows), 1)
            self.assertEqual(username_rows[0]["user_id"], 1)

            activation_rows = storage.query_completed_orders(
                limit=10,
                offset=0,
                status_filter="all",
                sort_key="newest",
                search_query="B1",
            )
            self.assertEqual(len(activation_rows), 1)
            self.assertEqual(activation_rows[0]["user_id"], 2)

            searched_users = storage.query_users(
                limit=10,
                offset=0,
                filter_mode="all",
                sort_key="name_az",
                search_query="a@example.com",
            )
            self.assertEqual(len(searched_users), 1)
            self.assertEqual(searched_users[0]["username"], "alpha")

            self.assertEqual(storage.count_completed_orders_filtered("all", search_query="alpha"), 1)
            self.assertEqual(storage.count_users_filtered("all", search_query="b@example.com"), 1)
        finally:
            storage.connection.close()
            if db_path.exists():
                db_path.unlink()

    def test_user_order_history_queries_are_scoped_to_the_user(self):
        db_path = Path.cwd() / f"test_{uuid.uuid4().hex}.sqlite3"
        try:
            storage = BotStorage(db_path)
            first_success = storage.create_order(
                user_id=7,
                activation_code="H1",
                app_name="ChatGPT",
                product_name="Plus 1M",
                email="h1@example.com",
                plan_type="free",
                raw_session="{}",
                status="success",
            )
            second_failed = storage.create_order(
                user_id=7,
                activation_code="H2",
                app_name="ChatGPT",
                product_name="Go 1M",
                email="h2@example.com",
                plan_type="free",
                raw_session="{}",
                status="failed",
            )
            storage.create_order(
                user_id=9,
                activation_code="OTHER",
                app_name="ChatGPT",
                product_name="Other",
                email="other@example.com",
                plan_type="free",
                raw_session="{}",
                status="success",
            )
            self.assertIsNotNone(first_success)
            self.assertIsNotNone(second_failed)
            self.assertEqual(
                storage.count_user_completed_orders_filtered(user_id=7, status_filter="all"),
                2,
            )
            self.assertEqual(
                storage.count_user_completed_orders_filtered(user_id=7, status_filter="success"),
                1,
            )
            rows = storage.query_user_completed_orders(
                user_id=7,
                limit=10,
                offset=0,
                status_filter="all",
                sort_key="newest",
            )
            self.assertEqual(len(rows), 2)
            self.assertEqual({row["activation_code"] for row in rows}, {"H1", "H2"})
        finally:
            storage.connection.close()
            if db_path.exists():
                db_path.unlink()


class ApiClientTests(unittest.TestCase):
    @mock.patch("receipt_api.requests.request")
    def test_check_activation_code_uses_product_header(self, request_mock):
        response = mock.Mock()
        response.status_code = 200
        response.headers = {"Content-Type": "application/json"}
        response.json.return_value = {"used": False, "app_name": "ChatGPT"}
        request_mock.return_value = response

        client = ReceiptApiClient("https://receipt-api.nitro.xin", "chatgpt")
        client.check_activation_code("ABC123")

        _, kwargs = request_mock.call_args
        self.assertEqual(kwargs["headers"]["X-Product-ID"], "chatgpt")
        self.assertEqual(kwargs["json"]["code"], "ABC123")

    @mock.patch("receipt_api.requests.request")
    def test_create_outstock_order_uses_device_header(self, request_mock):
        response = mock.Mock()
        response.status_code = 200
        response.headers = {"Content-Type": "text/plain"}
        response.text = "task-id-123"
        request_mock.return_value = response

        client = ReceiptApiClient("https://receipt-api.nitro.xin", "chatgpt")
        task_id = client.create_outstock_order("ABC123", '{"ok": true}')

        _, kwargs = request_mock.call_args
        self.assertEqual(kwargs["headers"]["X-Product-ID"], "chatgpt")
        self.assertEqual(kwargs["headers"]["X-Device-Id"], "web")
        self.assertEqual(kwargs["json"]["cdk"], "ABC123")
        self.assertEqual(kwargs["json"]["user"], '{"ok": true}')
        self.assertEqual(task_id, "task-id-123")

    @mock.patch("receipt_api.requests.request")
    def test_check_activation_code_retries_temporary_failures(self, request_mock):
        success_response = mock.Mock()
        success_response.status_code = 200
        success_response.headers = {"Content-Type": "application/json"}
        success_response.json.return_value = {"used": False, "app_name": "ChatGPT"}
        request_mock.side_effect = [
            requests.RequestException("network down"),
            requests.RequestException("still down"),
            success_response,
        ]

        client = ReceiptApiClient("https://receipt-api.nitro.xin", "chatgpt")
        payload = client.check_activation_code("ABC123")

        self.assertEqual(request_mock.call_count, 3)
        self.assertEqual(payload["used"], False)


class SettingsTests(unittest.TestCase):
    def test_render_key_returns_english_copy(self):
        settings = Settings(
            telegram_bot_token="token",
            telegram_api_id=1,
            telegram_api_hash="hash",
            api_base_url="https://example.com",
            product_id="chatgpt",
            admin_username="@DexAshkan",
            support_username="@support",
            guide_link="https://t.me/example",
            database_path=Path("db.sqlite3"),
            log_path=Path("bot.log"),
            exports_path=Path("exports"),
            telethon_session_name="telethon_bot",
            session_window_seconds=4,
            session_max_messages=3,
            outstock_poll_seconds=10,
            outstock_timeout_seconds=600,
            renew_button_text="فعالسازی اشتراک ChatGPT",
            support_button_text="ارتباط با پشتیبانی",
            confirm_button_text="تایید و ادامه",
            cancel_button_text="انصراف",
            usage_status_ready_text="سالم و قابل استفاده",
            usage_status_used_text="قبلا استفاده شده",
            activation_checking_message="در حال بررسی کد فعالسازی...",
            session_checking_message="در حال بررسی سشن اکانت...",
            welcome_message="لطفا یکی از گزینه های زیر رو انتخاب کنید",
            support_message="برای پشتیبانی به {support} پیام بدهید.",
            support_hint_message="اگر مشکل را نتوانستید حل کنید به {support} پیام بدهید.",
            request_activation_code_message="",
            activation_info_message="",
            activation_invalid_message="",
            activation_used_message="",
            activation_check_error_message="",
            request_session_message="",
            session_invalid_message="",
            subscription_warning_message="",
            final_confirm_message="",
            processing_order_message="",
            order_submit_error_message="",
            order_poll_error_message="",
            order_result_message="",
            order_timeout_message="",
            cancelled_message="",
            generic_error_message="",
            in_progress_message="",
            return_to_menu_message="",
        )
        rendered = settings.render_key("support_message", language="en")
        self.assertIn("message", rendered.lower())
        self.assertIn("@", rendered)
        self.assertEqual(settings.admin_usernames, ("@DexAshkan",))
        self.assertEqual(settings.super_admin_usernames, ())

    @mock.patch("settings.load_env_file")
    def test_load_all_from_env_supports_multiple_bots(self, load_env_file_mock):
        del load_env_file_mock
        with mock.patch.dict(
            "os.environ",
            {
                "BOT_COUNT": "2",
                "BOT_1_TOKEN": "token-1",
                "SUPER_ADMIN_USERNAME": "@super1",
                "BOT_1_ADMIN_USERNAME": "@admin1, @admin1b",
                "BOT_1_SUPPORT_USERNAME": "@support1",
                "BOT_1_GUIDE_LINK": "https://t.me/guide1",
                "BOT_2_TOKEN": "token-2",
                "BOT_2_ADMIN_USERNAME": "@admin2, @admin2b",
                "BOT_2_SUPPORT_USERNAME": "@support2",
                "BOT_2_GUIDE_LINK": "https://t.me/guide2",
                "TELEGRAM_API_ID": "1",
                "TELEGRAM_API_HASH": "hash",
            },
            clear=True,
        ):
            settings_list = Settings.load_all_from_env()

        self.assertEqual(len(settings_list), 2)
        self.assertEqual(settings_list[0].telegram_bot_token, "token-1")
        self.assertEqual(settings_list[0].super_admin_usernames, ("@super1",))
        self.assertEqual(settings_list[0].admin_username, "@admin1, @admin1b")
        self.assertEqual(settings_list[0].admin_usernames, ("@admin1", "@admin1b"))
        self.assertEqual(settings_list[0].support_username, "@support1")
        self.assertEqual(settings_list[0].guide_link, "https://t.me/guide1")
        self.assertEqual(settings_list[0].exports_path.name, "bot1")
        self.assertEqual(settings_list[0].telethon_session_name, "telethon_bot_1")
        self.assertEqual(settings_list[1].telegram_bot_token, "token-2")
        self.assertEqual(settings_list[1].admin_username, "@admin2, @admin2b")
        self.assertEqual(settings_list[1].admin_usernames, ("@admin2", "@admin2b"))
        self.assertEqual(settings_list[1].support_username, "@support2")
        self.assertEqual(settings_list[1].guide_link, "https://t.me/guide2")
        self.assertEqual(settings_list[1].exports_path.name, "bot2")
        self.assertEqual(settings_list[1].telethon_session_name, "telethon_bot_2")
        self.assertEqual(settings_list[1].super_admin_usernames, ("@super1",))

    @mock.patch("settings.load_env_file")
    def test_load_all_from_env_keeps_base_paths_for_single_bot_count(self, load_env_file_mock):
        del load_env_file_mock
        with mock.patch.dict(
            "os.environ",
            {
                "BOT_COUNT": "1",
                "BOT_1_TOKEN": "token-1",
                "SUPER_ADMIN_USERNAME": "@super1",
                "BOT_1_ADMIN_USERNAME": "@admin1",
                "BOT_1_SUPPORT_USERNAME": "@support1",
                "BOT_1_GUIDE_LINK": "https://t.me/guide1",
                "TELEGRAM_API_ID": "1",
                "TELEGRAM_API_HASH": "hash",
                "DATABASE_PATH": "bot_data.sqlite3",
                "LOG_PATH": "bot.log",
                "EXPORTS_PATH": "exports",
                "TELETHON_SESSION_NAME": "telethon_bot",
            },
            clear=True,
        ):
            settings_list = Settings.load_all_from_env()

        self.assertEqual(len(settings_list), 1)
        self.assertEqual(settings_list[0].database_path.name, "bot_data.sqlite3")
        self.assertEqual(settings_list[0].log_path.name, "bot.log")
        self.assertEqual(settings_list[0].exports_path.name, "exports")
        self.assertEqual(settings_list[0].telethon_session_name, "telethon_bot")
        self.assertEqual(settings_list[0].super_admin_usernames, ("@super1",))

    @mock.patch("settings.load_env_file")
    def test_load_all_from_env_uses_global_support_defaults(self, load_env_file_mock):
        del load_env_file_mock
        with mock.patch.dict(
            "os.environ",
            {
                "BOT_COUNT": "2",
                "BOT_1_TOKEN": "token-1",
                "BOT_2_TOKEN": "token-2",
                "SUPER_ADMIN_USERNAME": "@super1",
                "BOT_1_ADMIN_USERNAME": "@admin1",
                "BOT_2_ADMIN_USERNAME": "@admin2",
                "SUPPORT_USERNAME": "@sharedsupport",
                "GUIDE_LINK": "https://t.me/sharedguide",
                "TELEGRAM_API_ID": "1",
                "TELEGRAM_API_HASH": "hash",
            },
            clear=True,
        ):
            settings_list = Settings.load_all_from_env()

        self.assertEqual(settings_list[0].support_username, "@sharedsupport")
        self.assertEqual(settings_list[1].support_username, "@sharedsupport")
        self.assertEqual(settings_list[0].guide_link, "https://t.me/sharedguide")
        self.assertEqual(settings_list[1].guide_link, "https://t.me/sharedguide")
        self.assertEqual(settings_list[0].super_admin_usernames, ("@super1",))
        self.assertEqual(settings_list[1].super_admin_usernames, ("@super1",))


class ActivationBotAppTaskTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_session_task_preserves_current_task_when_requested(self):
        app = ActivationBotApp.__new__(ActivationBotApp)
        app.session_tasks = {}

        current = asyncio.current_task()
        self.assertIsNotNone(current)
        app.session_tasks[42] = current

        app.cancel_session_task(42, preserve_current=True)

        self.assertNotIn(42, app.session_tasks)
        self.assertFalse(current.cancelled())

    async def test_activation_code_local_validation_allows_nonstandard_ascii(self):
        app = ActivationBotApp.__new__(ActivationBotApp)
        self.assertTrue(
            app._is_valid_activation_code("R_6FC104DC-2B94-40B7-952E-31D84DC3C52E")
        )
        self.assertEqual(
            app._normalize_activation_code("R_6FC104DC-2b94-40B7-952E-31D84DC3C52E"),
            "R_6FC104DC-2b94-40B7-952E-31D84DC3C52E",
        )

    async def test_activation_code_local_validation_rejects_spaces_and_persian(self):
        app = ActivationBotApp.__new__(ActivationBotApp)
        self.assertFalse(app._is_valid_activation_code("78DC 7BA3-DE88-485C"))
        self.assertFalse(app._is_valid_activation_code("کد-TEST-123"))

    async def test_activation_code_resolves_to_api_returned_code(self):
        app = ActivationBotApp.__new__(ActivationBotApp)
        resolved = app._resolve_activation_code(
            "R_6FC104DC-2B94-40B7-952E-31D84DC3C52E",
            {"code": "6FC104DC-2B94-40B7-952E-31D84DC3C52E"},
        )
        self.assertEqual(resolved, "6FC104DC-2B94-40B7-952E-31D84DC3C52E")

    async def test_chatgpt_command_detection(self):
        app = ActivationBotApp.__new__(ActivationBotApp)
        self.assertTrue(app._is_chatgpt_command("/chatgpt"))
        self.assertTrue(app._is_chatgpt_command("/chatgpt@TestBot"))
        self.assertFalse(app._is_chatgpt_command("/chatgpt extra"))

    async def test_admin_command_detection(self):
        app = ActivationBotApp.__new__(ActivationBotApp)
        self.assertTrue(app._is_admin_command("/admin"))
        self.assertTrue(app._is_admin_command("/admin@TestBot"))
        self.assertFalse(app._is_admin_command("/admin extra"))

    async def test_admin_identity_accepts_multiple_usernames(self):
        app = ActivationBotApp.__new__(ActivationBotApp)
        app.settings = mock.Mock(
            admin_usernames=("@GemPeakSup",),
            super_admin_usernames=("@DexAshkan",),
        )
        app.storage = mock.Mock()
        app.storage.get_admin_chat_id.return_value = None
        self.assertTrue(app._is_admin_identity(1, "DexAshkan"))
        self.assertTrue(app._is_admin_identity(1, "GemPeakSup"))
        self.assertFalse(app._is_admin_identity(1, "AnotherUser"))

    async def test_parse_admin_view_data(self):
        app = ActivationBotApp.__new__(ActivationBotApp)
        parsed = app._parse_admin_view_data(b"admin:view:history:2:50:success:newest")
        self.assertEqual(parsed["view_kind"], "history")
        self.assertEqual(parsed["page"], 2)
        self.assertEqual(parsed["per_page"], 50)
        self.assertEqual(parsed["filter_key"], "success")
        self.assertEqual(parsed["sort_key"], "newest")

    async def test_telegram_id_text_prefers_username(self):
        app = ActivationBotApp.__new__(ActivationBotApp)
        self.assertEqual(app._telegram_id_text("DexAshkan"), "@DexAshkan")
        self.assertEqual(app._telegram_id_text("@DexAshkan"), "@DexAshkan")
        self.assertEqual(app._telegram_id_text(""), "-")

    async def test_admin_notification_targets_collects_all_known_admins(self):
        app = ActivationBotApp.__new__(ActivationBotApp)
        app.storage = mock.Mock()
        app.storage.list_admin_users.return_value = [
            {"user_id": 10},
            {"user_id": 20},
        ]
        app.storage.get_admin_chat_id.return_value = 30
        self.assertEqual(app._admin_notification_targets(), [10, 20, 30])


if __name__ == "__main__":
    unittest.main()
