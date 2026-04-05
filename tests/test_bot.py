import base64
import asyncio
import json
import unittest
import uuid
from datetime import timedelta
from pathlib import Path
from unittest import mock

from bot_app import ActivationBotApp
from receipt_api import ReceiptApiClient
from settings import Settings
from session_data import (
    SessionData,
    SessionValidationError,
    build_outstock_user_candidates,
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


class SettingsTests(unittest.TestCase):
    def test_render_key_returns_english_copy(self):
        settings = Settings(
            telegram_bot_token="token",
            telegram_api_id=1,
            telegram_api_hash="hash",
            api_base_url="https://example.com",
            product_id="chatgpt",
            support_username="@support",
            database_path=Path("db.sqlite3"),
            log_path=Path("bot.log"),
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

    async def test_activation_code_local_validation_rejects_spaces_and_persian(self):
        app = ActivationBotApp.__new__(ActivationBotApp)
        self.assertFalse(app._is_valid_activation_code("78DC 7BA3-DE88-485C"))
        self.assertFalse(app._is_valid_activation_code("کد-TEST-123"))


if __name__ == "__main__":
    unittest.main()
