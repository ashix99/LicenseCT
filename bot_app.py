from __future__ import annotations

import asyncio
import html
import json
import logging
import re
from datetime import datetime
from typing import Any

try:
    from telethon import Button, TelegramClient, events
except ImportError:  # pragma: no cover - runtime dependency
    Button = None
    TelegramClient = None
    events = None

from receipt_api import ApiError, ReceiptApiClient
from session_data import (
    SessionData,
    SessionValidationError,
    extract_outstock_user,
)
from settings import BASE_DIR, Settings
from storage import BotStorage, utc_now_iso

ACTIVATION_CODE_WHITESPACE_PATTERN = re.compile(r"\s")
ACTIVATION_CODE_PERSIAN_PATTERN = re.compile(r"[\u0600-\u06FF]")


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


class ActivationBotApp:
    UI_TEXTS = {
        "fa": {
            "choose_language_title": "انتخاب زبان",
            "choose_language_body": "لطفا زبان خود را انتخاب کنید.",
            "welcome_title": "خوش آمدید",
            "main_menu_title": "منوی اصلی",
            "order_in_progress_title": "سفارش در حال انجام است",
            "error_title": "خطا",
            "support_title": "پشتیبانی",
            "flow_cancelled_title": "فرآیند لغو شد",
            "activation_title": "فعالسازی اشتراک ChatGPT",
            "activation_invalid_title": "کد فعالسازی نامعتبر است",
            "activation_code_format_title": "کد فعالسازی نامعتبر است",
            "activation_code_format_body": "کد فعالسازی نباید فاصله یا حروف فارسی داشته باشد.\n\nلطفا کد را دوباره بدون فاصله و بدون حروف فارسی ارسال کنید.",
            "please_wait_title": "لطفا صبر کنید",
            "activation_check_error_title": "خطا در بررسی کد فعالسازی",
            "activation_used_title": "کد فعالسازی قبلا استفاده شده",
            "activation_checked_title": "کد فعالسازی بررسی شد",
            "request_session_title": "ارسال سشن اکانت",
            "session_check_error_title": "خطا در بررسی سشن اکانت",
            "session_check_unexpected": "در بررسی سشن اکانت خطای غیرمنتظره‌ای رخ داد.\nلطفا دوباره سشن اکانت را کامل و درست ارسال کنید.\n{support_hint}",
            "session_invalid_title": "سشن اکانت نامعتبر است",
            "subscription_warning_title": "هشدار اشتراک فعال",
            "final_confirm_title": "تایید نهایی سفارش",
            "processing_order_title": "سفارش در حال انجام است",
            "order_poll_error_title": "خطا در پیگیری سفارش",
            "order_timeout_title": "پاسخ سفارش دیر رسید",
            "order_submit_error_title": "خطا در ثبت سفارش",
            "unexpected_error_title": "خطای غیرمنتظره",
            "return_to_menu_title": "بازگشت به منوی اصلی",
            "language_changed_notice": "زبان تغییر کرد.",
            "request_inactive": "این درخواست دیگر فعال نیست.",
            "request_cancelled": "لغو شد.",
            "request_confirmed": "تایید شد.",
            "submitting_order_notice": "در حال ثبت سفارش...",
            "invalid_request": "درخواست نامعتبر است.",
            "language_button_text": "تغییر زبان",
            "change_account_button_text": "عوض کردن اکانت",
            "retry_button_text": "تلاش دوباره",
            "change_account_notice": "لطفا سشن اکانت جدید را ارسال کنید.",
            "unknown_value": "نامشخص",
            "success_result_title": "فعالسازی با موفقیت انجام شد",
            "failed_result_title": "فعالسازی ناموفق بود",
            "retry_later_hint": "لطفا مجدد یا بعدا تلاش کنید.",
            "result_email": "ایمیل",
            "result_product": "محصول",
            "result_app": "اپ",
            "result_activation_code": "کد فعالسازی",
            "result_activation_date": "تاریخ فعالسازی (میلادی)",
            "result_status": "وضعیت",
            "result_message": "پیام",
            "result_note": "نکته",
            "result_api_details": "جزئیات API",
        },
        "en": {
            "choose_language_title": "Choose Language",
            "choose_language_body": "Please choose your language.",
            "welcome_title": "Welcome",
            "main_menu_title": "Main Menu",
            "order_in_progress_title": "Order In Progress",
            "error_title": "Error",
            "support_title": "Support",
            "flow_cancelled_title": "Process Cancelled",
            "activation_title": "Activate ChatGPT Subscription",
            "activation_invalid_title": "Invalid Activation Code",
            "activation_code_format_title": "Invalid Activation Code",
            "activation_code_format_body": "The activation code must not contain spaces or Persian letters.\n\nPlease send the code again without spaces and without Persian letters.",
            "please_wait_title": "Please Wait",
            "activation_check_error_title": "Activation Code Check Error",
            "activation_used_title": "Activation Code Already Used",
            "activation_checked_title": "Activation Code Checked",
            "request_session_title": "Send Account Session",
            "session_check_error_title": "Session Check Error",
            "session_check_unexpected": "An unexpected error occurred while checking the account session.\nPlease send the account session completely and correctly again.\n{support_hint}",
            "session_invalid_title": "Invalid Account Session",
            "subscription_warning_title": "Active Subscription Warning",
            "final_confirm_title": "Final Order Confirmation",
            "processing_order_title": "Order In Progress",
            "order_poll_error_title": "Order Tracking Error",
            "order_timeout_title": "Order Response Timed Out",
            "order_submit_error_title": "Order Submission Error",
            "unexpected_error_title": "Unexpected Error",
            "return_to_menu_title": "Back To Main Menu",
            "language_changed_notice": "Language changed.",
            "request_inactive": "This request is no longer active.",
            "request_cancelled": "Cancelled.",
            "request_confirmed": "Confirmed.",
            "submitting_order_notice": "Submitting order...",
            "invalid_request": "Invalid request.",
            "language_button_text": "Change Language",
            "change_account_button_text": "Change Account",
            "retry_button_text": "Retry",
            "change_account_notice": "Please send a new account session.",
            "unknown_value": "Unknown",
            "success_result_title": "Activation Completed Successfully",
            "failed_result_title": "Activation Failed",
            "retry_later_hint": "Please try again now or later.",
            "result_email": "Email",
            "result_product": "Product",
            "result_app": "App",
            "result_activation_code": "Activation Code",
            "result_activation_date": "Activation Date (Gregorian)",
            "result_status": "Status",
            "result_message": "Message",
            "result_note": "Note",
            "result_api_details": "API Details",
        },
    }

    def __init__(self, settings: Settings, logger: logging.Logger) -> None:
        if TelegramClient is None or events is None or Button is None:
            raise RuntimeError(
                "Telethon نصب نشده است. ابتدا وابستگی‌های requirements.txt را نصب کنید."
            )

        self.settings = settings
        self.logger = logger
        self.storage = BotStorage(settings.database_path)
        self.api = ReceiptApiClient(settings.api_base_url, settings.product_id)
        session_path = BASE_DIR / settings.telethon_session_name
        session_path.parent.mkdir(parents=True, exist_ok=True)
        self.client = TelegramClient(
            str(session_path),
            settings.telegram_api_id,
            settings.telegram_api_hash,
        )
        self.session_tasks: dict[int, asyncio.Task[Any]] = {}
        self.order_tasks: dict[int, asyncio.Task[Any]] = {}
        self.session_status_messages: dict[int, int] = {}
        self._register_handlers()

    @staticmethod
    def _mask_value(value: str | None, *, keep_start: int = 6, keep_end: int = 4) -> str:
        if not value:
            return ""
        if len(value) <= keep_start + keep_end:
            return "*" * len(value)
        return f"{value[:keep_start]}...{value[-keep_end:]}"

    def _format_panel(self, title: str, body: str, emoji: str) -> str:
        clean_body = body.strip()
        if not clean_body:
            return f"{emoji} <b>{title}</b>"
        return f"{emoji} <b>{title}</b>\n\n{clean_body}"

    def _normalize_language(self, value: str | None) -> str:
        language = str(value or "").strip().lower()
        if language in {"fa", "en"}:
            return language
        return ""

    def _language_from_state(self, state: dict[str, Any] | None) -> str:
        if not state:
            return ""
        return self._normalize_language(state.get("language"))

    def _language_for_user(self, user_id: int, state: dict[str, Any] | None = None) -> str:
        return self._language_from_state(state or self.storage.get_state(user_id)) or "fa"

    def _ui_text(self, key: str, language: str) -> str:
        normalized = self._normalize_language(language) or "fa"
        return self.UI_TEXTS[normalized][key]

    def _render_key(self, key: str, *, language: str, **values: Any) -> str:
        return self.settings.render_key(key, language=language, **values)

    def _button_label(self, key: str, language: str) -> str:
        normalized = self._normalize_language(language) or "fa"
        emoji_map = {
            "renew": "✅",
            "support": "🧑‍💻",
            "language": "🌍",
            "cancel": "❌",
            "confirm": "✅",
        }
        text_map = {
            "renew": self.settings.get_text("renew_button_text", normalized),
            "support": self.settings.get_text("support_button_text", normalized),
            "language": self._ui_text("language_button_text", normalized),
            "cancel": self.settings.get_text("cancel_button_text", normalized),
            "confirm": self.settings.get_text("confirm_button_text", normalized),
        }
        return f"{emoji_map[key]} {text_map[key]}"

    def _inline_label(self, key: str, language: str) -> str:
        normalized = self._normalize_language(language) or "fa"
        emoji_map = {
            "change_account": "🔄",
            "retry": "🔁",
        }
        text_map = {
            "change_account": self._ui_text("change_account_button_text", normalized),
            "retry": self._ui_text("retry_button_text", normalized),
        }
        return f"{emoji_map[key]} {text_map[key]}"

    def _button_variants(self, key: str) -> set[str]:
        return {
            self._button_label(key, "fa"),
            self._button_label(key, "en"),
        }

    def _language_selector_buttons(self) -> list[list[Any]]:
        return [[
            Button.inline("🇮🇷 فارسی", data=b"set_language:fa"),
            Button.inline("🇺🇸 English", data=b"set_language:en"),
        ]]

    def _change_account_buttons(self, language: str) -> list[list[Any]]:
        return [[
            Button.inline(
                self._inline_label("change_account", language),
                data=b"change_account",
            )
        ]]

    def _retry_order_buttons(self, language: str) -> list[list[Any]]:
        return [[
            Button.inline(
                self._inline_label("retry", language),
                data=b"retry_order",
            )
        ]]

    def _should_suggest_retry_later(self, message_text: str) -> bool:
        haystack = (message_text or "").lower()
        markers = (
            "service unavailable",
            "temporarily unavailable",
            "try again",
            "request failed",
            "timeout",
            "timed out",
            "bad gateway",
            "gateway timeout",
        )
        return any(marker in haystack for marker in markers)

    async def _send_optional_sticker(self, user_id: int, kind: str | None) -> None:
        return

    async def _send_message(
        self,
        user_id: int,
        text: str,
        *,
        buttons: Any | None = None,
        link_preview: bool = True,
        sticker_kind: str | None = None,
    ) -> Any:
        await self._send_optional_sticker(user_id, sticker_kind)
        return await self.client.send_message(
            user_id,
            text,
            buttons=buttons,
            link_preview=link_preview,
            parse_mode="html",
        )

    async def _reply(
        self,
        user_id: int,
        responder: Any,
        text: str,
        *,
        buttons: Any | None = None,
        link_preview: bool = True,
        sticker_kind: str | None = None,
    ) -> Any:
        await self._send_optional_sticker(user_id, sticker_kind)
        return await responder(
            text,
            buttons=buttons,
            link_preview=link_preview,
            parse_mode="html",
        )

    async def _delete_message(self, user_id: int, message: Any | None) -> None:
        if message is None:
            return

        message_id = getattr(message, "id", None)
        if message_id is None and isinstance(message, int):
            message_id = message
        if message_id is None:
            return

        try:
            await self.client.delete_messages(user_id, [message_id])
        except Exception:  # pragma: no cover - runtime fallback
            return

    async def _clear_session_status_message(self, user_id: int) -> None:
        message_id = self.session_status_messages.pop(user_id, None)
        await self._delete_message(user_id, message_id)

    def _register_handlers(self) -> None:
        @self.client.on(events.NewMessage(pattern=r"^/start$"))
        async def start_handler(event: Any) -> None:
            await self._safely_handle(event, self.handle_start)

        @self.client.on(events.NewMessage(incoming=True))
        async def message_handler(event: Any) -> None:
            await self._safely_handle(event, self.handle_message)

        @self.client.on(events.CallbackQuery())
        async def callback_handler(event: Any) -> None:
            await self._safely_handle(event, self.handle_callback)

    async def _safely_handle(self, event: Any, handler: Any) -> None:
        if not getattr(event, "is_private", False):
            return
        try:
            await handler(event)
        except Exception as exc:  # pragma: no cover - runtime fallback
            user_id = getattr(event, "sender_id", None)
            self.logger.exception("Unhandled bot error for user %s", user_id)
            self.storage.log_event(
                user_id=user_id,
                event_type="unhandled_exception",
                details={"error": str(exc)},
            )
            if user_id is not None:
                language = self._language_for_user(user_id)
                await self._send_message(
                    user_id,
                    self._format_panel(
                        self._ui_text("error_title", language),
                        self._render_key("generic_error_message", language=language),
                        "⚠️",
                    ),
                    sticker_kind="warning",
                )

    def main_menu_buttons(self, language: str) -> list[list[Any]]:
        return [
            [Button.text(self._button_label("renew", language), resize=True)],
            [Button.text(self._button_label("support", language), resize=True)],
            [Button.text(self._button_label("language", language), resize=True)],
        ]

    def flow_menu_buttons(self, language: str) -> list[list[Any]]:
        return [
            [Button.text(self._button_label("cancel", language), resize=True)],
            [Button.text(self._button_label("support", language), resize=True)],
        ]

    def menu_buttons_for_state(self, state_name: str, language: str) -> list[list[Any]]:
        if state_name and state_name != "idle":
            return self.flow_menu_buttons(language)
        return self.main_menu_buttons(language)

    def confirm_buttons(self, confirm_data: bytes, language: str) -> list[list[Any]]:
        return [
            [
                Button.inline(self._button_label("confirm", language), data=confirm_data),
                Button.inline(self._button_label("cancel", language), data=b"cancel_flow"),
            ]
        ]

    async def _send_language_selector(
        self,
        user_id: int,
        *,
        responder: Any | None = None,
        language: str = "fa",
    ) -> None:
        text = self._format_panel(
            self._ui_text("choose_language_title", language),
            self._ui_text("choose_language_body", language),
            "🌍",
        )
        if responder is None:
            await self._send_message(
                user_id,
                text,
                buttons=self._language_selector_buttons(),
            )
            return
        await self._reply(
            user_id,
            responder,
            text,
            buttons=self._language_selector_buttons(),
        )

    async def _send_main_menu(
        self,
        user_id: int,
        *,
        responder: Any | None = None,
        title_key: str = "welcome_title",
    ) -> None:
        language = self._language_for_user(user_id)
        text = self._format_panel(
            self._ui_text(title_key, language),
            self._render_key("welcome_message", language=language),
            "✨",
        )
        if responder is None:
            await self._send_message(
                user_id,
                text,
                buttons=self.main_menu_buttons(language),
            )
            return
        await self._reply(
            user_id,
            responder,
            text,
            buttons=self.main_menu_buttons(language),
        )

    def _normalize_activation_code(self, value: str) -> str:
        return value.strip().upper()

    def _is_valid_activation_code(self, value: str) -> bool:
        raw_value = value or ""
        if ACTIVATION_CODE_WHITESPACE_PATTERN.search(raw_value):
            return False
        if ACTIVATION_CODE_PERSIAN_PATTERN.search(raw_value):
            return False
        return True

    def _sanitize_api_text(self, value: str) -> str:
        return re.sub(r"(?i)\bcdk\b", "activation code", value or "")

    def _sanitize_payload_for_user(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {
                self._sanitize_api_text(str(key)): self._sanitize_payload_for_user(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self._sanitize_payload_for_user(item) for item in value]
        if isinstance(value, str):
            return self._sanitize_api_text(value)
        return value

    def _build_activation_code_format_message(self, language: str) -> str:
        return self._format_panel(
            self._ui_text("activation_code_format_title", language),
            self._ui_text("activation_code_format_body", language),
            "⚠️",
        )

    def _build_order_result_panel(
        self,
        *,
        language: str,
        activation_code: str,
        app_name: str,
        product_name: str,
        email: str,
        result: dict[str, Any],
    ) -> str:
        success = bool(result.get("success"))
        title = (
            self._ui_text("success_result_title", language)
            if success
            else self._ui_text("failed_result_title", language)
        )
        emoji = "✅" if success else "❌"
        activated_at = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
        status_text = str(result.get("status") or "unknown")
        message_text = self._sanitize_api_text(
            str(result.get("message") or result.get("error") or "")
        ).strip()
        unknown_value = self._ui_text("unknown_value", language)

        lines = [
            f"<b>{html.escape(self._ui_text('result_email', language))}:</b> {html.escape(email or unknown_value)}",
            f"<b>{html.escape(self._ui_text('result_product', language))}:</b> {html.escape(product_name or unknown_value)}",
            f"<b>{html.escape(self._ui_text('result_app', language))}:</b> {html.escape(app_name or unknown_value)}",
            f"<b>{html.escape(self._ui_text('result_activation_code', language))}:</b> <code>{html.escape(activation_code)}</code>",
            f"<b>{html.escape(self._ui_text('result_activation_date', language))}:</b> {html.escape(activated_at)}",
            f"<b>{html.escape(self._ui_text('result_status', language))}:</b> {html.escape(status_text)}",
        ]

        if message_text:
            lines.append(
                f"<b>{html.escape(self._ui_text('result_message', language))}:</b> {html.escape(message_text)}"
            )
            if not success and self._should_suggest_retry_later(message_text):
                lines.append(
                    f"<b>{html.escape(self._ui_text('result_note', language))}:</b> {html.escape(self._ui_text('retry_later_hint', language))}"
                )
        elif not success:
            lines.append(f"<b>{html.escape(self._ui_text('result_api_details', language))}:</b>")
            lines.append(
                f"<pre>{html.escape(json_dumps(self._sanitize_payload_for_user(result)))}</pre>"
            )

        return f"{emoji} <b>{title}</b>\n\n" + "\n".join(lines)

    async def run(self) -> None:
        await self.client.start(bot_token=self.settings.telegram_bot_token)
        self.logger.info("Bot started")
        await self.client.run_until_disconnected()

    async def handle_start(self, event: Any) -> None:
        user_id = event.sender_id
        state = self.storage.get_state(user_id)
        language = self._language_from_state(state)

        if not language:
            await self.reset_user_flow(user_id, force=True)
            self.storage.log_event(
                user_id=user_id,
                event_type="start_language_selection",
                details="language_required",
            )
            self.logger.info("start_language_selection user_id=%s", user_id)
            await self._send_language_selector(user_id, responder=event.respond, language="fa")
            return

        if state["state"] == "processing_order":
            await self._reply(
                user_id,
                event.respond,
                self._format_panel(
                    self._ui_text("order_in_progress_title", language),
                    self._render_key("in_progress_message", language=language),
                    "⏳",
                ),
                buttons=self.flow_menu_buttons(language),
                sticker_kind="processing",
            )
            return

        await self.reset_user_flow(user_id, force=True)
        self.storage.log_event(user_id=user_id, event_type="start", details="start")
        self.logger.info("start user_id=%s", user_id)
        await self._send_main_menu(user_id, responder=event.respond)

    async def handle_message(self, event: Any) -> None:
        text = (event.raw_text or "").strip()
        if not text or text == "/start":
            return

        user_id = event.sender_id
        state = self.storage.get_state(user_id)
        language = self._language_from_state(state)

        if text in self._button_variants("support"):
            await self.handle_support(user_id, responder=event.respond)
            return

        if text in self._button_variants("language"):
            await self._send_language_selector(
                user_id,
                responder=event.respond,
                language=language or "fa",
            )
            return

        if not language:
            await self._send_language_selector(user_id, responder=event.respond, language="fa")
            return

        if text in self._button_variants("cancel"):
            if state["state"] == "processing_order":
                await self._reply(
                    user_id,
                    event.respond,
                    self._format_panel(
                        self._ui_text("order_in_progress_title", language),
                        self._render_key("in_progress_message", language=language),
                        "⏳",
                    ),
                    buttons=self.flow_menu_buttons(language),
                    sticker_kind="processing",
                )
            else:
                await self.cancel_flow(user_id, responder=event.respond)
            return

        if text in self._button_variants("renew"):
            if state["state"] == "processing_order":
                await self._reply(
                    user_id,
                    event.respond,
                    self._format_panel(
                        self._ui_text("order_in_progress_title", language),
                        self._render_key("in_progress_message", language=language),
                        "⏳",
                    ),
                    buttons=self.flow_menu_buttons(language),
                    sticker_kind="processing",
                )
            else:
                await self.start_activation_flow(user_id, responder=event.respond)
            return

        if state["state"] == "waiting_activation_code":
            await self.handle_activation_code_input(event, text)
            return

        if state["state"] == "waiting_session_fragments":
            await self.handle_session_fragment(event, text)
            return

        if state["state"] == "processing_order":
            await self._reply(
                user_id,
                event.respond,
                self._format_panel(
                    self._ui_text("order_in_progress_title", language),
                    self._render_key("in_progress_message", language=language),
                    "⏳",
                ),
                buttons=self.flow_menu_buttons(language),
                sticker_kind="processing",
            )
            return

        await self._send_main_menu(user_id, responder=event.respond, title_key="main_menu_title")

    async def handle_callback(self, event: Any) -> None:
        user_id = event.sender_id
        data = bytes(event.data or b"")
        state = self.storage.get_state(user_id)
        language = self._language_from_state(state) or "fa"

        if data.startswith(b"set_language:"):
            selected = data.split(b":", 1)[1].decode("utf-8", errors="ignore")
            language = "en" if selected == "en" else "fa"
            self.storage.save_state(user_id, language=language)
            self.storage.log_event(
                user_id=user_id,
                event_type="language_changed",
                details={"language": language},
            )
            await event.answer(self._ui_text("language_changed_notice", language))
            await self._send_main_menu(user_id)
            return

        if data == b"cancel_flow":
            await event.answer(self._ui_text("request_cancelled", language))
            await self.cancel_flow(user_id)
            return

        if data == b"confirm_subscription":
            if state["state"] != "waiting_subscription_confirm":
                await event.answer(self._ui_text("request_inactive", language), alert=True)
                return

            self.storage.save_state(user_id, state="waiting_final_confirm")
            self.storage.log_event(
                user_id=user_id,
                event_type="subscription_warning_confirmed",
                details="confirmed",
            )
            await event.answer(self._ui_text("request_confirmed", language))
            await self.send_final_confirmation(user_id)
            return

        if data == b"confirm_final":
            if state["state"] != "waiting_final_confirm":
                await event.answer(self._ui_text("request_inactive", language), alert=True)
                return

            await event.answer(self._ui_text("submitting_order_notice", language))
            await self.begin_order_processing(user_id)
            return

        if data == b"change_account":
            if state["state"] not in {
                "waiting_session_fragments",
                "waiting_subscription_confirm",
                "waiting_final_confirm",
            }:
                await event.answer(self._ui_text("request_inactive", language), alert=True)
                return

            self.cancel_session_task(user_id)
            self.storage.save_state(
                user_id,
                state="waiting_session_fragments",
                session_fragments=[],
                session_started_at="",
                session_email="",
                session_plan_type="",
                raw_session="",
                order_id=None,
            )
            self.storage.log_event(
                user_id=user_id,
                event_type="change_account_requested",
                details="waiting_session_fragments",
            )
            await event.answer(self._ui_text("change_account_notice", language))
            await self.send_request_session_prompt(user_id)
            return

        if data == b"retry_order":
            if state["state"] != "waiting_retry_order":
                await event.answer(self._ui_text("request_inactive", language), alert=True)
                return

            await event.answer(self._ui_text("submitting_order_notice", language))
            await self.begin_order_processing(user_id)
            return

        await event.answer(self._ui_text("invalid_request", language), alert=True)

    async def handle_support(self, user_id: int, *, responder: Any | None = None) -> None:
        state = self.storage.get_state(user_id)
        language = self._language_for_user(user_id, state)
        text = self._format_panel(
            self._ui_text("support_title", language),
            self._render_key("support_message", language=language),
            "🛟",
        )
        if responder is None:
            await self._send_message(
                user_id,
                text,
                buttons=self.menu_buttons_for_state(state["state"], language),
                sticker_kind="support",
            )
            return
        await self._reply(
            user_id,
            responder,
            text,
            buttons=self.menu_buttons_for_state(state["state"], language),
            sticker_kind="support",
        )

    async def cancel_flow(self, user_id: int, *, responder: Any | None = None) -> None:
        state = self.storage.get_state(user_id)
        language = self._language_for_user(user_id, state)
        await self.reset_user_flow(user_id, force=True)
        self.storage.log_event(
            user_id=user_id, event_type="flow_cancelled", details="cancelled"
        )
        text = self._format_panel(
            self._ui_text("flow_cancelled_title", language),
            self._render_key("cancelled_message", language=language),
            "🧹",
        )
        if responder is None:
            await self._send_message(
                user_id,
                text,
                buttons=self.main_menu_buttons(language),
                sticker_kind="info",
            )
            return
        await self._reply(
            user_id,
            responder,
            text,
            buttons=self.main_menu_buttons(language),
            sticker_kind="info",
        )

    async def start_activation_flow(
        self,
        user_id: int,
        *,
        responder: Any | None = None,
    ) -> None:
        language = self._language_for_user(user_id)
        self.cancel_session_task(user_id)
        self.storage.save_state(
            user_id,
            state="waiting_activation_code",
            activation_code="",
            activation_app_name="",
            activation_product_name="",
            activation_payload="",
            session_fragments=[],
            session_started_at="",
            session_email="",
            session_plan_type="",
            raw_session="",
            order_id=None,
        )
        self.storage.log_event(
            user_id=user_id,
            event_type="activation_flow_started",
            details="waiting_activation_code",
        )
        self.logger.info("activation_flow_started user_id=%s", user_id)
        text = self._format_panel(
            self._ui_text("activation_title", language),
            self._render_key("request_activation_code_message", language=language),
            "🔑",
        )
        if responder is None:
            await self._send_message(
                user_id,
                text,
                buttons=self.flow_menu_buttons(language),
                sticker_kind="activation",
            )
            return
        await self._reply(
            user_id,
            responder,
            text,
            buttons=self.flow_menu_buttons(language),
            sticker_kind="activation",
        )

    async def send_request_session_prompt(self, user_id: int) -> None:
        language = self._language_for_user(user_id)
        await self._send_message(
            user_id,
            self._format_panel(
                self._ui_text("request_session_title", language),
                self._render_key("request_session_message", language=language),
                "🧾",
            ),
            link_preview=False,
            buttons=self.flow_menu_buttons(language),
            sticker_kind="info",
        )

    async def handle_activation_code_input(self, event: Any, text: str) -> None:
        user_id = event.sender_id
        language = self._language_for_user(user_id)
        activation_code = self._normalize_activation_code(text)
        if not activation_code:
            await self._reply(
                user_id,
                event.respond,
                self._format_panel(
                    self._ui_text("activation_invalid_title", language),
                    self._render_key("activation_invalid_message", language=language),
                    "⚠️",
                ),
                buttons=self.flow_menu_buttons(language),
                sticker_kind="warning",
            )
            return

        if not self._is_valid_activation_code(activation_code):
            await self._reply(
                user_id,
                event.respond,
                self._build_activation_code_format_message(language),
                buttons=self.flow_menu_buttons(language),
                sticker_kind="warning",
            )
            return

        checking_message = await self._reply(
            user_id,
            event.respond,
            self._format_panel(
                self._ui_text("please_wait_title", language),
                self._render_key("activation_checking_message", language=language),
                "⏳",
            ),
            buttons=self.flow_menu_buttons(language),
        )

        self.storage.log_event(
            user_id=user_id,
            event_type="activation_code_received",
            details={"activation_code": activation_code},
        )
        self.logger.info(
            "activation_code_received user_id=%s code=%s",
            user_id,
            self._mask_value(activation_code, keep_start=8, keep_end=6),
        )
        try:
            payload = await asyncio.to_thread(self.api.check_activation_code, activation_code)
        except ApiError as exc:
            self.logger.warning(
                "activation_check_failed user_id=%s path=%s status=%s request_id=%s body=%r",
                user_id,
                exc.path,
                exc.status_code,
                exc.request_id,
                exc.response_body,
            )
            self.storage.log_event(
                user_id=user_id,
                event_type="activation_code_check_error",
                details={
                    "error": exc.to_dict(),
                    "activation_code": activation_code,
                },
            )
            await self._delete_message(user_id, checking_message)
            await self._reply(
                user_id,
                event.respond,
                self._format_panel(
                    self._ui_text("activation_check_error_title", language),
                    self._render_key(
                        "activation_check_error_message",
                        language=language,
                        error=self._sanitize_api_text(str(exc)),
                    ),
                    "⚠️",
                ),
                buttons=self.flow_menu_buttons(language),
                sticker_kind="warning",
            )
            return

        if payload.get("used"):
            self.storage.log_event(
                user_id=user_id,
                event_type="activation_code_used",
                details=payload,
            )
            await self._delete_message(user_id, checking_message)
            await self._reply(
                user_id,
                event.respond,
                self._format_panel(
                    self._ui_text("activation_used_title", language),
                    self._render_key("activation_used_message", language=language),
                    "⚠️",
                ),
                buttons=self.flow_menu_buttons(language),
                sticker_kind="warning",
            )
            return

        unknown_value = self._ui_text("unknown_value", language)
        app_name = str(payload.get("app_name") or unknown_value)
        product_name = str(payload.get("app_product_name") or unknown_value)
        self.storage.save_state(
            user_id,
            state="waiting_session_fragments",
            activation_code=activation_code,
            activation_app_name=app_name,
            activation_product_name=product_name,
            activation_payload=json.dumps(payload, ensure_ascii=False),
            session_fragments=[],
            session_started_at="",
            session_email="",
            session_plan_type="",
            raw_session="",
            order_id=None,
        )
        self.storage.log_event(
            user_id=user_id,
            event_type="activation_code_valid",
            details=payload,
        )
        self.logger.info(
            "activation_code_valid user_id=%s product=%s app=%s",
            user_id,
            product_name,
            app_name,
        )
        await self._delete_message(user_id, checking_message)
        await self._reply(
            user_id,
            event.respond,
            self._format_panel(
                self._ui_text("activation_checked_title", language),
                self._render_key(
                    "activation_info_message",
                    language=language,
                    app_name=app_name,
                    app_product_name=product_name,
                    usage_status=self.settings.get_text("usage_status_ready_text", language),
                ),
                "✅",
            ),
            buttons=self.flow_menu_buttons(language),
            sticker_kind="success",
        )
        await self.send_request_session_prompt(user_id)

    async def handle_session_fragment(self, event: Any, text: str) -> None:
        user_id = event.sender_id
        language = self._language_for_user(user_id)
        state = self.storage.get_state(user_id)
        fragments = list(state["session_fragments"])
        is_first_fragment = len(fragments) == 0
        fragments.append(text)

        self.storage.save_state(
            user_id,
            session_fragments=fragments,
            session_started_at=state["session_started_at"] or utc_now_iso(),
        )
        self.storage.log_event(
            user_id=user_id,
            event_type="session_fragment_received",
            details={"fragment_count": len(fragments)},
        )
        self.logger.info(
            "session_fragment_received user_id=%s fragment_count=%s",
            user_id,
            len(fragments),
        )

        if is_first_fragment:
            checking_message = await self._send_message(
                user_id,
                self._format_panel(
                    self._ui_text("please_wait_title", language),
                    self._render_key("session_checking_message", language=language),
                    "⏳",
                ),
            )
            message_id = getattr(checking_message, "id", None)
            if message_id is not None:
                self.session_status_messages[user_id] = message_id

        if len(fragments) >= self.settings.session_max_messages:
            self.cancel_session_task(user_id)
            await self.finalize_session_fragments(user_id)
            return

        if len(fragments) == 1:
            self.session_tasks[user_id] = asyncio.create_task(
                self._finalize_session_after_window(user_id)
            )

    async def _finalize_session_after_window(self, user_id: int) -> None:
        try:
            await asyncio.sleep(self.settings.session_window_seconds)
            await self.finalize_session_fragments(user_id)
        except asyncio.CancelledError:  # pragma: no cover - timing path
            return
        except Exception as exc:  # pragma: no cover - runtime fallback
            self.logger.exception(
                "session_finalize_unhandled user_id=%s error=%s",
                user_id,
                exc,
            )
            self.storage.log_event(
                user_id=user_id,
                event_type="session_finalize_unhandled",
                details={"error": str(exc)},
            )
            await self._clear_session_status_message(user_id)
            language = self._language_for_user(user_id)
            await self._send_message(
                user_id,
                self._format_panel(
                    self._ui_text("session_check_error_title", language),
                    self.settings.render(
                        self._ui_text("session_check_unexpected", language),
                        language=language,
                    ),
                    "⚠️",
                ),
                link_preview=False,
                buttons=self.flow_menu_buttons(language),
            )

    async def finalize_session_fragments(self, user_id: int) -> None:
        self.cancel_session_task(user_id, preserve_current=True)
        state = self.storage.get_state(user_id)
        language = self._language_for_user(user_id, state)
        if state["state"] != "waiting_session_fragments":
            return

        raw_session = "".join(state["session_fragments"]).strip()
        if not raw_session:
            return

        try:
            session_data = SessionData.parse(raw_session)
        except SessionValidationError as exc:
            await self._clear_session_status_message(user_id)
            self.storage.save_state(
                user_id,
                state="waiting_session_fragments",
                session_fragments=[],
                session_started_at="",
                session_email="",
                session_plan_type="",
                raw_session="",
            )
            self.storage.log_event(
                user_id=user_id,
                event_type="session_invalid",
                details={"error": str(exc), "raw_session": raw_session},
            )
            self.logger.warning(
                "session_invalid user_id=%s error=%s",
                user_id,
                exc,
            )
            await self._send_message(
                user_id,
                self._format_panel(
                    self._ui_text("session_invalid_title", language),
                    f"{html.escape(str(exc))}\n\n"
                    f"{self._render_key('session_invalid_message', language=language)}",
                    "⚠️",
                ),
                link_preview=False,
                buttons=self._change_account_buttons(language),
                sticker_kind="warning",
            )
            return
        except Exception as exc:  # pragma: no cover - runtime fallback
            await self._clear_session_status_message(user_id)
            self.storage.save_state(
                user_id,
                state="waiting_session_fragments",
                session_fragments=[],
                session_started_at="",
                session_email="",
                session_plan_type="",
                raw_session="",
            )
            self.storage.log_event(
                user_id=user_id,
                event_type="session_finalize_error",
                details={"error": str(exc), "raw_session": raw_session},
            )
            self.logger.exception(
                "session_finalize_error user_id=%s error=%s",
                user_id,
                exc,
            )
            await self._send_message(
                user_id,
                self._format_panel(
                    self._ui_text("session_check_error_title", language),
                    self.settings.render(
                        self._ui_text("session_check_unexpected", language),
                        language=language,
                    ),
                    "⚠️",
                ),
                link_preview=False,
                buttons=self.flow_menu_buttons(language),
                sticker_kind="warning",
            )
            return

        await self._clear_session_status_message(user_id)
        next_state = (
            "waiting_subscription_confirm"
            if session_data.has_active_subscription
            else "waiting_final_confirm"
        )
        self.storage.save_state(
            user_id,
            state=next_state,
            session_fragments=[],
            session_started_at="",
            session_email=session_data.email,
            session_plan_type=session_data.plan_type,
            raw_session=session_data.raw_text,
        )
        self.storage.log_event(
            user_id=user_id,
            event_type="session_valid",
            details={
                "email": session_data.email,
                "plan_type": session_data.plan_type,
                "raw_session": session_data.raw_text,
            },
        )
        self.logger.info(
            "session_valid user_id=%s email=%s plan_type=%s",
            user_id,
            session_data.email,
            session_data.plan_type,
        )

        if session_data.has_active_subscription:
            await self._send_message(
                user_id,
                self._format_panel(
                    self._ui_text("subscription_warning_title", language),
                    self._render_key("subscription_warning_message", language=language),
                    "⚠️",
                ),
                buttons=[
                    [
                        Button.inline(
                            self._button_label("confirm", language),
                            data=b"confirm_subscription",
                        ),
                        Button.inline(
                            self._inline_label("change_account", language),
                            data=b"change_account",
                        ),
                    ],
                    [Button.inline(self._button_label("cancel", language), data=b"cancel_flow")],
                ],
                sticker_kind="warning",
            )
            return

        await self.send_final_confirmation(user_id)

    async def send_final_confirmation(self, user_id: int) -> None:
        state = self.storage.get_state(user_id)
        language = self._language_for_user(user_id, state)
        unknown_value = self._ui_text("unknown_value", language)
        await self._send_message(
            user_id,
            self._format_panel(
                self._ui_text("final_confirm_title", language),
                self._render_key(
                    "final_confirm_message",
                    language=language,
                    email=state["session_email"] or unknown_value,
                    product=state["activation_product_name"] or unknown_value,
                ),
                "🟢",
            ),
            buttons=self.confirm_buttons(b"confirm_final", language),
            sticker_kind="info",
        )

    async def begin_order_processing(self, user_id: int) -> None:
        state = self.storage.get_state(user_id)
        language = self._language_for_user(user_id, state)
        if state["state"] not in {"waiting_final_confirm", "waiting_retry_order"}:
            return

        order_id = self.storage.create_order(
            user_id=user_id,
            activation_code=state["activation_code"],
            app_name=state["activation_app_name"],
            product_name=state["activation_product_name"],
            email=state["session_email"],
            plan_type=state["session_plan_type"],
            raw_session=state["raw_session"],
            status="processing",
        )
        self.storage.save_state(user_id, state="processing_order", order_id=order_id)
        self.storage.log_event(
            user_id=user_id,
            event_type="order_processing_started",
            details={"order_id": order_id},
        )
        self.logger.info(
            "order_processing_started user_id=%s order_id=%s product=%s email=%s",
            user_id,
            order_id,
            state["activation_product_name"],
            state["session_email"],
        )
        await self._send_message(
            user_id,
            self._format_panel(
                self._ui_text("processing_order_title", language),
                self._render_key("processing_order_message", language=language),
                "🚀",
            ),
            buttons=self.flow_menu_buttons(language),
            sticker_kind="processing",
        )

        existing_task = self.order_tasks.pop(user_id, None)
        if existing_task is not None:
            existing_task.cancel()

        self.order_tasks[user_id] = asyncio.create_task(
            self.process_order(
                user_id=user_id,
                order_id=order_id,
                activation_code=state["activation_code"],
                app_name=state["activation_app_name"],
                product_name=state["activation_product_name"],
                email=state["session_email"],
                raw_session=state["raw_session"],
            )
        )

    async def process_order(
        self,
        *,
        user_id: int,
        order_id: int,
        activation_code: str,
        app_name: str,
        product_name: str,
        email: str,
        raw_session: str,
    ) -> None:
        language = self._language_for_user(user_id)
        try:
            outstock_user = extract_outstock_user(raw_session)
            self.storage.log_event(
                user_id=user_id,
                event_type="order_submit_payload",
                details={
                    "order_id": order_id,
                    "user_source": "raw_session",
                    "user_length": len(outstock_user),
                },
            )
            self.logger.info(
                "order_submit_payload user_id=%s order_id=%s user_source=raw_session user_length=%s code=%s",
                user_id,
                order_id,
                len(outstock_user),
                self._mask_value(activation_code, keep_start=8, keep_end=6),
            )
            task_id = await asyncio.to_thread(
                self.api.create_outstock_order,
                activation_code,
                outstock_user,
            )

            self.storage.update_order(
                order_id,
                task_id=task_id,
                status="submitted",
                task_result="",
            )
            self.storage.log_event(
                user_id=user_id,
                event_type="order_submitted",
                details={
                    "order_id": order_id,
                    "task_id": task_id,
                    "user_source": "raw_session",
                },
            )
            self.logger.info(
                "order_submitted user_id=%s order_id=%s task_id=%s user_source=raw_session",
                user_id,
                order_id,
                task_id,
            )

            loop = asyncio.get_running_loop()
            deadline = loop.time() + self.settings.outstock_timeout_seconds
            while loop.time() < deadline:
                await asyncio.sleep(self.settings.outstock_poll_seconds)
                try:
                    result = await asyncio.to_thread(self.api.get_outstock_status, task_id)
                except ApiError as exc:
                    self.storage.update_order(
                        order_id,
                        status="poll_error",
                        task_result=str(exc),
                    )
                    self.storage.log_event(
                        user_id=user_id,
                        event_type="order_poll_error",
                        details={"order_id": order_id, "error": exc.to_dict()},
                    )
                    self.logger.warning(
                        "order_poll_error user_id=%s order_id=%s path=%s status=%s request_id=%s body=%r",
                        user_id,
                        order_id,
                        exc.path,
                        exc.status_code,
                        exc.request_id,
                        exc.response_body,
                    )
                    await self._send_message(
                        user_id,
                        self._format_panel(
                            self._ui_text("order_poll_error_title", language),
                            self._render_key(
                                "order_poll_error_message",
                                language=language,
                                error=self._sanitize_api_text(str(exc)),
                            ),
                            "⚠️",
                        ),
                        sticker_kind="warning",
                    )
                    await self.finish_user_flow(user_id)
                    return

                raw_result = json_dumps(result)
                if result.get("pending", False):
                    self.storage.update_order(
                        order_id,
                        status="pending",
                        task_result=raw_result,
                    )
                    continue

                self.storage.update_order(
                    order_id,
                    status="success" if result.get("success") else "failed",
                    task_result=raw_result,
                )
                self.storage.log_event(
                    user_id=user_id,
                    event_type="order_completed",
                    details={"order_id": order_id, "result": result},
                )
                self.logger.info(
                    "order_completed user_id=%s order_id=%s success=%s pending=%s",
                    user_id,
                    order_id,
                    result.get("success"),
                    result.get("pending"),
                )
                if result.get("success"):
                    await self._send_message(
                        user_id,
                        self._build_order_result_panel(
                            language=language,
                            activation_code=activation_code,
                            app_name=app_name,
                            product_name=product_name,
                            email=email,
                            result=result,
                        ),
                        sticker_kind="success",
                    )
                    await self.finish_user_flow(user_id)
                    return

                self.storage.save_state(
                    user_id,
                    state="waiting_retry_order",
                    order_id=order_id,
                )
                await self._send_message(
                    user_id,
                    self._build_order_result_panel(
                        language=language,
                        activation_code=activation_code,
                        app_name=app_name,
                        product_name=product_name,
                        email=email,
                        result=result,
                    ),
                    buttons=self._retry_order_buttons(language),
                    sticker_kind="warning",
                )
                return

            self.storage.update_order(order_id, status="timeout", task_result="timeout")
            self.storage.log_event(
                user_id=user_id,
                event_type="order_timeout",
                details={"order_id": order_id},
            )
            self.logger.warning("order_timeout user_id=%s order_id=%s", user_id, order_id)
            await self._send_message(
                user_id,
                self._format_panel(
                    self._ui_text("order_timeout_title", language),
                    self._render_key("order_timeout_message", language=language),
                    "⚠️",
                ),
                sticker_kind="warning",
            )
            await self.finish_user_flow(user_id)
        except ApiError as exc:
            self.storage.update_order(
                order_id,
                status="submit_error",
                task_result=str(exc),
            )
            self.storage.log_event(
                user_id=user_id,
                event_type="order_submit_error",
                details={"order_id": order_id, "error": exc.to_dict()},
            )
            self.logger.warning(
                "order_submit_error user_id=%s order_id=%s path=%s status=%s request_id=%s body=%r",
                user_id,
                order_id,
                exc.path,
                exc.status_code,
                exc.request_id,
                exc.response_body,
            )
            await self._send_message(
                user_id,
                self._format_panel(
                    self._ui_text("order_submit_error_title", language),
                    self._render_key(
                        "order_submit_error_message",
                        language=language,
                        error=self._sanitize_api_text(str(exc)),
                    ),
                    "⚠️",
                ),
                sticker_kind="warning",
            )
            await self.finish_user_flow(user_id)
        except Exception as exc:  # pragma: no cover - runtime fallback
            self.logger.exception("Unexpected order processing error for user %s", user_id)
            self.storage.update_order(
                order_id,
                status="unexpected_error",
                task_result=str(exc),
            )
            self.storage.log_event(
                user_id=user_id,
                event_type="order_unexpected_error",
                details={"order_id": order_id, "error": str(exc)},
            )
            await self._send_message(
                user_id,
                self._format_panel(
                    self._ui_text("unexpected_error_title", language),
                    self._render_key("generic_error_message", language=language),
                    "⚠️",
                ),
                sticker_kind="warning",
            )
            await self.finish_user_flow(user_id)
        finally:
            self.order_tasks.pop(user_id, None)

    async def finish_user_flow(self, user_id: int) -> None:
        language = self._language_for_user(user_id)
        await self.reset_user_flow(user_id, force=True)
        await self._send_message(
            user_id,
            self._format_panel(
                self._ui_text("return_to_menu_title", language),
                self._render_key("return_to_menu_message", language=language),
                "📌",
            ),
            buttons=self.main_menu_buttons(language),
            sticker_kind="info",
        )

    async def reset_user_flow(self, user_id: int, *, force: bool = False) -> None:
        self.cancel_session_task(user_id)
        await self._clear_session_status_message(user_id)
        state = self.storage.get_state(user_id)
        if force or state["state"] != "processing_order":
            self.storage.reset_state(user_id)

    def cancel_session_task(self, user_id: int, *, preserve_current: bool = False) -> None:
        task = self.session_tasks.pop(user_id, None)
        if task is not None:
            if preserve_current and task is asyncio.current_task():
                return
            task.cancel()
